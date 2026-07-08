"""Upload & Process page and HTMX endpoints."""
import os
import re
import json
import uuid
import time
import math
import logging
import tempfile
import threading
from concurrent.futures import ThreadPoolExecutor
from typing import Optional

import pandas as pd
from fastapi import APIRouter, Request, UploadFile, File, Form
from fastapi.responses import HTMLResponse, StreamingResponse
from sqlalchemy import Float, Integer, Numeric

from database import Session, SaleComp, LeaseComp

log = logging.getLogger(__name__)
from engine.pipeline import run_mapping_stage, run_geocoding_stage, run_vision_pdf_stage
from engine.brokers import resolve_broker
from engine.cleaning import apply_rate_logic
from engine.mapping import SALE_SCHEMA, LEASE_SCHEMA, dedupe_mappings_by_target
from engine.mapping_audit import audit_segment
from learning.corrections import persist_with_learning
from utils import normalize_address
from web.config import settings
from web.dependencies import get_learning_store

router = APIRouter(prefix="/upload", tags=["upload"])

# Server-side job storage
_jobs: dict[str, dict] = {}


def _schema_fields_for_type(file_type: Optional[str]) -> list[str]:
    ft = (file_type or "").lower()
    if ft in ("lease", "both"):
        return list(LEASE_SCHEMA.keys())
    if ft == "sale":
        return list(SALE_SCHEMA.keys())
    return list(dict.fromkeys([*SALE_SCHEMA.keys(), *LEASE_SCHEMA.keys()]))


def _active_segments(job: dict) -> list:
    voided = set(job.get("voided_segment_keys") or [])
    return [seg for seg in job.get("segments", []) if seg.segment_key not in voided]


def _apply_mappings_with_notes_concat(raw_df: pd.DataFrame, mappings: dict[str, str]) -> pd.DataFrame:
    """Apply user-edited mappings to raw_df.

    - Each target field (except 'notes') is populated from at most one source column.
      If the user mapped two raw columns to the same target, the first wins.
    - 'notes' is special: ALL source columns mapped to 'notes' are concatenated into
      a single string in the form 'col1: val1 | col2: val2'.
    """
    out = pd.DataFrame(index=raw_df.index)
    notes_cols = [raw for raw, target in mappings.items() if target == "notes"]
    other_mappings: dict[str, str] = {}
    for raw, target in mappings.items():
        if not target or target in ("---", "unmapped", "notes"):
            continue
        if target in other_mappings.values():
            # Target already has a source; skip duplicates (client should have prevented this)
            continue
        other_mappings[raw] = target

    for raw, target in other_mappings.items():
        if raw in raw_df.columns:
            out[target] = raw_df[raw]

    if notes_cols:
        def _concat_row(row):
            parts = []
            for col in notes_cols:
                val = row.get(col)
                if val is None:
                    continue
                try:
                    if pd.isna(val):
                        continue
                except Exception:
                    pass
                s = str(val).strip()
                if s:
                    parts.append(f"{col}: {s}")
            return " | ".join(parts) if parts else None

        out["notes"] = raw_df.apply(_concat_row, axis=1)

    return out


def _coerce_numeric(val):
    """Coerce strings like '$32,572,600', '5.70%', '4,950 - 7,700' into float.

    Returns None for empty/unparseable. For ranges, returns the first number
    (keeps the row instead of dropping it; user can refine manually later).
    """
    if val is None:
        return None
    try:
        if pd.isna(val):
            return None
    except Exception:
        pass
    if isinstance(val, (int, float)):
        return float(val) if not (isinstance(val, float) and math.isnan(val)) else None
    s = str(val).strip()
    if not s:
        return None
    cleaned = s.replace(',', '').replace('$', '').replace('%', '')
    cleaned = re.sub(r'(?i)\bsf\b', '', cleaned).strip()
    try:
        return float(cleaned)
    except Exception:
        pass
    # Range or messy: take first signed number
    m = re.search(r'-?\d+\.?\d*', cleaned)
    if m:
        try:
            return float(m.group(0))
        except Exception:
            return None
    return None


def _safe_json_rows(df: pd.DataFrame) -> list[dict]:
    """Convert DataFrame rows to JSON-safe dicts."""
    rows = df.head(10).to_dict(orient="records")
    clean = []
    for row in rows:
        cr = {}
        for k, v in row.items():
            if v is None:
                cr[k] = None
            elif isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
                cr[k] = None
            elif hasattr(v, 'isoformat'):
                cr[k] = v.isoformat()
            else:
                cr[k] = v
        clean.append(cr)
    return clean


def _has_address(val) -> bool:
    """Return True only if ``val`` is a non-empty, non-whitespace address.

    Treats NULL / NaN / pandas-NA / empty-string / whitespace-only as missing.
    This is the canonical "does this comp have an address?" check used by both
    the upload save path and the application-level validation. Address is
    required: without it the row can't be geocoded, mapped, or matched in
    Comp Finder.
    """
    if val is None:
        return False
    try:
        if pd.isna(val):
            return False
    except (TypeError, ValueError):
        pass
    if isinstance(val, float) and math.isnan(val):
        return False
    return bool(str(val).strip())


def _save_rows_to_db(
    concat_df: pd.DataFrame,
    *,
    file_type: str,
    source_file: Optional[str],
) -> dict:
    """Persist comp rows to the DB, skipping any row whose address is missing.

    Validation point: this is the LAST line of defense before INSERT. Rows
    without a usable ``address`` (NULL / empty / whitespace-only) are dropped
    here and logged. The schema-level NOT NULL + non-empty validator on the
    SaleComp / LeaseComp models is the second layer of defense.

    Returns a dict with::
        {"inserted_ids": [...], "skipped_count": N, "skipped_rows": [row_index, ...]}
    """
    session = Session()
    try:
        fallback_type = (file_type or "sale").lower()
        inserted_ids: list[int] = []
        skipped_rows: list[int] = []

        for row_idx, row in concat_df.iterrows():
            # ---- Defense in depth, layer 1: address required ----
            addr_val = row.get("address") if "address" in row.index else None
            if not _has_address(addr_val):
                skipped_rows.append(row_idx)
                log.warning(
                    "Skipping comp row (missing address) source_file=%s row_idx=%s",
                    source_file, row_idx,
                )
                continue

            # Per-row type routing: a single upload may mix lease + sale rows.
            row_type = str(row.get("source_type") or fallback_type).lower()
            Model = LeaseComp if row_type in ("lease", "both") else SaleComp
            numeric_cols = {
                c.name for c in Model.__table__.columns
                if isinstance(c.type, (Float, Integer, Numeric))
            }

            record = Model()
            for col in Model.__table__.columns:
                if col.name in ("id", "created_at") or col.name not in row.index:
                    continue
                val = row[col.name]
                try:
                    is_na = pd.isna(val)
                except Exception:
                    is_na = False
                if is_na:
                    continue
                if col.name in numeric_cols:
                    val = _coerce_numeric(val)
                    if val is None:
                        continue
                setattr(record, col.name, val)
            if source_file is not None:
                record.source_file = source_file
            session.add(record)
            session.flush()
            inserted_ids.append(record.id)

        session.commit()
        return {
            "inserted_ids": inserted_ids,
            "skipped_count": len(skipped_rows),
            "skipped_rows": skipped_rows,
        }
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def _clean_address_text(val) -> str:
    if val is None:
        return ""
    try:
        if pd.isna(val):
            return ""
    except Exception:
        pass
    text = str(val).strip()
    if not text or text.lower() in {"nan", "none", "null", "n/a", "na", "-", "--"}:
        return ""
    return text


def _normalize_entry_value(val, field: str = "") -> str:
    text = _clean_address_text(val).lower()
    if not text:
        return ""
    if field in {"sale_price", "leased_sf"}:
        num = _coerce_numeric(val)
        if num is not None:
            return f"{num:.2f}"
    if field.endswith("_date") or field in {"closing_date", "commencement_date"}:
        text = text.split("t", 1)[0]
    return re.sub(r"\s+", " ", re.sub(r"[^\w.\- ]+", " ", text)).strip()


def _row_completeness(row: pd.Series) -> tuple[int, int]:
    score = 0
    char_count = 0
    ignored = {"source_type", "source_file", "source_sheet", "latitude", "longitude", "geocode_source"}
    for col, val in row.items():
        if col in ignored:
            continue
        text = _clean_address_text(val)
        if text:
            score += 1
            char_count += len(text)
    return score, char_count


def _dedupe_identity(row: pd.Series, file_type: str) -> Optional[tuple[str, dict[str, str]]]:
    address = _clean_address_text(row.get("address")) or _clean_address_text(row.get("raw_address_data"))
    norm_address = normalize_address(address)
    if not norm_address:
        return None

    ft = (file_type or "").lower()
    if ft in {"lease", "both"}:
        identity_fields = ("tenant_name", "commencement_date")
        values = {
            field: _normalize_entry_value(row.get(field), field)
            for field in identity_fields
            if _normalize_entry_value(row.get(field), field)
        }
        if not values:
            return None
        return norm_address, values

    identity_fields = ("closing_date", "sale_price")
    values = {
        field: _normalize_entry_value(row.get(field), field)
        for field in identity_fields
        if _normalize_entry_value(row.get(field), field)
    }
    return norm_address, values


def _compatible_identity(cluster_identity: dict[str, str], row_identity: dict[str, str]) -> bool:
    for field, value in row_identity.items():
        existing = cluster_identity.get(field)
        if existing and existing != value:
            return False
    return True


def _dedupe_segments_within_sheets(segments: list) -> int:
    """Drop duplicate entries within the same uploaded sheet, keeping the richest row."""
    if not segments:
        return 0

    grouped: dict[tuple[str, str, str], list[dict]] = {}
    keep_all: list[dict] = []
    order = 0
    for seg in segments:
        df = getattr(seg, "cleaned_df", None)
        if df is None or df.empty:
            continue
        file_type = getattr(getattr(seg, "fingerprint", None), "file_type", "") or ""
        sheet_name = getattr(getattr(seg, "fingerprint", None), "sheet_name", None) or seg.segment_key.split("::", 1)[0]
        for row_idx, row in df.iterrows():
            identity = _dedupe_identity(row, file_type)
            item = {
                "seg": seg,
                "row_idx": row_idx,
                "row": row,
                "file_type": file_type,
                "sheet_name": sheet_name,
                "order": order,
            }
            order += 1
            if identity is None:
                keep_all.append(item)
                continue
            address_key, row_identity = identity
            grouped.setdefault((sheet_name, file_type.lower(), address_key), []).append({**item, "identity": row_identity})

    keep_items = list(keep_all)
    duplicate_count = 0
    for items in grouped.values():
        clusters: list[dict] = []
        for item in items:
            placed = False
            for cluster in clusters:
                if _compatible_identity(cluster["identity"], item["identity"]):
                    cluster["items"].append(item)
                    cluster["identity"].update(item["identity"])
                    placed = True
                    break
            if not placed:
                clusters.append({"identity": dict(item["identity"]), "items": [item]})

        for cluster in clusters:
            cluster_items = cluster["items"]
            if len(cluster_items) > 1:
                duplicate_count += len(cluster_items) - 1
            keep_items.append(max(cluster_items, key=lambda i: (*_row_completeness(i["row"]), -i["order"])))

    keep_by_segment: dict[str, list] = {}
    for item in sorted(keep_items, key=lambda i: i["order"]):
        keep_by_segment.setdefault(item["seg"].segment_key, []).append(item["row_idx"])

    for seg in segments:
        df = getattr(seg, "cleaned_df", None)
        if df is None or df.empty:
            continue
        keep_indices = keep_by_segment.get(seg.segment_key, [])
        seg.cleaned_df = df.loc[keep_indices].reset_index(drop=True) if keep_indices else df.iloc[0:0].copy()

    return duplicate_count


def _row_geocode_text(row) -> str:
    """Prefer richer combined address text when the pipeline produced it."""
    for col in ("raw_address_data", "address"):
        text = _clean_address_text(row.get(col))
        if text:
            return text
    return ""


def _rate_header_for_mapping(mappings: dict[str, str]) -> Optional[str]:
    return next((raw for raw, target in mappings.items() if target == "rate_psf"), None)


_AUDIT_MAX_WORKERS = 8


def _attach_audit_flags(job: dict, segments_data: list[dict]) -> None:
    """Audit each segment's mapping against its raw sample values (advisory).

    Sets ``entry["audit_flags"]`` on every segments_data entry to a list of
    {"header", "reason", "suggested_field"} dicts (``[]`` when clean or degraded).
    Best-effort and non-fatal: a per-segment failure yields ``[]`` for that
    segment. Segments are audited concurrently to bound wall-clock latency.
    """
    seg_by_key = {s.segment_key: s for s in job.get("segments", [])}
    raw_dfs = job.get("raw_dfs", {})

    def _flags_for(entry: dict) -> list[dict]:
        seg = seg_by_key.get(entry.get("segment_key"))
        raw_df = raw_dfs.get(entry.get("segment_key"))
        if seg is None or raw_df is None:
            return []
        try:
            mappings = seg.mapping_result.mappings or {}
            mapped_headers = [h for h in mappings if h in raw_df.columns]
            if not mapped_headers:
                return []
            sample_rows = raw_df[mapped_headers].head(5).to_dict("records")
            return audit_segment(mappings, sample_rows, seg.fingerprint.file_type)
        except Exception:
            log.warning("mapping audit failed for segment %s", entry.get("segment_key"), exc_info=True)
            return []

    if not segments_data:
        return
    with ThreadPoolExecutor(max_workers=min(_AUDIT_MAX_WORKERS, len(segments_data))) as pool:
        results = list(pool.map(_flags_for, segments_data))
    for entry, flags in zip(segments_data, results):
        entry["audit_flags"] = flags


@router.get("", response_class=HTMLResponse)
async def upload_page(request: Request):
    templates = request.app.state.templates
    user = request.state.user
    return templates.TemplateResponse(request, "upload.html", {
        "request": request,
        "user": user,
        "current_page": "upload",
        "logo_b64": request.app.state.logo_b64,
        "icon_b64": request.app.state.icon_b64,
    })


@router.post("/file", response_class=HTMLResponse)
async def upload_file(request: Request, file: UploadFile = File(...)):
    """Receive file, run mapping stage, return preview partial."""
    templates = request.app.state.templates
    store = get_learning_store()

    # Save to temp
    suffix = os.path.splitext(file.filename)[1]
    with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as tmp:
        content = await file.read()
        tmp.write(content)
        tmp_path = tmp.name

    job_id = str(uuid.uuid4())
    segments_data = []

    try:
        if suffix.lower() == '.pdf':
            # PDF vision pipeline
            seg_result = run_vision_pdf_stage(
                tmp_path,
                file.filename,
                max_pages=settings.PDF_MAX_PAGES,
                total_timeout_seconds=settings.PDF_TOTAL_TIMEOUT_SECONDS,
                raster_timeout_seconds=settings.PDF_RASTER_TIMEOUT_SECONDS,
                openai_timeout_seconds=settings.PDF_OPENAI_TIMEOUT_SECONDS,
                dpi=settings.PDF_DPI,
            )
            pdf_warnings = list(seg_result.cleaned_df.attrs.get("warnings", []))
            if seg_result.cleaned_df.empty:
                warning_text = " ".join(pdf_warnings) if pdf_warnings else "No rows were extracted."
                return HTMLResponse(
                    f'<div class="text-red-600 p-4">PDF processing stopped before usable rows were extracted. {warning_text} Try splitting the PDF into fewer pages and uploading again.</div>'
                )
            segments_data.append({
                "segment_key": seg_result.segment_key,
                "sheet_name": None,
                "raw_headers": list(seg_result.cleaned_df.columns),
                "mappings": seg_result.mapping_result.mappings,
                "confidence": seg_result.mapping_result.confidence,
                "source": seg_result.mapping_result.source,
                "file_type": seg_result.fingerprint.file_type,
                "schema_fields": _schema_fields_for_type(seg_result.fingerprint.file_type),
                "preview_rows": _safe_json_rows(seg_result.cleaned_df),
                "row_count": len(seg_result.cleaned_df),
                "voided": False,
                "warnings": pdf_warnings,
            })
            # Store segment results for later save
            _jobs[job_id] = {
                "segments": [seg_result],
                "raw_dfs": {seg_result.segment_key: seg_result.cleaned_df.copy()},
                "tmp_path": tmp_path,
                "filename": file.filename,
                "status": "mapped",
                "broker": None,
            }
        else:
            # Excel/CSV pipeline
            from comp_engine import get_sheet_names, robust_load_file_segmented
            sheets = get_sheet_names(tmp_path)
            all_segments = []
            raw_dfs: dict[str, pd.DataFrame] = {}

            for sheet in sheets:
                raw_segments = robust_load_file_segmented(tmp_path, sheet_name=sheet)
                for i, seg in enumerate(raw_segments):
                    seg_df = seg.get("df", seg) if isinstance(seg, dict) else seg.df if hasattr(seg, 'df') else pd.DataFrame()
                    if isinstance(seg_df, pd.DataFrame) and not seg_df.empty:
                        seg_key = f"{sheet or 'root'}::{i}"
                        mapping_result = run_mapping_stage(
                            seg_df, file.filename, sheet, store
                        )
                        from engine.types import SegmentResult
                        seg_result = SegmentResult(
                            segment_key=seg_key,
                            fingerprint=mapping_result.fingerprint,
                            mapping_result=mapping_result,
                            cleaned_df=mapping_result.cleaned_df,
                        )
                        all_segments.append(seg_result)
                        raw_dfs[seg_key] = seg_df.copy()
                        segments_data.append({
                            "segment_key": seg_key,
                            "sheet_name": sheet,
                            "raw_headers": list(seg_df.columns),
                            "mappings": mapping_result.mappings,
                            "confidence": mapping_result.confidence,
                            "source": mapping_result.source,
                            "file_type": mapping_result.fingerprint.file_type,
                            "schema_fields": _schema_fields_for_type(mapping_result.fingerprint.file_type),
                            "preview_rows": _safe_json_rows(seg_df),
                            "row_count": len(seg_df),
                            "voided": False,
                        })

            # Broker detection: only auto-detect from known broker keywords in filename,
            # not from row data (which caused garbage like "file.xlsx 8450 152.23 Hold").
            # Let the user enter/confirm the broker manually.
            broker_resolution = None

            _jobs[job_id] = {
                "segments": all_segments,
                "raw_dfs": raw_dfs,
                "tmp_path": tmp_path,
                "filename": file.filename,
                "status": "mapped",
                "broker": broker_resolution,
            }

    except Exception as e:
        return HTMLResponse(f'<div class="text-red-600 p-4">Error processing file: {e}</div>')

    # Audit layer: check each segment's mapping against its data before the
    # analyst edits (advisory, best-effort, degrades to no flags).
    try:
        _attach_audit_flags(_jobs[job_id], segments_data)
    except Exception:
        log.warning("mapping audit layer failed; continuing without flags", exc_info=True)

    # Determine schema fields for mapping dropdowns
    first_type = segments_data[0]["file_type"] if segments_data else "sale"
    schema_fields = _schema_fields_for_type(first_type)

    broker_resolution = _jobs[job_id].get("broker")
    broker_name = ""
    if broker_resolution:
        broker_name = (getattr(broker_resolution, "broker_name", None)
                       or getattr(broker_resolution, "candidate_name", None)
                       or "")

    preview_state = {
        "jobId": job_id,
        "segments": segments_data,
        "schemaFields": schema_fields,
        "schemaFieldsByType": {
            "sale": list(SALE_SCHEMA.keys()),
            "lease": list(LEASE_SCHEMA.keys()),
        },
        "brokerName": broker_name,
    }
    return templates.TemplateResponse(request, "partials/upload_preview.html", {
        "request": request,
        "preview_state": preview_state,
        "broker": broker_resolution,
        "filename": file.filename,
    })


@router.post("/mapping")
async def update_mapping(request: Request):
    """Deprecated: mappings are now managed client-side. Kept as no-op for backward compat."""
    return {"status": "deprecated"}


@router.post("/apply-mappings")
async def apply_mappings(request: Request):
    """Apply client-submitted mappings to job state before geocoding.

    Rebuilds each segment's cleaned_df from the stored raw_df using the new
    mappings. 'notes' target supports multi-source concatenation.
    """
    body = await request.json()
    job_id = body.get("job_id")
    job = _jobs.get(job_id)
    if not job:
        return {"error": "Session expired. Please re-upload."}
    final_mappings = body.get("final_mappings", {})
    final_types = body.get("final_types", {}) or {}
    voided_segment_keys = set(body.get("voided_segment_keys") or [])
    broker_name = body.get("broker_name", "")
    raw_dfs = job.get("raw_dfs", {})
    job["voided_segment_keys"] = list(voided_segment_keys)
    active_segments = _active_segments(job)
    if not active_segments:
        return {"error": "All sheets are voided. Restore at least one sheet before saving."}

    for seg in active_segments:
        # Per-segment type override (user toggled SALE <-> LEASE in the preview).
        # Applied BEFORE the cleaned_df is rebuilt so rate logic, dedupe identity,
        # source_type, table routing, and learning all use the corrected type.
        requested_type = str(final_types.get(seg.segment_key, "")).lower()
        if requested_type in ("sale", "lease"):
            seg.fingerprint.file_type = requested_type

        if seg.segment_key not in final_mappings:
            continue
        new_maps = final_mappings[seg.segment_key]
        # Drop any "unmapped"/"---"/empty values
        new_maps = {k: v for k, v in new_maps.items() if v and v not in ("---", "unmapped")}
        raw_df = raw_dfs.get(seg.segment_key)
        raw_headers = list(raw_df.columns) if raw_df is not None else list(new_maps.keys())
        new_maps = dedupe_mappings_by_target(new_maps, raw_headers)
        if not hasattr(seg.mapping_result, "original_mappings"):
            seg.mapping_result.original_mappings = dict(seg.mapping_result.mappings)
        seg.mapping_result.mappings = new_maps

        if raw_df is not None:
            cleaned_df = _apply_mappings_with_notes_concat(raw_df, new_maps)
            if seg.fingerprint.file_type.lower() in ("lease", "both"):
                cleaned_df = apply_rate_logic(cleaned_df, rate_header=_rate_header_for_mapping(new_maps))
            seg.cleaned_df = cleaned_df

    deduped_count = _dedupe_segments_within_sheets(active_segments)
    if deduped_count:
        job["deduped_count"] = job.get("deduped_count", 0) + deduped_count

    if broker_name:
        job["confirmed_broker"] = broker_name
    return {"status": "ok"}


@router.post("/geocode")
async def start_geocode(request: Request):
    """Start geocoding in a background thread, return job_id."""
    form = await request.form()
    job_id = form.get("job_id")
    broker_name = form.get("broker_name", "")
    job = _jobs.get(job_id)
    if not job:
        return {"error": "Session expired"}
    active_segments = _active_segments(job)
    if not active_segments:
        return {"error": "All sheets are voided. Restore at least one sheet before geocoding."}
    if job.get("status") == "geocoding":
        return {"job_id": job_id, "status": "geocoding"}

    job["status"] = "geocoding"
    job["geocode_progress"] = {"current": 0, "total": 0, "address": ""}
    if broker_name:
        job["confirmed_broker"] = broker_name

    def _geocode_thread():
        try:
            store = get_learning_store()
            api_key = settings.GOOGLE_API_KEY
            from engine.geocoding import resolve_geocode, _normalize_raw
            from engine import openai_client

            active_segments = _active_segments(job)
            deduped_count = _dedupe_segments_within_sheets(active_segments)
            if deduped_count:
                job["deduped_count"] = job.get("deduped_count", 0) + deduped_count

            address_rows = []
            for seg in active_segments:
                df = seg.cleaned_df
                if "address" not in df.columns and "raw_address_data" not in df.columns:
                    continue
                address_rows.append((seg, df))
                job["geocode_progress"]["total"] += len(df)

            resolved_cache: dict[str, dict] = {}
            for seg, df in address_rows:
                lats, lngs, cities, zips, formatted_addresses = [], [], [], [], []
                for _, row in df.iterrows():
                    raw = _row_geocode_text(row)
                    job["geocode_progress"]["address"] = raw

                    if raw:
                        cache_key = _normalize_raw(raw).lower()
                        if cache_key not in resolved_cache:
                            resolved_cache[cache_key] = resolve_geocode(raw, api_key, store, openai_client)
                        result = resolved_cache[cache_key]
                    else:
                        result = {"source": "skipped", "latitude": None, "longitude": None}

                    lats.append(result.get("latitude"))
                    lngs.append(result.get("longitude"))
                    cities.append(result.get("city"))
                    zips.append(result.get("zip_code"))
                    formatted_addresses.append(result.get("formatted_address"))
                    job["geocode_progress"]["current"] += 1

                df["latitude"] = lats
                df["longitude"] = lngs
                if any(cities):
                    df["city"] = cities
                if any(zips):
                    df["zip_code"] = zips
                if any(formatted_addresses):
                    existing_addresses = df["address"] if "address" in df.columns else pd.Series([None] * len(df), index=df.index)
                    df["address"] = [
                        formatted or _clean_address_text(existing)
                        for formatted, existing in zip(formatted_addresses, existing_addresses)
                    ]
                seg.cleaned_df = df

            job["status"] = "geocoded"
        except Exception as exc:
            job["status"] = "error"
            job["geocode_error"] = str(exc)

    thread = threading.Thread(target=_geocode_thread, daemon=True)
    thread.start()
    return {"job_id": job_id, "status": "geocoding"}


@router.get("/geocode-stream")
async def geocode_stream(request: Request):
    """SSE stream for geocoding progress."""
    import asyncio
    job_id = request.query_params.get("job_id")
    job = _jobs.get(job_id)
    if not job:
        return HTMLResponse("Job not found", status_code=404)

    async def event_generator():
        while True:
            progress = job.get("geocode_progress", {})
            current = progress.get("current", 0)
            total = progress.get("total", 1) or 1
            address = progress.get("address", "")
            pct = int((current / total) * 100)
            data = json.dumps({"current": current, "total": total, "pct": pct, "address": address})
            yield f"data: {data}\n\n"

            if job.get("status") == "geocoded":
                yield f"data: {json.dumps({'done': True, 'current': current, 'total': total, 'pct': 100})}\n\n"
                break
            if job.get("status") == "error":
                yield f"data: {json.dumps({'done': True, 'error': job.get('geocode_error', 'Geocoding failed'), 'current': current, 'total': total, 'pct': pct})}\n\n"
                break
            await asyncio.sleep(0.5)

    return StreamingResponse(event_generator(), media_type="text/event-stream")


@router.post("/save", response_class=HTMLResponse)
async def save_to_db(request: Request):
    """Save geocoded data to database with learning."""
    form = await request.form()
    job_id = form.get("job_id")
    job = _jobs.get(job_id)
    if not job:
        return HTMLResponse('<div class="text-red-600">Session expired. Please re-upload.</div>')

    user = request.state.user
    store = get_learning_store()
    segments = _active_segments(job)
    if not segments:
        return HTMLResponse('<div class="text-red-600">All sheets are voided. Restore at least one sheet before saving.</div>')
    confirmed_broker = job.get("confirmed_broker", "")

    # Build final_mappings and edited_dfs
    final_mappings = {}
    edited_dfs = {}
    _dedupe_segments_within_sheets(segments)
    for seg in segments:
        final_mappings[seg.segment_key] = seg.mapping_result.mappings
        seg_df = seg.cleaned_df.copy()
        seg_df["source_type"] = seg.fingerprint.file_type.upper()
        seg_df["source_sheet"] = seg.fingerprint.sheet_name or seg.segment_key.split("::", 1)[0]
        edited_dfs[seg.segment_key] = seg_df

    # DB saver function — closes over file_type + filename, delegates to
    # _save_rows_to_db which performs the missing-address validation.
    file_type = segments[0].fingerprint.file_type if segments else "sale"
    save_stats: dict = {"skipped_count": 0}

    def db_saver(concat_df: pd.DataFrame) -> list[int]:
        result = _save_rows_to_db(
            concat_df,
            file_type=file_type,
            source_file=job.get("filename"),
        )
        save_stats["skipped_count"] = result["skipped_count"]
        return result["inserted_ids"]

    try:
        inserted_ids = persist_with_learning(
            segments=segments,
            final_mappings=final_mappings,
            edited_dfs=edited_dfs,
            confirmed_broker=confirmed_broker if confirmed_broker else None,
            geocode_overrides={},
            store=store,
            db_saver=db_saver,
            user=user.get("username", "unknown"),
        )
        # Cleanup
        _jobs.pop(job_id, None)
        skipped = save_stats.get("skipped_count", 0)
        skipped_banner = ""
        if skipped:
            skipped_banner = (
                f'<div class="text-amber-700 text-sm mt-2">'
                f'Skipped {skipped} row(s) with missing address — '
                f'address is required for geocoding and matching.</div>'
            )
        return HTMLResponse(f'''
            <div class="bg-green-50 border border-green-200 rounded-lg p-6 text-center">
                <div class="text-green-700 font-semibold text-lg mb-2">
                    Successfully saved {len(inserted_ids)} records
                </div>
                <p class="text-green-600 text-sm mb-4">Data has been added to the database.</p>
                {skipped_banner}
                <a href="/database" class="btn-primary inline-block">View in Database</a>
            </div>
        ''')
    except Exception as e:
        return HTMLResponse(f'<div class="text-red-600 p-4">Error saving: {e}</div>')
