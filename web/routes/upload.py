"""Upload & Process page and HTMX endpoints."""
import os
import re
import json
import uuid
import time
import math
import tempfile
import threading
from typing import Optional

import pandas as pd
from fastapi import APIRouter, Request, UploadFile, File, Form
from fastapi.responses import HTMLResponse, StreamingResponse
from sqlalchemy import Float, Integer, Numeric

from database import Session, SaleComp, LeaseComp
from engine.pipeline import run_mapping_stage, run_geocoding_stage, run_vision_pdf_stage
from engine.brokers import resolve_broker
from engine.cleaning import apply_rate_logic
from engine.mapping import SALE_SCHEMA, LEASE_SCHEMA, dedupe_mappings_by_target
from learning.corrections import persist_with_learning
from web.config import settings
from web.dependencies import get_learning_store

router = APIRouter(prefix="/upload", tags=["upload"])

# Server-side job storage
_jobs: dict[str, dict] = {}


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


def _row_geocode_text(row) -> str:
    """Prefer richer combined address text when the pipeline produced it."""
    for col in ("raw_address_data", "address"):
        text = _clean_address_text(row.get(col))
        if text:
            return text
    return ""


def _rate_header_for_mapping(mappings: dict[str, str]) -> Optional[str]:
    return next((raw for raw, target in mappings.items() if target == "rate_psf"), None)


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
                "preview_rows": _safe_json_rows(seg_result.cleaned_df),
                "row_count": len(seg_result.cleaned_df),
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
                            "preview_rows": _safe_json_rows(seg_df),
                            "row_count": len(seg_df),
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

    # Determine schema fields for mapping dropdowns
    first_type = segments_data[0]["file_type"] if segments_data else "sale"
    schema_fields = list(LEASE_SCHEMA.keys()) if first_type.lower() in ("lease", "both") else list(SALE_SCHEMA.keys())

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
    broker_name = body.get("broker_name", "")
    raw_dfs = job.get("raw_dfs", {})

    for seg in job["segments"]:
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

            address_rows = []
            for seg in job["segments"]:
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
    segments = job["segments"]
    confirmed_broker = job.get("confirmed_broker", "")

    # Build final_mappings and edited_dfs
    final_mappings = {}
    edited_dfs = {}
    for seg in segments:
        final_mappings[seg.segment_key] = seg.mapping_result.mappings
        edited_dfs[seg.segment_key] = seg.cleaned_df

    # DB saver function
    def db_saver(concat_df: pd.DataFrame) -> list[int]:
        session = Session()
        try:
            file_type = segments[0].fingerprint.file_type if segments else "sale"
            Model = LeaseComp if file_type.lower() in ("lease", "both") else SaleComp
            numeric_cols = {
                c.name for c in Model.__table__.columns
                if isinstance(c.type, (Float, Integer, Numeric))
            }
            ids = []
            for _, row in concat_df.iterrows():
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
                record.source_file = job["filename"]
                session.add(record)
                session.flush()
                ids.append(record.id)
            session.commit()
            return ids
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

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
        return HTMLResponse(f'''
            <div class="bg-green-50 border border-green-200 rounded-lg p-6 text-center">
                <div class="text-green-700 font-semibold text-lg mb-2">
                    Successfully saved {len(inserted_ids)} records
                </div>
                <p class="text-green-600 text-sm mb-4">Data has been added to the database.</p>
                <a href="/database" class="btn-primary inline-block">View in Database</a>
            </div>
        ''')
    except Exception as e:
        return HTMLResponse(f'<div class="text-red-600 p-4">Error saving: {e}</div>')
