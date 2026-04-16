"""Upload & Process page and HTMX endpoints."""
import os
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

from database import Session, SaleComp, LeaseComp
from engine.pipeline import run_mapping_stage, run_geocoding_stage, run_vision_pdf_stage
from engine.brokers import resolve_broker
from engine.mapping import SALE_SCHEMA, LEASE_SCHEMA
from learning.corrections import persist_with_learning
from web.config import settings
from web.dependencies import get_learning_store

router = APIRouter(prefix="/upload", tags=["upload"])

# Server-side job storage
_jobs: dict[str, dict] = {}


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


@router.get("", response_class=HTMLResponse)
async def upload_page(request: Request):
    templates = request.app.state.templates
    user = request.state.user
    return templates.TemplateResponse("upload.html", {
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
            seg_result = run_vision_pdf_stage(tmp_path, file.filename)
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
            })
            # Store segment results for later save
            _jobs[job_id] = {
                "segments": [seg_result],
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

            # Broker detection from first segment
            broker_resolution = None
            if all_segments:
                first_df = all_segments[0].cleaned_df
                # Try to extract broker from filename or first few cells
                sample_text = file.filename + " " + " ".join(str(v) for v in first_df.iloc[0].values[:5] if pd.notna(v))
                broker_resolution = resolve_broker(sample_text, store)

            _jobs[job_id] = {
                "segments": all_segments,
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

    return templates.TemplateResponse("partials/upload_preview.html", {
        "request": request,
        "job_id": job_id,
        "segments": segments_data,
        "schema_fields": schema_fields,
        "broker": _jobs[job_id].get("broker"),
        "filename": file.filename,
    })


@router.post("/mapping", response_class=HTMLResponse)
async def update_mapping(request: Request):
    """Re-apply user-edited column mappings."""
    templates = request.app.state.templates
    form = await request.form()
    job_id = form.get("job_id")
    job = _jobs.get(job_id)
    if not job:
        return HTMLResponse('<div class="text-red-600">Session expired. Please re-upload.</div>')

    # Parse mapping overrides from form
    new_mappings = {}
    for key, value in form.items():
        if key.startswith("mapping__"):
            parts = key.split("__", 2)  # mapping__segkey__rawheader
            if len(parts) == 3:
                seg_key, raw_header = parts[1], parts[2]
                if seg_key not in new_mappings:
                    new_mappings[seg_key] = {}
                if value and value != "---":
                    new_mappings[seg_key][raw_header] = value

    # Update stored mappings
    for seg in job["segments"]:
        if seg.segment_key in new_mappings:
            seg.mapping_result.mappings = new_mappings[seg.segment_key]
            # Re-apply mappings to get cleaned_df
            new_df = pd.DataFrame()
            for raw, target in new_mappings[seg.segment_key].items():
                if raw in seg.mapping_result.fingerprint.headers:
                    # Find original data
                    pass  # cleaned_df already has target columns
            seg.mapping_result.mappings = new_mappings[seg.segment_key]

    # Rebuild segments_data for template
    segments_data = []
    for seg in job["segments"]:
        first_type = seg.fingerprint.file_type
        segments_data.append({
            "segment_key": seg.segment_key,
            "sheet_name": seg.fingerprint.sheet_name,
            "raw_headers": seg.mapping_result.fingerprint.headers,
            "mappings": seg.mapping_result.mappings,
            "confidence": seg.mapping_result.confidence,
            "source": seg.mapping_result.source,
            "file_type": seg.fingerprint.file_type,
            "preview_rows": _safe_json_rows(seg.cleaned_df),
            "row_count": len(seg.cleaned_df),
        })

    first_type = segments_data[0]["file_type"] if segments_data else "sale"
    schema_fields = list(LEASE_SCHEMA.keys()) if first_type.lower() in ("lease", "both") else list(SALE_SCHEMA.keys())

    return templates.TemplateResponse("partials/upload_preview.html", {
        "request": request,
        "job_id": job_id,
        "segments": segments_data,
        "schema_fields": schema_fields,
        "broker": job.get("broker"),
        "filename": job["filename"],
    })


@router.post("/geocode")
async def start_geocode(request: Request):
    """Start geocoding in a background thread, return job_id."""
    form = await request.form()
    job_id = form.get("job_id")
    broker_name = form.get("broker_name", "")
    job = _jobs.get(job_id)
    if not job:
        return {"error": "Session expired"}

    job["status"] = "geocoding"
    job["geocode_progress"] = {"current": 0, "total": 0, "address": ""}
    if broker_name:
        job["confirmed_broker"] = broker_name

    def _geocode_thread():
        store = get_learning_store()
        api_key = settings.GOOGLE_API_KEY
        for seg in job["segments"]:
            df = seg.cleaned_df
            if "address" not in df.columns:
                continue
            total = len(df)
            job["geocode_progress"]["total"] += total

            # Geocode row by row for progress
            from engine.geocoding import resolve_geocode
            from engine import openai_client
            lats, lngs = [], []
            for idx, row in df.iterrows():
                raw = str(row.get("address", ""))
                job["geocode_progress"]["address"] = raw
                result = resolve_geocode(raw, api_key, store, openai_client)
                lats.append(result.get("latitude"))
                lngs.append(result.get("longitude"))
                job["geocode_progress"]["current"] += 1

            df["latitude"] = lats
            df["longitude"] = lngs
            seg.cleaned_df = df

        job["status"] = "geocoded"

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
            ids = []
            for _, row in concat_df.iterrows():
                record = Model()
                for col in Model.__table__.columns:
                    if col.name != "id" and col.name != "created_at" and col.name in row.index:
                        val = row[col.name]
                        if pd.notna(val):
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
