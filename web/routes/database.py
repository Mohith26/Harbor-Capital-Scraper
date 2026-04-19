"""Database View page and HTMX endpoints."""
import io
import json
import pandas as pd
from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, StreamingResponse
from database import Session, SaleComp, LeaseComp

router = APIRouter(prefix="/database", tags=["database"])

def _load_data(session, comp_type: str) -> pd.DataFrame:
    Model = SaleComp if comp_type == "sales" else LeaseComp
    records = session.query(Model).all()
    if not records:
        return pd.DataFrame()
    rows = [{c.name: getattr(r, c.name) for c in Model.__table__.columns} for r in records]
    return pd.DataFrame(rows)

def _apply_filters(df: pd.DataFrame, filters: dict, comp_type: str) -> pd.DataFrame:
    if not filters or df.empty:
        return df
    search = filters.get("search", "").strip()
    if search:
        search_cols = ["address"]
        if comp_type == "sales":
            search_cols += ["buyer", "seller", "notes"]
        else:
            search_cols += ["tenant_name", "notes"]
        mask = pd.Series(False, index=df.index)
        for col in search_cols:
            if col in df.columns:
                mask |= df[col].astype(str).str.contains(search, case=False, na=False, regex=False)
        df = df[mask]
    for key in ["city", "zip_code"]:
        vals = filters.get(key)
        if vals:
            df = df[df[key].isin(vals)]
    numeric_filters = {
        "min_sale_price": ("sale_price", ">="), "max_sale_price": ("sale_price", "<="),
        "min_price_per_sf": ("price_per_sf", ">="), "max_price_per_sf": ("price_per_sf", "<="),
        "min_building_size": ("building_size", ">="), "max_building_size": ("building_size", "<="),
        "min_rate_monthly": ("rate_monthly", ">="), "max_rate_monthly": ("rate_monthly", "<="),
    }
    for fkey, (col, op) in numeric_filters.items():
        val = filters.get(fkey)
        if val is not None and col in df.columns:
            if op == ">=":
                df = df[pd.to_numeric(df[col], errors="coerce") >= float(val)]
            else:
                df = df[pd.to_numeric(df[col], errors="coerce") <= float(val)]
    for date_col in ["closing_date", "commencement_date"]:
        if date_col in df.columns:
            min_date = filters.get(f"min_{date_col}")
            max_date = filters.get(f"max_{date_col}")
            if min_date or max_date:
                dates = pd.to_datetime(df[date_col], errors="coerce")
                if min_date:
                    df = df[dates >= pd.to_datetime(min_date)]
                if max_date:
                    df = df[dates <= pd.to_datetime(max_date)]
    return df

def _compute_metrics(df: pd.DataFrame, comp_type: str) -> dict:
    if df.empty:
        return {"count": 0, "avg_price": None, "avg_psf": None, "avg_size": None}
    if comp_type == "sales":
        return {
            "count": len(df),
            "avg_price": pd.to_numeric(df.get("sale_price"), errors="coerce").mean(),
            "avg_psf": pd.to_numeric(df.get("price_per_sf"), errors="coerce").mean(),
            "avg_size": pd.to_numeric(df.get("building_size"), errors="coerce").mean(),
        }
    return {
        "count": len(df),
        "avg_rate_monthly": pd.to_numeric(df.get("rate_monthly"), errors="coerce").mean(),
        "avg_rate_annually": pd.to_numeric(df.get("rate_annually"), errors="coerce").mean(),
        "avg_size": pd.to_numeric(df.get("leased_sf"), errors="coerce").mean(),
    }

def _parse_filters(request: Request) -> dict:
    params = dict(request.query_params)
    filters = {}
    if "search" in params:
        filters["search"] = params["search"]
    for key in ["city", "zip_code"]:
        val = params.get(key)
        if val:
            filters[key] = val.split(",")
    for key in ["min_sale_price", "max_sale_price", "min_price_per_sf", "max_price_per_sf",
                 "min_building_size", "max_building_size", "min_rate_monthly", "max_rate_monthly",
                 "min_closing_date", "max_closing_date", "min_commencement_date", "max_commencement_date"]:
        val = params.get(key)
        if val:
            filters[key] = val
    return filters

def _build_active_filters(filters: dict) -> list:
    """Build list of (key, display_label) tuples for filter chips."""
    chips = []
    for city in filters.get("city", []):
        chips.append(("city", f"City: {city}"))
    for z in filters.get("zip_code", []):
        chips.append(("zip_code", f"Zip: {z}"))
    label_map = {
        "min_sale_price": "Min Price", "max_sale_price": "Max Price",
        "min_price_per_sf": "Min $/SF", "max_price_per_sf": "Max $/SF",
        "min_building_size": "Min Size", "max_building_size": "Max Size",
        "min_rate_monthly": "Min Rate", "max_rate_monthly": "Max Rate",
    }
    for key, label in label_map.items():
        val = filters.get(key)
        if val is not None:
            chips.append((key, f"{label}: {val}"))
    return chips

def _safe_json(table_data):
    """Convert table data to JSON-safe format (handle NaN, NaT, etc.)."""
    import math
    clean = []
    for row in table_data:
        clean_row = {}
        for k, v in row.items():
            if v is None:
                clean_row[k] = None
            elif isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
                clean_row[k] = None
            elif hasattr(v, 'isoformat'):
                clean_row[k] = v.isoformat()
            else:
                clean_row[k] = v
        clean.append(clean_row)
    return clean

@router.get("", response_class=HTMLResponse)
async def database_page(request: Request):
    templates = request.app.state.templates
    user = request.state.user
    session = Session()
    try:
        comp_type = request.query_params.get("type", "sales")
        filters = _parse_filters(request)
        df = _load_data(session, comp_type)
        total = len(df)
        df_filtered = _apply_filters(df, filters, comp_type)
        sale_count = session.query(SaleComp).count()
        lease_count = session.query(LeaseComp).count()
        metrics = _compute_metrics(df_filtered, comp_type)
        table_data = _safe_json(df_filtered.to_dict(orient="records")) if not df_filtered.empty else []
        columns = list(df_filtered.columns) if not df_filtered.empty else []
        Model = SaleComp if comp_type == "sales" else LeaseComp
        cities = sorted([r[0] for r in session.query(Model.city).distinct().all() if r[0]])
        zips = sorted([r[0] for r in session.query(Model.zip_code).distinct().all() if r[0]])
        return templates.TemplateResponse(request, "database.html", {
            "request": request,
            "user": user,
            "current_page": "database",
            "logo_b64": request.app.state.logo_b64,
            "icon_b64": request.app.state.icon_b64,
            "comp_type": comp_type,
            "sale_count": sale_count,
            "lease_count": lease_count,
            "metrics": metrics,
            "table_data": json.dumps(table_data),
            "total": total,
            "filtered": len(df_filtered),
            "filters": filters,
            "columns": json.dumps(columns),
            "cities": cities,
            "zips": zips,
            "active_cities": filters.get("city", []),
            "active_zips": filters.get("zip_code", []),
            "active_filters": _build_active_filters(filters),
        })
    finally:
        session.close()

@router.get("/table", response_class=HTMLResponse)
async def database_table(request: Request):
    templates = request.app.state.templates
    session = Session()
    try:
        comp_type = request.query_params.get("type", "sales")
        filters = _parse_filters(request)
        df = _load_data(session, comp_type)
        total = len(df)
        df_filtered = _apply_filters(df, filters, comp_type)
        table_data = _safe_json(df_filtered.to_dict(orient="records")) if not df_filtered.empty else []
        columns = list(df_filtered.columns) if not df_filtered.empty else []
        return templates.TemplateResponse(request, "partials/data_table.html", {
            "request": request,
            "table_data": json.dumps(table_data),
            "total": total,
            "filtered": len(df_filtered),
            "comp_type": comp_type,
            "columns": json.dumps(columns),
        })
    finally:
        session.close()

@router.get("/metrics", response_class=HTMLResponse)
async def database_metrics(request: Request):
    templates = request.app.state.templates
    session = Session()
    try:
        comp_type = request.query_params.get("type", "sales")
        filters = _parse_filters(request)
        df = _load_data(session, comp_type)
        df_filtered = _apply_filters(df, filters, comp_type)
        sale_count = session.query(SaleComp).count()
        lease_count = session.query(LeaseComp).count()
        metrics = _compute_metrics(df_filtered, comp_type)
        return templates.TemplateResponse(request, "partials/metrics_row.html", {
            "request": request,
            "sale_count": sale_count,
            "lease_count": lease_count,
            "metrics": metrics,
            "comp_type": comp_type,
        })
    finally:
        session.close()

@router.post("/export")
async def database_export(request: Request):
    body = await request.json()
    fmt = body.get("format", "csv")
    ids = body.get("ids", [])
    comp_type = body.get("type", "sales")
    session = Session()
    try:
        df = _load_data(session, comp_type)
        if ids:
            df = df[df["id"].isin(ids)]
        if df.empty:
            return StreamingResponse(io.BytesIO(b"No data"), media_type="text/plain")
        if fmt == "csv":
            buffer = io.StringIO()
            df.to_csv(buffer, index=False)
            return StreamingResponse(
                io.BytesIO(buffer.getvalue().encode()),
                media_type="text/csv",
                headers={"Content-Disposition": f"attachment; filename={comp_type}_comps.csv"},
            )
        elif fmt == "xlsx":
            buffer = io.BytesIO()
            df.to_excel(buffer, index=False, engine="xlsxwriter")
            buffer.seek(0)
            return StreamingResponse(
                buffer,
                media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                headers={"Content-Disposition": f"attachment; filename={comp_type}_comps.xlsx"},
            )
        elif fmt == "kml":
            from comp_engine import generate_kml
            kml_str = generate_kml(df)
            return StreamingResponse(
                io.BytesIO(kml_str.encode()),
                media_type="application/vnd.google-earth.kml+xml",
                headers={"Content-Disposition": f"attachment; filename={comp_type}_comps.kml"},
            )
    finally:
        session.close()

@router.delete("/records")
async def delete_records(request: Request):
    user = request.state.user
    if user.get("role") != "admin":
        return HTMLResponse("Forbidden", status_code=403)
    body = await request.json()
    ids = body.get("ids", [])
    comp_type = body.get("type", "sales")
    Model = SaleComp if comp_type == "sales" else LeaseComp
    session = Session()
    try:
        session.query(Model).filter(Model.id.in_(ids)).delete(synchronize_session=False)
        session.commit()
        return HTMLResponse(f'<div class="text-green-700 text-sm p-2">Deleted {len(ids)} records.</div>')
    except Exception as e:
        session.rollback()
        return HTMLResponse(f'<div class="text-red-700 text-sm p-2">Error: {e}</div>')
    finally:
        session.close()

@router.get("/count")
async def database_count(request: Request):
    """Return filtered record count as JSON (for live filter preview)."""
    session = Session()
    try:
        comp_type = request.query_params.get("type", "sales")
        filters = _parse_filters(request)
        df = _load_data(session, comp_type)
        df_filtered = _apply_filters(df, filters, comp_type)
        return {"count": len(df_filtered), "total": len(df)}
    finally:
        session.close()

@router.get("/filter-options", response_class=HTMLResponse)
async def filter_options(request: Request):
    """Return distinct values for categorical filters."""
    session = Session()
    try:
        comp_type = request.query_params.get("type", "sales")
        Model = SaleComp if comp_type == "sales" else LeaseComp
        cities = sorted([r[0] for r in session.query(Model.city).distinct().all() if r[0]])
        zips = sorted([r[0] for r in session.query(Model.zip_code).distinct().all() if r[0]])
        return HTMLResponse(json.dumps({"cities": cities, "zips": zips}))
    finally:
        session.close()

@router.get("/filter-panel", response_class=HTMLResponse)
async def filter_panel(request: Request):
    templates = request.app.state.templates
    session = Session()
    try:
        comp_type = request.query_params.get("type", "sales")
        filters = _parse_filters(request)
        Model = SaleComp if comp_type == "sales" else LeaseComp
        cities = sorted([r[0] for r in session.query(Model.city).distinct().all() if r[0]])
        zips = sorted([r[0] for r in session.query(Model.zip_code).distinct().all() if r[0]])
        return templates.TemplateResponse(request, "partials/filter_panel.html", {
            "request": request,
            "comp_type": comp_type,
            "cities": cities,
            "zips": zips,
            "active_cities": filters.get("city", []),
            "active_zips": filters.get("zip_code", []),
            "filters": filters,
        })
    finally:
        session.close()

@router.get("/map-data")
async def map_data(request: Request):
    """Return GeoJSON-like data for map markers."""
    session = Session()
    try:
        comp_type = request.query_params.get("type", "sales")
        filters = _parse_filters(request)
        df = _load_data(session, comp_type)
        df = _apply_filters(df, filters, comp_type)
        points = []
        for _, row in df.iterrows():
            if pd.notna(row.get("latitude")) and pd.notna(row.get("longitude")):
                popup_parts = [f"<b>{row.get('address', 'N/A')}</b>"]
                if comp_type == "sales" and pd.notna(row.get("sale_price")):
                    popup_parts.append(f"Price: ${row['sale_price']:,.0f}")
                elif comp_type == "leases" and pd.notna(row.get("rate_monthly")):
                    popup_parts.append(f"Rate: ${row['rate_monthly']:,.2f}/mo")
                points.append({
                    "lat": float(row["latitude"]),
                    "lng": float(row["longitude"]),
                    "popup": "<br>".join(popup_parts),
                })
        return points
    finally:
        session.close()
