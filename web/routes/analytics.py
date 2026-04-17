"""Analytics page with 6 chart tabs."""
import json
import math
import pandas as pd
from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse
from database import Session, SaleComp, LeaseComp

router = APIRouter(prefix="/analytics", tags=["analytics"])


def _load_data(session, comp_type: str) -> pd.DataFrame:
    Model = SaleComp if comp_type == "sales" else LeaseComp
    records = session.query(Model).all()
    if not records:
        return pd.DataFrame()
    rows = [{c.name: getattr(r, c.name) for c in Model.__table__.columns} for r in records]
    return pd.DataFrame(rows)


def _safe_val(v):
    if v is None:
        return None
    if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
        return None
    return v


def _compute_chart_data(df: pd.DataFrame, comp_type: str) -> dict:
    """Pre-aggregate all chart data in Python, pass as JSON to templates."""
    charts = {}

    if df.empty:
        return charts

    # Distribution data
    if comp_type == "sales":
        for col, label in [("sale_price", "Sale Price"), ("building_size", "Building Size (SF)"), ("price_per_sf", "Price per SF")]:
            if col in df.columns:
                vals = pd.to_numeric(df[col], errors="coerce").dropna().tolist()
                charts[f"dist_{col}"] = {"values": vals, "label": label}
    else:
        for col, label in [("rate_monthly", "Rate Monthly"), ("leased_sf", "Leased SF"), ("rate_annually", "Rate Annually")]:
            if col in df.columns:
                vals = pd.to_numeric(df[col], errors="coerce").dropna().tolist()
                charts[f"dist_{col}"] = {"values": vals, "label": label}

    # Price vs Size scatter
    if comp_type == "sales":
        x_col, y_col = "building_size", "sale_price"
    else:
        x_col, y_col = "leased_sf", "rate_monthly"
    if x_col in df.columns and y_col in df.columns:
        valid = df[[x_col, y_col]].dropna()
        charts["scatter"] = {
            "x": pd.to_numeric(valid[x_col], errors="coerce").tolist(),
            "y": pd.to_numeric(valid[y_col], errors="coerce").tolist(),
            "labels": df.loc[valid.index, "address"].tolist() if "address" in df.columns else [],
            "x_label": x_col.replace("_", " ").title(),
            "y_label": y_col.replace("_", " ").title(),
        }

    # Trends (time series)
    date_col = "closing_date" if comp_type == "sales" else "commencement_date"
    val_col = "sale_price" if comp_type == "sales" else "rate_monthly"
    if date_col in df.columns and val_col in df.columns:
        temp = df[[date_col, val_col]].copy()
        temp[date_col] = pd.to_datetime(temp[date_col], errors="coerce")
        temp[val_col] = pd.to_numeric(temp[val_col], errors="coerce")
        temp = temp.dropna()
        if not temp.empty:
            temp = temp.sort_values(date_col)
            charts["trends"] = {
                "dates": temp[date_col].dt.strftime("%Y-%m-%d").tolist(),
                "values": temp[val_col].tolist(),
                "label": val_col.replace("_", " ").title(),
            }

    # By Zip Code
    val_col = "sale_price" if comp_type == "sales" else "rate_monthly"
    if "zip_code" in df.columns and val_col in df.columns:
        temp = df[["zip_code", val_col]].copy()
        temp[val_col] = pd.to_numeric(temp[val_col], errors="coerce")
        grouped = temp.groupby("zip_code")[val_col].agg(["mean", "count"]).reset_index()
        grouped = grouped.sort_values("count", ascending=False).head(20)
        charts["by_zip"] = {
            "zips": grouped["zip_code"].tolist(),
            "means": [_safe_val(v) for v in grouped["mean"].tolist()],
            "counts": grouped["count"].tolist(),
            "label": f"Avg {val_col.replace('_', ' ').title()}",
        }

    # Map points
    if "latitude" in df.columns and "longitude" in df.columns:
        points = []
        for _, row in df.iterrows():
            if pd.notna(row.get("latitude")) and pd.notna(row.get("longitude")):
                popup = f"<b>{row.get('address', 'N/A')}</b>"
                if comp_type == "sales" and pd.notna(row.get("sale_price")):
                    popup += f"<br>${row['sale_price']:,.0f}"
                elif pd.notna(row.get("rate_monthly")):
                    popup += f"<br>${row['rate_monthly']:,.2f}/mo"
                points.append({"lat": float(row["latitude"]), "lng": float(row["longitude"]), "popup": popup})
        charts["map_points"] = points

    return charts


def _compute_metrics(df: pd.DataFrame, comp_type: str) -> dict:
    if df.empty:
        return {"count": 0}
    metrics = {"count": len(df)}
    if comp_type == "sales":
        metrics["avg_price"] = _safe_val(pd.to_numeric(df.get("sale_price"), errors="coerce").mean())
        metrics["avg_psf"] = _safe_val(pd.to_numeric(df.get("price_per_sf"), errors="coerce").mean())
        metrics["avg_size"] = _safe_val(pd.to_numeric(df.get("building_size"), errors="coerce").mean())
    else:
        metrics["avg_rate_monthly"] = _safe_val(pd.to_numeric(df.get("rate_monthly"), errors="coerce").mean())
        metrics["avg_rate_annually"] = _safe_val(pd.to_numeric(df.get("rate_annually"), errors="coerce").mean())
        metrics["avg_size"] = _safe_val(pd.to_numeric(df.get("leased_sf"), errors="coerce").mean())
    return metrics


@router.get("", response_class=HTMLResponse)
async def analytics_page(request: Request):
    templates = request.app.state.templates
    user = request.state.user
    session = Session()
    try:
        comp_type = request.query_params.get("type", "sales")
        df = _load_data(session, comp_type)
        sale_count = session.query(SaleComp).count()
        lease_count = session.query(LeaseComp).count()
        metrics = _compute_metrics(df, comp_type)
        charts = _compute_chart_data(df, comp_type)
        return templates.TemplateResponse(request, "analytics.html", {
            "request": request,
            "user": user,
            "current_page": "analytics",
            "logo_b64": request.app.state.logo_b64,
            "icon_b64": request.app.state.icon_b64,
            "comp_type": comp_type,
            "sale_count": sale_count,
            "lease_count": lease_count,
            "metrics": metrics,
            "charts": json.dumps(charts),
        })
    finally:
        session.close()
