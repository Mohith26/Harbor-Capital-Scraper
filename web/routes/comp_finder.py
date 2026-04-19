"""Comp Finder page — search and ranked results."""
import json
import math
import pandas as pd
from fastapi import APIRouter, Request, Form
from fastapi.responses import HTMLResponse
from database import Session, SaleComp, LeaseComp
from web.config import settings
from web.dependencies import get_learning_store

router = APIRouter(prefix="/finder", tags=["finder"])


def _safe_val(v):
    if v is None:
        return None
    if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
        return None
    return v


@router.get("", response_class=HTMLResponse)
async def finder_page(request: Request):
    templates = request.app.state.templates
    user = request.state.user
    return templates.TemplateResponse(request, "comp_finder.html", {
        "request": request,
        "user": user,
        "current_page": "finder",
        "logo_b64": request.app.state.logo_b64,
        "icon_b64": request.app.state.icon_b64,
        "comp_type": request.query_params.get("type", "sales"),
    })


@router.post("/search", response_class=HTMLResponse)
async def search_comps(request: Request):
    """Run comp search and return results partial."""
    templates = request.app.state.templates
    form = await request.form()

    address = form.get("address", "").strip()
    comp_type = form.get("comp_type", "sales")
    max_radius = float(form.get("max_radius", 10))
    use_ai = form.get("use_ai") == "on"

    if not address:
        return HTMLResponse('<div class="text-red-600 text-sm p-4">Please enter an address.</div>')

    store = get_learning_store()

    # Geocode subject address
    from engine.geocoding import resolve_geocode
    from engine import openai_client
    geo = resolve_geocode(address, settings.GOOGLE_API_KEY, store, openai_client)

    if not geo.get("latitude") or not geo.get("longitude"):
        return HTMLResponse('<div class="text-red-600 text-sm p-4">Could not geocode address. Please try a more specific address.</div>')

    # Build subject dict
    subject = {
        "latitude": geo["latitude"],
        "longitude": geo["longitude"],
        "address": geo.get("formatted_address", address),
    }
    # Optional fields
    for field in ["building_size", "leased_sf", "sale_price", "rate_monthly", "year_built"]:
        val = form.get(field)
        if val:
            try:
                subject[field] = float(val)
            except ValueError:
                pass
    for field in ["city", "zip_code"]:
        val = form.get(field)
        if val:
            subject[field] = val

    # Load comps and compute scores
    from comp_finder import load_comps, compute_match_scores

    comps_df = load_comps(comp_type)
    if comps_df.empty:
        return HTMLResponse('<div class="text-gray-500 text-sm p-4">No comps in database for this type.</div>')

    # Weights from form
    weights = {}
    for w in ["distance", "size", "price", "age"]:
        val = form.get(f"weight_{w}")
        if val:
            try:
                weights[w] = float(val)
            except ValueError:
                pass

    scored = compute_match_scores(subject, comps_df, comp_type, weights, max_radius)

    score_col = "match_score"
    if use_ai:
        try:
            from comp_finder import compute_ai_scores, blend_scores
            ai_scored = compute_ai_scores(subject, scored, comp_type)
            scored["final_score"] = blend_scores(scored["match_score"], ai_scored, ai_weight=0.3)
            score_col = "final_score"
        except Exception as e:
            print(f"[finder] AI scoring failed, falling back to weighted: {e}")

    # Filter out comps beyond max_radius
    if "distance_miles" in scored.columns:
        scored = scored[scored["distance_miles"] <= max_radius]

    # Sort by score descending, take top 25
    scored = scored.sort_values(score_col, ascending=False).head(25)

    if scored.empty:
        return HTMLResponse(
            f'<div class="text-gray-500 text-sm p-4">No comparable properties found within '
            f'{max_radius:g} miles. Try increasing the radius or check that comps have '
            f'latitude/longitude set in the database.</div>'
        )

    # Build results for template
    results = []
    for idx, row in scored.iterrows():
        score = _safe_val(row.get(score_col, 0))
        result = {
            "rank": len(results) + 1,
            "address": row.get("address", "N/A"),
            "score": round(score * 100, 1) if score else 0,
            "lat": _safe_val(row.get("latitude")),
            "lng": _safe_val(row.get("longitude")),
        }
        if comp_type == "sales":
            result["price"] = _safe_val(row.get("sale_price"))
            result["size"] = _safe_val(row.get("building_size"))
            result["psf"] = _safe_val(row.get("price_per_sf"))
            result["date"] = row.get("closing_date", "")
        else:
            result["rate"] = _safe_val(row.get("rate_monthly"))
            result["size"] = _safe_val(row.get("leased_sf"))
            result["term"] = _safe_val(row.get("term_months"))
            result["date"] = row.get("commencement_date", "")
        results.append(result)

    return templates.TemplateResponse(request, "partials/comp_results.html", {
        "request": request,
        "results": results,
        "subject": subject,
        "comp_type": comp_type,
        "results_json": json.dumps(results),
        "subject_json": json.dumps(subject),
    })
