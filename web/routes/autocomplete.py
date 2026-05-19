"""Server-side address autocomplete via Google Places API (New), Texas-restricted."""
import logging
import httpx
from fastapi import APIRouter, Request
from sqlalchemy import or_, func as sa_func
from web.config import settings
from database import Session, SaleComp, LeaseComp

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api", tags=["autocomplete"])

# Texas bounding box (approximate). Used as a hard locationRestriction so only
# Texas results are returned.
_TX_BBOX_LOW = {"latitude": 25.8371, "longitude": -106.6456}   # SW corner
_TX_BBOX_HIGH = {"latitude": 36.5008, "longitude": -93.5083}   # NE corner


@router.get("/autocomplete")
async def autocomplete(request: Request, q: str = ""):
    """Return Google Places (New) autocomplete predictions for an address query.

    Uses Places API (New) v1. Hard-restricted to a Texas bounding box.
    """
    q = (q or "").strip()
    if len(q) < 3:
        return {"predictions": []}
    api_key = settings.GOOGLE_API_KEY
    if not api_key:
        logger.warning("autocomplete: GOOGLE_API_KEY not configured")
        return {"predictions": [], "error": "GOOGLE_API_KEY not configured"}

    url = "https://places.googleapis.com/v1/places:autocomplete"
    headers = {
        "Content-Type": "application/json",
        "X-Goog-Api-Key": api_key,
    }
    body = {
        "input": q,
        "includedRegionCodes": ["us"],
        "locationRestriction": {
            "rectangle": {"low": _TX_BBOX_LOW, "high": _TX_BBOX_HIGH}
        },
    }
    try:
        async with httpx.AsyncClient(timeout=5.0) as client:
            resp = await client.post(url, headers=headers, json=body)
        data = resp.json()
        if resp.status_code != 200:
            err = (data.get("error") or {}).get("message") or data.get("error") or resp.text
            logger.warning("autocomplete: Places API %s: %s", resp.status_code, err)
            return {
                "predictions": [],
                "error": f"Places API status: {resp.status_code}",
                "message": str(err),
            }
        predictions = []
        for s in data.get("suggestions", []):
            pp = s.get("placePrediction") or {}
            text = (pp.get("text") or {}).get("text", "")
            place_id = pp.get("placeId", "")
            if not text:
                continue
            # Bounding rectangle spills into AR/OK/LA/NM corners; keep TX only.
            if ", TX" not in text and ", TX," not in text and not text.endswith(", TX"):
                continue
            predictions.append({"description": text, "place_id": place_id})
        return {"predictions": predictions}
    except Exception as e:
        logger.exception("autocomplete: request failed")
        return {"predictions": [], "error": str(e)}


@router.get("/db-autocomplete")
async def db_autocomplete(request: Request, q: str = "", type: str = "all", fields: str = ""):
    """Return distinct DB-backed suggestions matching q.

    - type: "sales" | "leases" | "all" (default "all")
    - fields: comma-separated subset of {address, buyer, seller, tenant_name, city, zip_code}
              if empty, defaults are used per type
    """
    q = (q or "").strip()
    if len(q) < 2:
        return {"suggestions": []}
    like = f"%{q}%"
    requested = set(f.strip() for f in fields.split(",") if f.strip()) if fields else set()

    sales_field_map = {
        "address": SaleComp.address,
        "buyer": SaleComp.buyer,
        "seller": SaleComp.seller,
        "city": SaleComp.city,
        "zip_code": SaleComp.zip_code,
    }
    lease_field_map = {
        "address": LeaseComp.address,
        "tenant_name": LeaseComp.tenant_name,
        "city": LeaseComp.city,
        "zip_code": LeaseComp.zip_code,
    }

    if not requested:
        sales_use = set(sales_field_map.keys()) if type in ("sales", "all") else set()
        lease_use = set(lease_field_map.keys()) if type in ("leases", "all") else set()
    else:
        sales_use = requested & set(sales_field_map.keys()) if type in ("sales", "all") else set()
        lease_use = requested & set(lease_field_map.keys()) if type in ("leases", "all") else set()

    results = []
    seen = set()
    session = Session()
    try:
        for fname in sales_use:
            col = sales_field_map[fname]
            rows = (
                session.query(col)
                .filter(col.isnot(None), col.ilike(like) if hasattr(col, "ilike") else col.like(like))
                .distinct()
                .limit(15)
                .all()
            )
            for (val,) in rows:
                if not val:
                    continue
                key = (fname, str(val).strip().lower())
                if key in seen:
                    continue
                seen.add(key)
                results.append({"value": val, "field": fname, "comp_type": "sales"})
        for fname in lease_use:
            col = lease_field_map[fname]
            rows = (
                session.query(col)
                .filter(col.isnot(None), col.ilike(like) if hasattr(col, "ilike") else col.like(like))
                .distinct()
                .limit(15)
                .all()
            )
            for (val,) in rows:
                if not val:
                    continue
                key = (fname, str(val).strip().lower())
                if key in seen:
                    continue
                seen.add(key)
                results.append({"value": val, "field": fname, "comp_type": "leases"})
    finally:
        session.close()

    # Rank: exact-prefix matches first, then alphabetical, cap to 20
    q_lower = q.lower()
    results.sort(key=lambda r: (0 if str(r["value"]).lower().startswith(q_lower) else 1, str(r["value"]).lower()))
    return {"suggestions": results[:20]}
