"""Server-side address autocomplete via Google Places REST API (Texas-biased)."""
import httpx
from fastapi import APIRouter, Request
from sqlalchemy import or_, func as sa_func
from web.config import settings
from database import Session, SaleComp, LeaseComp

router = APIRouter(prefix="/api", tags=["autocomplete"])

# Texas center + radius for biasing
_TX_CENTER = "31.0,-99.0"
_TX_RADIUS = "600000"  # meters (~372 miles, covers Texas)


@router.get("/autocomplete")
async def autocomplete(request: Request, q: str = ""):
    """Return Google Places autocomplete predictions for an address query.

    Uses legacy Places API. Constrained to US, biased toward Texas.
    """
    q = (q or "").strip()
    if len(q) < 3:
        return {"predictions": []}
    api_key = settings.GOOGLE_API_KEY
    if not api_key:
        return {"predictions": [], "error": "GOOGLE_API_KEY not configured"}

    url = "https://maps.googleapis.com/maps/api/place/autocomplete/json"
    params = {
        "input": q,
        "key": api_key,
        "types": "address",
        "components": "country:us",
        "location": _TX_CENTER,
        "radius": _TX_RADIUS,
    }
    try:
        async with httpx.AsyncClient(timeout=5.0) as client:
            resp = await client.get(url, params=params)
        data = resp.json()
        status = data.get("status", "UNKNOWN")
        if status not in ("OK", "ZERO_RESULTS"):
            return {
                "predictions": [],
                "error": f"Places API status: {status}",
                "message": data.get("error_message", ""),
            }
        predictions = [
            {"description": p.get("description", ""), "place_id": p.get("place_id", "")}
            for p in data.get("predictions", [])
        ]
        return {"predictions": predictions}
    except Exception as e:
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
