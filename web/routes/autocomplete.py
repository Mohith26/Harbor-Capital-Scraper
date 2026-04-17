"""Server-side address autocomplete via Google Places REST API (Texas-biased)."""
import httpx
from fastapi import APIRouter, Request
from web.config import settings

router = APIRouter(prefix="/api", tags=["autocomplete"])

# Texas center + radius for biasing
_TX_CENTER = "31.0,-99.0"
_TX_RADIUS = "600000"  # meters (~372 miles, covers Texas)


@router.get("/autocomplete")
async def autocomplete(request: Request, q: str = ""):
    """Return Google Places autocomplete predictions for an address query.

    Constrained to US addresses biased toward Texas.
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
        "strictbounds": "true",
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
