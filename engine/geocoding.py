"""engine/geocoding.py — Google Maps geocoding wrapper."""
from __future__ import annotations
import re
import requests

TEXAS_BOUNDS = {
    'lat_min': 25.84, 'lat_max': 36.50,
    'lng_min': -106.65, 'lng_max': -93.51,
}


def _is_in_texas(lat, lng):
    return (TEXAS_BOUNDS['lat_min'] <= lat <= TEXAS_BOUNDS['lat_max'] and
            TEXAS_BOUNDS['lng_min'] <= lng <= TEXAS_BOUNDS['lng_max'])


def _extract_address_components(result):
    """Extract city and zip_code from Google geocoding address_components."""
    components = result.get('address_components', [])
    city = None
    zip_code = None
    state = None
    for c in components:
        if 'locality' in c['types']:
            city = c['long_name']
        if 'postal_code' in c['types']:
            zip_code = c['long_name']
        if 'administrative_area_level_1' in c['types']:
            state = c.get('short_name')
    return city, zip_code, state


def fetch_google_data(raw_text, api_key):
    """Geocode an address using the Google Maps Geocoding API.
    Restricts results to Texas. Returns dict with latitude, longitude, etc."""
    if not isinstance(raw_text, str) or not raw_text.strip():
        return None
    if not api_key or "YOUR_KEY" in api_key:
        return None

    addr = raw_text.strip()

    # Append "TX" if the address doesn't already mention Texas
    addr_lower = addr.lower()
    has_state = any(s in addr_lower for s in [' tx', ' texas', ', tx', ',tx'])
    if not has_state:
        addr = f"{addr}, TX"

    try:
        url = "https://maps.googleapis.com/maps/api/geocode/json"
        params = {
            "address": addr,
            "key": api_key,
            "components": "country:US|administrative_area:TX",
            "bounds": "25.84,-106.65|36.50,-93.51",
        }
        res = requests.get(url, params=params).json()

        top = None
        if res['status'] == 'OK':
            top = res['results'][0]
        elif res['status'] == 'ZERO_RESULTS':
            # Retry with simplified address (strip suite/unit/building) but keep TX restriction
            simplified = re.sub(r'(?i)\b(suite|ste|unit|bldg|building|floor|fl)\s*[#]?\s*\w+', '', addr).strip()
            simplified = re.sub(r'\s+', ' ', simplified)
            if simplified != addr:
                params['address'] = simplified
                res = requests.get(url, params=params).json()
                if res['status'] == 'OK':
                    top = res['results'][0]

        if top is None:
            return None

        lat = top['geometry']['location']['lat']
        lng = top['geometry']['location']['lng']
        formatted = top['formatted_address']
        city, zip_code, state = _extract_address_components(top)

        # Validate Texas bounds
        warning = None
        if not _is_in_texas(lat, lng):
            warning = f"Address geocoded outside Texas: {formatted}"
            return {
                "formatted_address": formatted,
                "latitude": None,
                "longitude": None,
                "city": city,
                "zip_code": zip_code,
                "state": state,
                "warning": warning,
            }

        return {
            "formatted_address": formatted,
            "latitude": lat,
            "longitude": lng,
            "city": city,
            "zip_code": zip_code,
            "state": state,
            "warning": warning,
        }

    except Exception:
        return None


# ---- Learned geocoding wrapper ----

def _normalize_raw(raw_text: str) -> str:
    """Texas bias + whitespace trim. Single source of truth for cache keys."""
    s = (raw_text or "").strip()
    if not s:
        return s
    s_upper = s.upper()
    if ", TX" in s_upper or s_upper.endswith(" TX") or " TX " in f" {s_upper} ":
        return s
    return f"{s}, TX"


def resolve_geocode(
    raw_text: str,
    api_key: str,
    store,
    openai_client,
) -> dict:
    """Learned geocoding flow:
       1. Override table — highest precedence (user-confirmed fixed mapping).
       2. Alias cache — raw_text → canonical address already resolved.
       3. Google direct call.
       4. On miss, LLM normalizes raw text → retry Google.
    Every successful resolution is written back to the alias cache.
    All lookups use the Texas-biased normalized form as the cache key.
    """
    normalized = _normalize_raw(raw_text)

    override = store.get_geocode_override(normalized)
    if override is not None:
        return {**override, "source": "override"}

    alias = store.get_geocode_alias(normalized)
    if alias is not None:
        store.bump_hit_count(normalized)
        return {**alias, "source": "alias_cache"}

    result = fetch_google_data(normalized, api_key=api_key)
    if result is not None and result.get("latitude") is not None:
        store.insert_geocode_alias(
            raw_text=normalized,
            canonical_address=result["formatted_address"],
            lat=result["latitude"],
            lng=result["longitude"],
        )
        return {**result, "source": "google"}

    # LLM fallback
    if openai_client is not None:
        try:
            cleaned = openai_client.normalize(raw_text)
        except Exception:
            cleaned = None
        if cleaned:
            cleaned_biased = _normalize_raw(cleaned)
            retry = fetch_google_data(cleaned_biased, api_key=api_key)
            if retry is not None and retry.get("latitude") is not None:
                store.insert_geocode_alias(
                    raw_text=normalized,  # key by ORIGINAL normalized, not cleaned
                    canonical_address=retry["formatted_address"],
                    lat=retry["latitude"],
                    lng=retry["longitude"],
                )
                return {**retry, "source": "google+llm"}

    return {"source": "failed", "raw_text": raw_text, "latitude": None, "longitude": None}
