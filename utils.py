import re
import math
from difflib import SequenceMatcher


# Standard abbreviation mappings for address normalization
_ABBREVS = {
    'street': 'st', 'avenue': 'ave', 'boulevard': 'blvd', 'drive': 'dr',
    'road': 'rd', 'lane': 'ln', 'parkway': 'pkwy', 'highway': 'hwy',
    'north': 'n', 'south': 's', 'east': 'e', 'west': 'w',
    'suite': 'ste', 'building': 'bldg', 'floor': 'fl',
}


def normalize_address(addr):
    """Normalize address for comparison: lowercase, remove punctuation, standardize abbreviations."""
    if not addr:
        return ""
    addr = str(addr).lower().strip()
    addr = re.sub(r'[^\w\s]', '', addr)
    addr = re.sub(r'\s+', ' ', addr)
    for full, abbr in _ABBREVS.items():
        addr = re.sub(rf'\b{full}\b', abbr, addr)
    return addr


def find_duplicates(new_address, existing_records, threshold=0.85):
    """Find duplicate addresses using fuzzy matching.
    existing_records: list of (id, address) tuples.
    Returns list of (existing_id, existing_address, similarity_score) above threshold."""
    norm_new = normalize_address(new_address)
    if not norm_new:
        return []
    matches = []
    for eid, eaddr in existing_records:
        norm_existing = normalize_address(eaddr)
        if not norm_existing:
            continue
        score = SequenceMatcher(None, norm_new, norm_existing).ratio()
        if score >= threshold:
            matches.append((eid, eaddr, score))
    return sorted(matches, key=lambda x: x[2], reverse=True)


def extract_zip_from_address(address):
    """Extract 5-digit zip code from address string."""
    if not address:
        return None
    match = re.search(r'\b(\d{5})\b', str(address))
    return match.group(1) if match else None


def extract_city_from_address(address):
    """Simple city extraction: try to get the city part before state/zip."""
    if not address:
        return None
    # Pattern: "..., City, TX 77001" or "..., City, Texas"
    match = re.search(r',\s*([A-Za-z\s]+?),\s*(?:TX|Texas)\b', str(address), re.IGNORECASE)
    return match.group(1).strip() if match else None


def haversine_miles(lat1, lon1, lat2, lon2):
    if any(x is None for x in [lat1, lon1, lat2, lon2]):
        return 99999
    R = 3958.8
    try:
        dlat = math.radians(lat2 - lat1)
        dlon = math.radians(lon2 - lon1)
        a = math.sin(dlat / 2) ** 2 + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon / 2) ** 2
        c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
        return R * c
    except Exception:
        return 99999
