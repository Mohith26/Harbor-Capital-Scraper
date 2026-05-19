import math
import pandas as pd
import numpy as np
from datetime import datetime
from dateutil.parser import parse as parse_date
from scipy.spatial.distance import cosine

from utils import haversine_miles
from database import Session, SaleComp, LeaseComp
from comp_engine import get_embeddings


# ---------------------------------------------------------------------------
# 1. INDIVIDUAL SCORING FUNCTIONS  (each returns 0.0–1.0)
# ---------------------------------------------------------------------------

def score_proximity(lat1, lon1, lat2, lon2, max_radius_miles=25.0):
    """Smooth exponential decay so closer is better but distant comps still
    contribute a small signal. A comp at the radius edge scores ~0.05 rather
    than dropping to a hard 0 — this prevents the "everything past N miles
    is 0%" cliff seen in B028.
    """
    dist = haversine_miles(lat1, lon1, lat2, lon2)
    if dist is None or dist >= 99000:  # haversine returns 99999 for missing lat/lon
        return None
    # Decay constant: at dist == max_radius we want ~0.05 (5%).
    # exp(-dist / k) = 0.05  =>  k = max_radius / ln(20) ≈ max_radius / 3.0
    k = max(0.5, max_radius_miles / 3.0)
    return math.exp(-dist / k)


def score_numeric_similarity(subject_val, comp_val, tolerance_pct=0.5):
    """Smooth Gaussian-style similarity on percent-difference.

    Old behavior: linear with hard cutoff -> comps >tolerance_pct away got
    exactly 0, which clustered the distribution (see B028).

    New behavior: score = exp(-(pct_diff / tolerance_pct)^2). This decays
    smoothly so a 50%-different comp scores ~0.37, a 100%-different comp
    scores ~0.02, and there is no abrupt zero-cliff.
    """
    if subject_val is None or comp_val is None:
        return None
    try:
        s, c = float(subject_val), float(comp_val)
    except (ValueError, TypeError):
        return None
    if s == 0 and c == 0:
        return 1.0
    denom = max(abs(s), abs(c))
    if denom == 0:
        return 1.0
    pct_diff = abs(s - c) / denom
    # exp(-(x/tol)^2) — Gaussian. At x=tol score=1/e≈0.37; at x=2*tol≈0.018.
    if tolerance_pct <= 0:
        return 1.0 if pct_diff == 0 else 0.0
    return math.exp(-((pct_diff / tolerance_pct) ** 2))


def score_categorical_match(subject_val, comp_val):
    if subject_val is None or comp_val is None:
        return None
    return 1.0 if str(subject_val).strip().lower() == str(comp_val).strip().lower() else 0.0


def score_recency(comp_date_str, max_age_years=3):
    if not comp_date_str:
        return None
    try:
        comp_dt = parse_date(str(comp_date_str), fuzzy=True)
        age_days = (datetime.now() - comp_dt).days
        age_years = age_days / 365.25
        if age_years < 0:
            return 1.0
        return max(0.0, 1.0 - age_years / max_age_years)
    except Exception:
        return None


# ---------------------------------------------------------------------------
# 2. LOAD COMPS FROM DATABASE
# ---------------------------------------------------------------------------

def load_comps(comp_type):
    session = Session()
    try:
        ct = (comp_type or "").lower()
        Model = SaleComp if ct == "sales" else LeaseComp
        records = session.query(Model).all()
        if not records:
            return pd.DataFrame()
        rows = [{c.name: getattr(r, c.name) for c in Model.__table__.columns} for r in records]
        return pd.DataFrame(rows)
    finally:
        session.close()


# ---------------------------------------------------------------------------
# 3. COMPOSITE WEIGHTED SCORING
# ---------------------------------------------------------------------------

def _subj_lat(subj):
    return subj.get("latitude", subj.get("lat"))


def _subj_lng(subj):
    return subj.get("longitude", subj.get("lng"))


SALE_SCORE_MAP = {
    "proximity":  lambda subj, row, mr: score_proximity(_subj_lat(subj), _subj_lng(subj), row.get("latitude"), row.get("longitude"), mr),
    "size":       lambda subj, row, _: score_numeric_similarity(subj.get("building_size"), row.get("building_size")),
    "price":      lambda subj, row, _: score_numeric_similarity(subj.get("sale_price"), row.get("sale_price")),
    "price_psf":  lambda subj, row, _: score_numeric_similarity(subj.get("price_per_sf"), row.get("price_per_sf")),
    "year_built": lambda subj, row, _: score_numeric_similarity(subj.get("year_built"), row.get("year_built"), tolerance_pct=0.15),
    "recency":    lambda subj, row, _: score_recency(row.get("closing_date")),
}

LEASE_SCORE_MAP = {
    "proximity":      lambda subj, row, mr: score_proximity(_subj_lat(subj), _subj_lng(subj), row.get("latitude"), row.get("longitude"), mr),
    "size":           lambda subj, row, _: score_numeric_similarity(subj.get("leased_sf"), row.get("leased_sf")),
    "rate_monthly":   lambda subj, row, _: score_numeric_similarity(subj.get("rate_monthly"), row.get("rate_monthly")),
    "rate_annually":  lambda subj, row, _: score_numeric_similarity(subj.get("rate_annually"), row.get("rate_annually")),
    "building_type":  lambda subj, row, _: score_categorical_match(subj.get("building_type"), row.get("building_type")),
    "recency":        lambda subj, row, _: score_recency(row.get("commencement_date")),
}

# Default weights when caller doesn't specify any
DEFAULT_SALE_WEIGHTS = {
    "proximity": 0.4, "size": 0.2, "price": 0.15, "price_psf": 0.1,
    "year_built": 0.05, "recency": 0.1,
}
DEFAULT_LEASE_WEIGHTS = {
    "proximity": 0.4, "size": 0.25, "rate_monthly": 0.15, "rate_annually": 0.05,
    "building_type": 0.05, "recency": 0.1,
}

# Map user-facing weight slider names (from the HTML form) to internal scoring
# axis names. The /finder/search route accepts weight_distance, weight_size,
# weight_price, weight_age — those become keys 'distance','size','price','age'
# in the weights dict. Without this map, only 'size' and 'price' happen to
# match an internal axis name, and proximity/age never get weighted. This was
# the root cause of "always 0%" and B028's clustering.
WEIGHT_KEY_ALIASES_SALES = {
    "distance":  ["proximity"],
    "proximity": ["proximity"],
    "size":      ["size"],
    "price":     ["price", "price_psf"],   # split user 'price' across $-total and $/SF
    "age":       ["year_built", "recency"],
    "year_built": ["year_built"],
    "recency":   ["recency"],
}

WEIGHT_KEY_ALIASES_LEASES = {
    "distance":  ["proximity"],
    "proximity": ["proximity"],
    "size":      ["size"],
    "price":     ["rate_monthly", "rate_annually"],
    "rate":      ["rate_monthly", "rate_annually"],
    "rate_monthly": ["rate_monthly"],
    "rate_annually": ["rate_annually"],
    "age":       ["recency"],
    "recency":   ["recency"],
    "building_type": ["building_type"],
}


def _normalize_weights(weights, is_sales, score_map):
    """Translate weight keys to internal scoring axis weights.

    Two calling conventions are supported:

    1. User-facing form keys: ``distance / size / price / age``
       (what the /finder/search HTML form posts). Each is expanded to one
       or more internal axes via WEIGHT_KEY_ALIASES_*. When one user key
       expands to multiple axes (e.g. 'price' -> price-total + $/SF), the
       weight is split evenly across them.

    2. Internal axis keys: ``proximity / size / price / price_psf /
       year_built / recency`` (what app.py and DEFAULT_*_WEIGHTS use).
       These are kept as-is.

    To avoid conflating the two, we sniff the input: if any "internal-only"
    key is present (price_psf, year_built, recency, rate_monthly,
    rate_annually, building_type, proximity), we assume the caller is using
    the internal convention and skip alias expansion entirely.

    The returned dict is normalized so weights sum to 1.0 across the axes
    present in score_map.
    """
    if not weights:
        return {}

    internal_only_keys = {
        "proximity", "price_psf", "year_built", "recency",
        "rate_monthly", "rate_annually", "building_type",
    }
    caller_uses_internal_keys = any(k in internal_only_keys for k in weights)
    aliases = WEIGHT_KEY_ALIASES_SALES if is_sales else WEIGHT_KEY_ALIASES_LEASES

    normalized = {}
    for k, v in weights.items():
        try:
            w = float(v)
        except (TypeError, ValueError):
            continue
        if w <= 0:
            continue

        if caller_uses_internal_keys:
            # Treat every key as an internal axis name.
            if k in score_map:
                normalized[k] = normalized.get(k, 0.0) + w
            continue

        targets = aliases.get(k)
        if targets:
            share = w / len(targets)
            for axis in targets:
                if axis in score_map:
                    normalized[axis] = normalized.get(axis, 0.0) + share
        elif k in score_map:
            normalized[k] = normalized.get(k, 0.0) + w
        # else: silently ignore (unknown axis name)

    total = sum(normalized.values())
    if total > 0:
        normalized = {k: v / total for k, v in normalized.items()}
    return normalized


def compute_match_scores(subject, comps_df, comp_type, weights, max_radius=25.0):
    """Score each comp against the subject property.

    Returns comps_df with added score columns and sorted by match_score desc.

    Scoring model:
      * Each axis (proximity, size, price, ...) produces a 0..1 sub-score via
        the matching scorer in SALE_SCORE_MAP / LEASE_SCORE_MAP. A None
        sub-score means "subject didn't provide this field" and the axis is
        dropped (its weight is redistributed across remaining axes).
      * Axis weights come from the user-facing form (distance/size/price/age)
        via _normalize_weights, which maps them onto the internal axes and
        normalizes the total weight to 1.0.
      * Overall match_score = weighted average of sub-scores (0..1).
    """
    ct = (comp_type or "").lower()
    is_sales = ct == "sales"
    score_map = SALE_SCORE_MAP if is_sales else LEASE_SCORE_MAP
    if not weights:
        weights = DEFAULT_SALE_WEIGHTS if is_sales else DEFAULT_LEASE_WEIGHTS

    normalized_weights = _normalize_weights(weights, is_sales, score_map)
    # Fallback: if nothing normalized (e.g., caller passed only unknown keys),
    # fall back to the defaults so we never end up with all-zero weights.
    if not normalized_weights:
        normalized_weights = _normalize_weights(
            DEFAULT_SALE_WEIGHTS if is_sales else DEFAULT_LEASE_WEIGHTS,
            is_sales, score_map,
        )

    # Build score columns
    score_cols = {}
    for key in score_map:
        col_name = f"{key}_score"
        scores = []
        for _, row in comps_df.iterrows():
            val = score_map[key](subject, row.to_dict(), max_radius)
            scores.append(val)
        score_cols[col_name] = scores

    for col_name, vals in score_cols.items():
        comps_df[col_name] = vals

    # Weighted average — drop axes the subject didn't supply (None sub-score),
    # re-normalizing so we never penalize a missing field as 0.
    totals = []
    for idx, row in comps_df.iterrows():
        weighted_sum = 0.0
        weight_sum = 0.0
        for axis, w in normalized_weights.items():
            val = row.get(f"{axis}_score")
            if val is None or w <= 0:
                continue
            weighted_sum += w * val
            weight_sum += w
        totals.append(weighted_sum / weight_sum if weight_sum > 0 else 0.0)

    comps_df["match_score"] = totals

    # Distance column for display
    s_lat, s_lng = _subj_lat(subject), _subj_lng(subject)
    comps_df["distance_miles"] = comps_df.apply(
        lambda r: round(haversine_miles(s_lat, s_lng, r.get("latitude"), r.get("longitude")), 1),
        axis=1,
    )

    return comps_df.sort_values("match_score", ascending=False).reset_index(drop=True)


# ---------------------------------------------------------------------------
# 4. AI SEMANTIC MATCHING (OPTIONAL)
# ---------------------------------------------------------------------------

def build_property_description(props, comp_type):
    parts = []
    addr = props.get("address") or props.get("raw_address_data") or "Unknown location"
    parts.append(addr)

    if comp_type == "Sales":
        if props.get("building_size"):
            parts.append(f"{props['building_size']:,.0f} SF")
        if props.get("sale_price"):
            parts.append(f"sold for ${props['sale_price']:,.0f}")
        if props.get("price_per_sf"):
            parts.append(f"${props['price_per_sf']:,.2f}/SF")
        if props.get("year_built"):
            parts.append(f"built {int(props['year_built'])}")
        if props.get("cap_rate"):
            parts.append(f"{props['cap_rate']:.2f}% cap rate")
        if props.get("closing_date"):
            parts.append(f"closed {props['closing_date']}")
    else:
        if props.get("leased_sf"):
            parts.append(f"{props['leased_sf']:,.0f} SF leased")
        if props.get("rate_monthly"):
            parts.append(f"${props['rate_monthly']:,.2f}/SF/mo")
        if props.get("rate_annually"):
            parts.append(f"${props['rate_annually']:,.2f}/SF/yr")
        if props.get("building_type"):
            parts.append(f"{props['building_type']} building")
        if props.get("lease_type"):
            parts.append(f"{props['lease_type']} lease")
        if props.get("commencement_date"):
            parts.append(f"commenced {props['commencement_date']}")

    if props.get("city"):
        parts.append(props["city"])
    if props.get("notes"):
        parts.append(props["notes"])

    return ", ".join(parts)


def compute_ai_scores(subject, comps_df, comp_type):
    """Compute cosine similarity between subject and each comp using OpenAI embeddings."""
    subject_desc = build_property_description(subject, comp_type)

    comp_descs = []
    for _, row in comps_df.iterrows():
        comp_descs.append(build_property_description(row.to_dict(), comp_type))

    all_texts = [subject_desc] + comp_descs
    embeddings = get_embeddings(all_texts)

    subject_emb = embeddings[0]
    scores = []
    for i in range(1, len(embeddings)):
        sim = 1.0 - cosine(subject_emb, embeddings[i])
        scores.append(max(0.0, sim))

    return pd.Series(scores, index=comps_df.index)


def blend_scores(weighted_scores, ai_scores, ai_weight=0.3):
    return (1.0 - ai_weight) * weighted_scores + ai_weight * ai_scores
