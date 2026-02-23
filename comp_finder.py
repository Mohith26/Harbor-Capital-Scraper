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
    dist = haversine_miles(lat1, lon1, lat2, lon2)
    if dist >= max_radius_miles:
        return 0.0
    return max(0.0, 1.0 - dist / max_radius_miles)


def score_numeric_similarity(subject_val, comp_val, tolerance_pct=0.5):
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
    ratio = abs(s - c) / denom
    return max(0.0, 1.0 - ratio / tolerance_pct)


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
        Model = SaleComp if comp_type == "Sales" else LeaseComp
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

SALE_SCORE_MAP = {
    "proximity":  lambda subj, row, mr: score_proximity(subj["lat"], subj["lng"], row.get("latitude"), row.get("longitude"), mr),
    "size":       lambda subj, row, _: score_numeric_similarity(subj.get("building_size"), row.get("building_size")),
    "price":      lambda subj, row, _: score_numeric_similarity(subj.get("sale_price"), row.get("sale_price")),
    "price_psf":  lambda subj, row, _: score_numeric_similarity(subj.get("price_per_sf"), row.get("price_per_sf")),
    "year_built": lambda subj, row, _: score_numeric_similarity(subj.get("year_built"), row.get("year_built"), tolerance_pct=0.15),
    "recency":    lambda subj, row, _: score_recency(row.get("closing_date")),
}

LEASE_SCORE_MAP = {
    "proximity":      lambda subj, row, mr: score_proximity(subj["lat"], subj["lng"], row.get("latitude"), row.get("longitude"), mr),
    "size":           lambda subj, row, _: score_numeric_similarity(subj.get("leased_sf"), row.get("leased_sf")),
    "rate_monthly":   lambda subj, row, _: score_numeric_similarity(subj.get("rate_monthly"), row.get("rate_monthly")),
    "rate_annually":  lambda subj, row, _: score_numeric_similarity(subj.get("rate_annually"), row.get("rate_annually")),
    "building_type":  lambda subj, row, _: score_categorical_match(subj.get("building_type"), row.get("building_type")),
    "recency":        lambda subj, row, _: score_recency(row.get("commencement_date")),
}


def compute_match_scores(subject, comps_df, comp_type, weights, max_radius=25.0):
    """Score each comp against the subject property.

    Returns comps_df with added score columns and sorted by match_score desc.
    """
    score_map = SALE_SCORE_MAP if comp_type == "Sales" else LEASE_SCORE_MAP

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

    # Compute weighted total — skip dimensions with None scores (subject didn't provide that field)
    totals = []
    for idx, row in comps_df.iterrows():
        weighted_sum = 0.0
        weight_sum = 0.0
        for key in score_map:
            col_name = f"{key}_score"
            w = weights.get(key, 0.0)
            val = row[col_name]
            if val is not None and w > 0:
                weighted_sum += w * val
                weight_sum += w
        totals.append(weighted_sum / weight_sum if weight_sum > 0 else 0.0)

    comps_df["match_score"] = totals

    # Distance column for display
    comps_df["distance_miles"] = comps_df.apply(
        lambda r: round(haversine_miles(subject["lat"], subject["lng"], r.get("latitude"), r.get("longitude")), 1),
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
