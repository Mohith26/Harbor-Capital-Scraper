"""engine/mapping.py — column mapping using embeddings and Hungarian algorithm."""
from __future__ import annotations
import numpy as np
import pandas as pd
from scipy.optimize import linear_sum_assignment
from scipy.spatial.distance import cosine
from difflib import get_close_matches

from engine.openai_client import _client
from engine.cleaning import clean_header, get_column_profile

# Cache for schema embeddings (they never change)
_schema_embedding_cache = {}


def get_embeddings(texts):
    """Get embeddings from OpenAI text-embedding-3-small. Returns numpy array."""
    client = _client()
    response = client.embeddings.create(input=texts, model="text-embedding-3-small")
    return np.array([item.embedding for item in response.data])


LEASE_SCHEMA = {
    'address':           {'desc': "address location property city state zip street", 'type': 'text'},
    'tenant_name':       {'desc': "tenant name lessee company occupant business",   'type': 'text'},
    'leased_sf':         {'desc': "size sqft square footage area dimensions rba leased space", 'type': 'numeric_clean'},
    'rate_psf':          {'desc': "rent rate price cost base rent annual rent monthly", 'type': 'numeric_money'},
    'lease_type':        {'desc': "lease type structure nnn gross full service",     'type': 'text'},
    'term_months':       {'desc': "term months duration length years",              'type': 'numeric_clean'},
    'commencement_date': {'desc': "commencement start date move in possession",     'type': 'date'},
    'escalations':       {'desc': "escalations bumps increases steps annual increase", 'type': 'text'},
    'ti_allowance':      {'desc': "ti allowance work letter improvement allowance construction", 'type': 'numeric_money'},
    'free_rent':         {'desc': "free rent abatement concessions months free",    'type': 'text'},
    'clear_height':      {'desc': "clear height ceiling height clearance",          'type': 'numeric_clean'},
    'building_type':     {'desc': "building type construction class metal tilt wall", 'type': 'text'},
    'year_built':        {'desc': "year built age renovated constructed vintage",   'type': 'numeric_clean'},
    'notes':             {'desc': "notes comments details observations",            'type': 'text'},
}

SALE_SCHEMA = {
    'address':      {'desc': "address location property city state zip street",        'type': 'text'},
    'sale_price':   {'desc': "sale price purchase price price cost transaction value",  'type': 'numeric_money'},
    'building_size':{'desc': "size sqft square footage area dimensions rba building sf",'type': 'numeric_clean'},
    'price_per_sf': {'desc': "price per sf price/sf rate psf unit price",              'type': 'numeric_money'},
    'closing_date': {'desc': "closing date sold date transaction date",                'type': 'date'},
    'year_built':   {'desc': "year built age renovated constructed",                   'type': 'numeric_clean'},
    'cap_rate':     {'desc': "cap rate capitalization yield return",                    'type': 'numeric_clean'},
    'buyer':        {'desc': "buyer purchaser acquirer buying entity",                  'type': 'text'},
    'seller':       {'desc': "seller vendor grantor selling entity",                    'type': 'text'},
    'notes':        {'desc': "notes comments details observations",                    'type': 'text'},
}

BASE_OVERRIDES = {
    'price per sf': 'price_per_sf', 'sale price psf': 'price_per_sf', 'pps': 'price_per_sf',
    'price psf': 'price_per_sf', 'per sf': 'price_per_sf', 'psf': 'price_per_sf',
    'price/sf': 'price_per_sf', 'price sf': 'price_per_sf',
    'price lsf': 'price_per_sf',
    'rent': 'rate_psf', 'base rent': 'rate_psf', 'rental rate': 'rate_psf',
    'base rent yearly': 'rate_psf', 'base rent monthly': 'rate_psf', 'base rate': 'rate_psf',
    'asking rate': 'rate_psf', 'nnn rate': 'rate_psf', 'gross rate': 'rate_psf',
    'rate': 'rate_psf', 'rate month': 'rate_psf', 'rate monthly': 'rate_psf',
    'rate per month': 'rate_psf', 'rate/sf': 'rate_psf', 'rate psf': 'rate_psf',
    'rate sf': 'rate_psf', 'rate per sf': 'rate_psf',
    'rate/acre': 'rate_psf', 'rate acre': 'rate_psf', 'rate / acre / month': 'rate_psf',
    'rate per acre': 'rate_psf',
    'lease rate': 'rate_psf', 'lease rate/sf': 'rate_psf',
    'first year rent': 'rate_psf', 'first year rent per area': 'rate_psf',
    'asking rent': 'rate_psf', 'effective rent': 'rate_psf',
    'effective rent per area': 'rate_psf', 'rent per area': 'rate_psf',
    'date closed': 'closing_date', 'closing date': 'closing_date', 'sale date': 'closing_date',
    'transaction date': 'closing_date', 'date of sale': 'closing_date', 'closed': 'closing_date',
    'close date': 'closing_date',
    'esc': 'escalations', 'escalation': 'escalations', 'escalation percent': 'escalations',
    'steps': 'escalations', 'annual increase': 'escalations', 'bumps': 'escalations',
    'annual bumps': 'escalations',
    'construction': 'building_type', 'building class': 'building_type',
    'property type': 'building_type', 'construction type': 'building_type',
    'class': 'building_type', 'building type': 'building_type',
    'months': 'term_months', 'lease term': 'term_months', 'term': 'term_months',
    'comments': 'notes', 'notes': 'notes', 'remarks': 'notes',
    'buyer': 'buyer', 'seller': 'seller', 'purchaser': 'buyer', 'grantor': 'seller',
    'cap rate': 'cap_rate', 'in place cap rate': 'cap_rate', 'goingin cap rate': 'cap_rate',
    'cap': 'cap_rate', 'stabilized cap rate': 'cap_rate',
    'pricing guidance cap rate / yoc': 'cap_rate',
    'sale price': 'sale_price', 'purchase price': 'sale_price', 'total price': 'sale_price',
    'consideration': 'sale_price', 'sale price $': 'sale_price',
    'rentable area': 'building_size', 'size sf': 'building_size', 'sizesf': 'building_size',
    'total sf': 'building_size', 'building sf': 'building_size', 'rba': 'building_size',
    'total sq ft': 'building_size', 'sq ft': 'building_size', 'area': 'building_size',
    'building name': 'address',
    'tenant': 'tenant_name', 'tenant name': 'tenant_name', 'lessee': 'tenant_name',
    'occupant': 'tenant_name', 'company': 'tenant_name',
    'commencement': 'commencement_date', 'commencement date': 'commencement_date',
    'start date': 'commencement_date', 'signed date': 'commencement_date',
    'lease commencement': 'commencement_date', 'date': 'commencement_date',
    'ti': 'ti_allowance', 'ti allowance': 'ti_allowance', 'work letter': 'ti_allowance',
    'tenant improvement': 'ti_allowance',
    'free rent': 'free_rent', 'free rent months': 'free_rent', 'abatement': 'free_rent',
    'concession': 'free_rent',
    'clear height': 'clear_height', 'ceiling height': 'clear_height', 'clearance': 'clear_height',
    'year built': 'year_built', 'built': 'year_built', 'vintage': 'year_built',
    'address': 'address', 'property address': 'address', 'property name': 'address',
    'property': 'address', 'location': 'address', 'street address': 'address',
    'lease type': 'lease_type', 'rate type': 'lease_type', 'structure': 'lease_type',
    'lease structure': 'lease_type',
    'acreage': 'building_size', 'acres': 'building_size', 'land area': 'building_size',
}

LEASE_OVERRIDES = {
    'sf': 'leased_sf', 'size': 'leased_sf', 'sqft': 'leased_sf',
    'area leased': 'leased_sf', 'leased sf': 'leased_sf', 'space': 'leased_sf',
    'leased area': 'leased_sf', 'deal sf': 'leased_sf',
    'lease size': 'leased_sf', 'lease size sf': 'leased_sf',
    'total space': 'leased_sf', 'building area': 'leased_sf',
    'price': 'rate_psf', 'date': 'commencement_date',
    'sign date': 'commencement_date', 'signed date': 'commencement_date',
}

SALE_OVERRIDES = {
    'sf': 'building_size', 'size': 'building_size', 'sqft': 'building_size',
    'building size': 'building_size', 'total size': 'building_size',
    'price': 'sale_price', 'date': 'closing_date',
    'transaction date': 'closing_date',
}


def _find_override(cleaned_header, overrides, target_col):
    """Return override score (0.0 = no match, higher = better).
    Exact matches score highest, longer/more-specific substring matches score higher."""
    best_score = 0.0

    # Exact match — highest priority
    if cleaned_header in overrides and overrides[cleaned_header] == target_col:
        return 100.0 + len(cleaned_header)

    # Substring: score by match specificity (key length / header length)
    for key, val in overrides.items():
        if val == target_col and len(key) >= 3 and key in cleaned_header:
            specificity = len(key) / max(len(cleaned_header), 1)
            score = 90.0 + (specificity * 10.0)
            best_score = max(best_score, score)

    if best_score > 0:
        return best_score

    # Fuzzy match for misspellings — lowest override tier
    override_keys_for_target = [k for k, v in overrides.items() if v == target_col and len(k) >= 4]
    if override_keys_for_target:
        matches = get_close_matches(cleaned_header, override_keys_for_target, n=1, cutoff=0.8)
        if matches:
            return 85.0

    return 0.0


def dedupe_mappings_by_target(
    mappings: dict[str, str],
    raw_headers: list[str] | None = None,
    confidence: dict[str, float] | None = None,
    allow_duplicate_targets: tuple[str, ...] = ("notes",),
) -> dict[str, str]:
    """Keep at most one source column per target field.

    The highest-confidence source wins; ties fall back to the input header order.
    `notes` remains multi-source because the upload flow concatenates note fields.
    """
    if not mappings:
        return {}

    raw_order = {str(h): i for i, h in enumerate(raw_headers or [])}
    allowed = set(allow_duplicate_targets or ())
    chosen: dict[str, tuple[tuple[float, int], str]] = {}
    deduped: dict[str, str] = {}

    for fallback_order, (raw, target) in enumerate(mappings.items()):
        if not target or target in ("---", "unmapped"):
            continue
        if target in allowed:
            deduped[raw] = target
            continue

        try:
            score = float((confidence or {}).get(raw, 0.0))
        except (TypeError, ValueError):
            score = 0.0
        order = raw_order.get(str(raw), fallback_order)
        specificity = len(clean_header(raw))
        rank = (score, specificity, -order)
        current = chosen.get(target)
        if current is None or rank > current[0]:
            chosen[target] = (rank, raw)

    ordered = sorted(
        chosen.items(),
        key=lambda item: raw_order.get(str(item[1][1]), len(raw_order)),
    )
    for target, (_, raw) in ordered:
        deduped[raw] = target
    return deduped


def _get_schema_embeddings(schema_dict):
    """Get embeddings for schema descriptions, with caching."""
    cache_key = tuple(sorted(schema_dict.keys()))
    if cache_key not in _schema_embedding_cache:
        descs = [schema_dict[k]['desc'] for k in schema_dict]
        _schema_embedding_cache[cache_key] = get_embeddings(descs)
    return _schema_embedding_cache[cache_key]


def classify_file_type(headers, filename="", sheet_name=None):
    """Classify file as LEASE, SALE, BOTH, or UNKNOWN based on headers, filename, and sheet name."""
    fname = str(filename).lower()
    lease_score = 10 if any(x in fname for x in ['lease', 'leasing', 'tenant']) else 0
    sale_score = 10 if any(x in fname for x in ['sale', 'sold', 'transaction', 'purchase']) else 0

    # Sheet name is a stronger signal than filename (weight 15 vs 10)
    if sheet_name:
        sname = str(sheet_name).lower()
        if any(x in sname for x in ['lease', 'leasing', 'tenant']):
            lease_score += 15
        if any(x in sname for x in ['sale', 'sold', 'transaction', 'purchase']):
            sale_score += 15

    clean_headers = [str(h).lower().strip() for h in headers]
    lease_triggers = {'tenant', 'lessee', 'term', 'commencement', 'base rent', 'rent', 'leased',
                      'free rent', 'escalation', 'opex', 'base rate', 'lease type', 'signed date',
                      'rate type', 'ti allowance', 'ti', 'abatement'}
    sale_triggers = {'buyer', 'seller', 'closing', 'cap rate', 'purchase', 'sale price', 'deal',
                     'transaction', 'sale date', 'price per sf', 'acreage', 'purchase price'}

    lease_score += sum(1 for h in clean_headers if any(t in h for t in lease_triggers))
    sale_score += sum(1 for h in clean_headers if any(t in h for t in sale_triggers))

    if lease_score > sale_score:
        return "LEASE"
    elif sale_score > lease_score:
        return "SALE"
    elif lease_score > 0 and sale_score > 0:
        return "BOTH"
    return "UNKNOWN"


def generate_standardized_df(df, schema_dict, file_type, threshold=0.55):
    """Map input columns to schema using Hungarian algorithm for globally optimal matching.
    Returns (standardized_df, mapping_confidence_dict, mappings)."""
    input_headers = df.columns.tolist()
    clean_headers = [clean_header(h) for h in input_headers]
    # Replace empty strings with placeholder (OpenAI API rejects empty input)
    clean_headers = [h if h.strip() else "unknown column" for h in clean_headers]
    target_cols = list(schema_dict.keys())
    col_profiles = [get_column_profile(df[col]) for col in input_headers]

    # Build override dict for this file type
    overrides = dict(BASE_OVERRIDES)
    if file_type == "LEASE":
        overrides.update(LEASE_OVERRIDES)
    else:
        overrides.update(SALE_OVERRIDES)

    # Get embeddings
    head_vecs = get_embeddings(clean_headers)
    target_vecs = _get_schema_embeddings(schema_dict)

    n_targets = len(target_cols)
    n_inputs = len(input_headers)

    # Build score matrix for Hungarian algorithm
    # score_matrix[t_idx][h_idx] = score (higher is better)
    score_matrix = np.zeros((n_targets, n_inputs))

    for t_idx, target_col in enumerate(target_cols):
        target_type = schema_dict[target_col]['type']
        for h_idx in range(n_inputs):
            in_clean = clean_headers[h_idx]

            # Check overrides — specificity-weighted score
            override_score = _find_override(in_clean, overrides, target_col)
            if override_score > 0:
                score_matrix[t_idx, h_idx] = override_score
                continue

            # Semantic similarity
            sem_score = 1 - cosine(head_vecs[h_idx], target_vecs[t_idx])

            # Type bonuses (strengthened)
            input_type = col_profiles[h_idx]
            bonus = 0.0
            if target_type == input_type:
                bonus = 0.25
            elif target_type in ('numeric_money', 'numeric_clean') and input_type == 'text':
                bonus = -0.20
            elif target_type == 'date' and input_type != 'date':
                bonus = -0.30
            elif target_type == 'text' and input_type in ('numeric_money', 'numeric_clean'):
                bonus = -0.15

            score_matrix[t_idx, h_idx] = sem_score + bonus

    # Solve with Hungarian algorithm (minimizes cost, so negate scores)
    # Pad matrix if needed (more targets than inputs or vice versa)
    max_dim = max(n_targets, n_inputs)
    padded = np.zeros((max_dim, max_dim))
    padded[:n_targets, :n_inputs] = -score_matrix  # Negate for minimization
    row_ind, col_ind = linear_sum_assignment(padded)

    mappings = {}
    confidence = {}
    address_candidates = []

    # Collect address candidates from all input columns
    exclude_addr_patterns = {'state', 'city', 'zip', 'postal', 'county', 'country', 'submarket'}
    if 'address' in target_cols:
        addr_t_idx = target_cols.index('address')
        for h_idx in range(n_inputs):
            addr_score = 1 - cosine(head_vecs[h_idx], target_vecs[addr_t_idx])
            if addr_score > 0.55:
                hdr_clean = clean_header(input_headers[h_idx])
                if not any(pat in hdr_clean for pat in exclude_addr_patterns):
                    address_candidates.append((input_headers[h_idx], addr_score))

    for t_idx, h_idx in zip(row_ind, col_ind):
        if t_idx >= n_targets or h_idx >= n_inputs:
            continue
        score = score_matrix[t_idx, h_idx]
        if score >= threshold or score >= 80.0:
            mappings[target_cols[t_idx]] = input_headers[h_idx]
            confidence[target_cols[t_idx]] = round(min(score, 1.0), 3)
        else:
            confidence[target_cols[t_idx]] = 0.0

    # Fill in missing confidence entries
    for t in target_cols:
        if t not in confidence:
            confidence[t] = 0.0

    # Build output DataFrame
    out = pd.DataFrame()
    for t in target_cols:
        if t in mappings:
            col_data = df[mappings[t]]
            # Safety: if duplicate columns return a DataFrame, take first column
            if isinstance(col_data, pd.DataFrame):
                col_data = col_data.iloc[:, 0]
            out[t] = col_data.values
        else:
            out[t] = None

    # Merge address candidates into raw_address_data
    if address_candidates:
        address_candidates.sort(key=lambda x: x[1], reverse=True)
        cand_cols = list(dict.fromkeys([x[0] for x in address_candidates]))[:3]
        out['raw_address_data'] = df[cand_cols].apply(
            lambda x: ' '.join(x.dropna().astype(str)), axis=1
        )
    else:
        out['raw_address_data'] = out.get('address', "")

    return out, confidence, mappings


# ---- Correction-weighted variant ----

HINT_BIAS = 0.10  # cost reduction per confirmed correction (capped at 5 hits)


def _apply_hard_overrides(raw_headers, normalized, file_type, schema_cols, mappings, confidence):
    """Apply BASE_OVERRIDES + file-type overrides on top of existing mappings.

    Hard override aliases still compete per target: if both "Rate" and
    "Rate PSF" point to rate_psf, only the more specific match survives.
    """
    overrides = dict(BASE_OVERRIDES)
    overrides.update(LEASE_OVERRIDES if file_type in ("LEASE", "lease") else SALE_OVERRIDES)

    best_by_target: dict[str, tuple[float, int, str]] = {}
    for i, raw in enumerate(raw_headers):
        cleaned = normalized[i]
        best_for_raw: tuple[float, str] | None = None
        for target_col in schema_cols:
            score = _find_override(cleaned, overrides, target_col)
            if score and (best_for_raw is None or score > best_for_raw[0]):
                best_for_raw = (score, target_col)
        if best_for_raw is None:
            continue
        score, target_col = best_for_raw
        current = best_by_target.get(target_col)
        if current is None or (score, -i) > (current[0], -current[1]):
            best_by_target[target_col] = (score, i, raw)

    for target_col, (_, _, raw) in best_by_target.items():
        for existing_raw, existing_target in list(mappings.items()):
            if existing_raw == raw or existing_target == target_col:
                mappings.pop(existing_raw, None)
                confidence.pop(existing_raw, None)
        mappings[raw] = target_col
        confidence[raw] = 1.0


def generate_standardized_df_with_hints(
    df: "pd.DataFrame",
    schema_dict: dict,
    file_type: str,
    store=None,
    threshold: float = 0.55,
):
    """Same as generate_standardized_df but applies correction hints to the cost matrix.

    Returns (out_df, mappings, confidence) where:
        mappings    = {raw_header: target_schema_col}
        confidence  = {raw_header: float}

    If store is None or has no corrections, behaviour matches the standard
    embedding pipeline (modulo type bonuses — this variant omits them to keep
    the cost matrix well-conditioned when hints are applied).
    """
    raw_headers = [str(c) for c in df.columns]
    normalized = [clean_header(h) or "unknown column" for h in raw_headers]

    schema_cols = list(schema_dict.keys())

    header_embeds = get_embeddings(normalized)
    schema_embeds = _get_schema_embeddings(schema_dict)

    header_arr = np.nan_to_num(np.array(header_embeds, dtype=float), nan=0.0, posinf=0.0, neginf=0.0)
    schema_arr = np.nan_to_num(np.array(schema_embeds, dtype=float), nan=0.0, posinf=0.0, neginf=0.0)

    # cosine similarity → cost (1 - sim)
    norms_h = np.linalg.norm(header_arr, axis=1, keepdims=True) + 1e-12
    norms_s = np.linalg.norm(schema_arr, axis=1, keepdims=True) + 1e-12
    with np.errstate(divide="ignore", invalid="ignore", over="ignore"):
        sim = (header_arr / norms_h) @ (schema_arr / norms_s).T   # shape (n_headers, n_schema)
    sim = np.nan_to_num(sim, nan=0.0, posinf=1.0, neginf=-1.0)
    cost = 1.0 - sim

    # Apply hint bias — reduce cost for correction-confirmed assignments
    if store is not None:
        for i, norm_h in enumerate(normalized):
            corrections = store.get_corrections_for_context(
                file_type=file_type, raw_header=norm_h
            )
            for target_col, weight in corrections.items():
                if target_col in schema_cols:
                    j = schema_cols.index(target_col)
                    cost[i, j] -= HINT_BIAS * min(weight, 5) / 5.0

    n_h, n_s = cost.shape
    max_dim = max(n_h, n_s)
    padded = np.full((max_dim, max_dim), 1.0)  # default cost = 1 (no match)
    padded[:n_h, :n_s] = cost

    row_ind, col_ind = linear_sum_assignment(padded)

    mappings: dict[str, str] = {}
    confidence: dict[str, float] = {}
    for i, j in zip(row_ind, col_ind):
        if i >= n_h or j >= n_s:
            continue
        score = sim[i, j]
        if score >= threshold:
            mappings[raw_headers[i]] = schema_cols[j]
            confidence[raw_headers[i]] = float(round(score, 3))

    # Hard overrides take precedence over embedding assignment
    _apply_hard_overrides(raw_headers, normalized, file_type, schema_cols, mappings, confidence)
    mappings = dedupe_mappings_by_target(mappings, raw_headers, confidence)

    # Build output DataFrame
    out = pd.DataFrame()
    for raw, target in mappings.items():
        col_data = df[raw]
        if isinstance(col_data, pd.DataFrame):
            col_data = col_data.iloc[:, 0]
        out[target] = col_data.values

    return out, mappings, confidence
