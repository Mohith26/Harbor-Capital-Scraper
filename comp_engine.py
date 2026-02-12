import pandas as pd
import re
import os
import requests
from sentence_transformers import SentenceTransformer
from scipy.spatial.distance import cosine
from dateutil.parser import parse

# --- 1. SETUP ---
model = SentenceTransformer('all-MiniLM-L6-v2')

HOUSTON_RATE_THRESHOLD = 4.0  # Configurable: rates <= this are assumed monthly

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

# --- 2. HEADER DETECTION KEYWORDS ---
HEADER_KEYWORDS = {
    'address', 'city', 'tenant', 'buyer', 'seller', 'date', 'price', 'sqft',
    'size', 'rate', 'term', 'commencement', 'lessee', 'sf', 'psf', 'rent',
    'building', 'property', 'lease', 'cap', 'closing', 'year', 'type',
    'height', 'notes',
}

# --- 3. FILE LOADER (CSV + XLSX) ---

def robust_load_file(file_path):
    """Load CSV or Excel file with intelligent header detection and split-header merging."""
    ext = os.path.splitext(file_path)[1].lower()

    # Read first 30 rows to find header
    try:
        if ext in ('.xlsx', '.xls'):
            df_raw = pd.read_excel(file_path, header=None, nrows=30, engine='openpyxl')
        else:
            df_raw = pd.read_csv(file_path, header=None, nrows=30)
    except Exception as e:
        print(f"   > Error reading file: {e}")
        return None

    # Score each row to find the best header
    best_row_idx, max_score = -1, 0
    for idx, row in df_raw.iterrows():
        row_text = " ".join(row.dropna().astype(str)).lower()
        score = sum(1 for k in HEADER_KEYWORDS if k in row_text)
        if score > max_score:
            max_score, best_row_idx = score, idx

    # Require minimum score of 2 to accept a header row
    if best_row_idx == -1 or max_score < 2:
        try:
            if ext in ('.xlsx', '.xls'):
                return pd.read_excel(file_path, engine='openpyxl')
            return pd.read_csv(file_path)
        except:
            return None

    # Reload with detected header
    try:
        if ext in ('.xlsx', '.xls'):
            df = pd.read_excel(file_path, header=best_row_idx, engine='openpyxl')
        else:
            df = pd.read_csv(file_path, header=best_row_idx)
    except:
        return None

    # Check for split/multi-row headers (merge up to 2 sub-header rows)
    df = _merge_split_headers(df)
    df = df.dropna(how='all')
    return df


def _is_data_row(row_text):
    """Heuristic: returns True if a row looks like data rather than a header."""
    if '$' in row_text:
        return True
    if re.search(r'\d{1,2}[/\-]\d{1,2}[/\-]\d{2,4}', row_text):
        return True
    digits = sum(c.isdigit() for c in row_text)
    if len(row_text) > 0 and (digits / len(row_text)) > 0.3:
        return True
    return False


def _merge_split_headers(df):
    """If the first row(s) after the header look like sub-headers, merge them into column names."""
    if len(df) < 1:
        return df

    for _ in range(2):  # Check up to 2 sub-header rows
        if len(df) < 1:
            break
        sub_row = df.iloc[0]
        row_text = " ".join(sub_row.dropna().astype(str))

        if _is_data_row(row_text):
            break

        non_empty = sub_row.dropna().astype(str).str.strip().apply(len).gt(0).sum()
        if non_empty <= len(df.columns) * 0.3:
            break

        # Merge sub-header into column names
        new_columns = []
        for col, sub in zip(df.columns, sub_row):
            col_str = str(col).strip()
            sub_str = str(sub).strip()
            if "Unnamed" in col_str:
                col_str = ""
            if sub_str.lower() == "nan" or not sub_str:
                sub_str = ""
            combined = f"{col_str} {sub_str}".strip()
            new_columns.append(combined if combined else "Unknown")

        df.columns = new_columns
        df = df.iloc[1:].reset_index(drop=True)

    return df


# --- 4. HELPERS ---

def clean_header(header):
    text = str(header).lower().replace('_', ' ').replace('.', ' ').replace('\n', ' ')
    return re.sub(r'[^\w\s]', '', text).strip()


def get_column_profile(series):
    """Profile a column's data type from a sample of values."""
    sample = series.dropna().astype(str).head(20).tolist()
    if not sample:
        return 'empty'

    joined = " ".join(sample).lower()
    has_money = '$' in joined

    # Try numeric detection
    clean = [re.sub(r'[$,%]', '', x).strip() for x in sample]
    numeric_count = 0
    for x in clean:
        try:
            float(x.replace(',', ''))
            numeric_count += 1
        except ValueError:
            pass

    is_numeric = (numeric_count / len(sample)) > 0.7 if sample else False

    # Date detection
    if not is_numeric:
        date_count = 0
        for val in sample[:5]:
            try:
                parse(val, fuzzy=False)
                if any(c in val for c in ['/', '-']):
                    date_count += 1
            except (ValueError, OverflowError):
                pass
        if date_count >= 2:
            return 'date'

    if is_numeric:
        return 'numeric_money' if has_money else 'numeric_clean'
    return 'text'


def classify_file_type(headers, filename=""):
    """Classify file as LEASE, SALE, BOTH, or UNKNOWN based on headers and filename."""
    fname = str(filename).lower()
    lease_score = 10 if any(x in fname for x in ['lease', 'leasing', 'tenant']) else 0
    sale_score = 10 if any(x in fname for x in ['sale', 'sold', 'transaction']) else 0

    clean_headers = [str(h).lower().strip() for h in headers]
    lease_triggers = {'tenant', 'lessee', 'term', 'commencement', 'base rent', 'rent', 'leased'}
    sale_triggers = {'buyer', 'seller', 'closing', 'cap rate', 'purchase', 'sale price', 'deal'}

    lease_score += sum(1 for h in clean_headers if any(t in h for t in lease_triggers))
    sale_score += sum(1 for h in clean_headers if any(t in h for t in sale_triggers))

    if lease_score > sale_score:
        return "LEASE"
    elif sale_score > lease_score:
        return "SALE"
    elif lease_score > 0:
        return "BOTH"
    return "UNKNOWN"


# --- 5. OVERRIDE DICTIONARIES ---

BASE_OVERRIDES = {
    'price per sf': 'price_per_sf', 'sale price psf': 'price_per_sf', 'pps': 'price_per_sf',
    'price psf': 'price_per_sf',
    'rent': 'rate_psf', 'base rent': 'rate_psf', 'rental rate': 'rate_psf',
    'base rent yearly': 'rate_psf', 'base rent monthly': 'rate_psf',
    'date closed': 'closing_date', 'closing date': 'closing_date', 'sale date': 'closing_date',
    'esc': 'escalations', 'escalation': 'escalations', 'escalation percent': 'escalations',
    'steps': 'escalations',
    'construction': 'building_type', 'loading': 'building_type',
    'months': 'term_months', 'lease term': 'term_months',
    'comments': 'notes', 'notes': 'notes',
    'buyer': 'buyer', 'seller': 'seller',
    'cap rate': 'cap_rate', 'in place cap rate': 'cap_rate', 'goingin cap rate': 'cap_rate',
    'sale price': 'sale_price', 'purchase price': 'sale_price',
    'rentable area': 'building_size', 'size sf': 'building_size', 'sizesf': 'building_size',
    'tenant': 'tenant_name', 'tenant name': 'tenant_name', 'lessee': 'tenant_name',
    'commencement': 'commencement_date', 'commencement date': 'commencement_date',
    'start date': 'commencement_date',
    'ti': 'ti_allowance', 'ti allowance': 'ti_allowance', 'work letter': 'ti_allowance',
    'free rent': 'free_rent', 'free rent months': 'free_rent', 'abatement': 'free_rent',
    'clear height': 'clear_height', 'ceiling height': 'clear_height',
    'year built': 'year_built',
    'address': 'address', 'property address': 'address', 'property name': 'address',
    'property': 'address',
}

LEASE_OVERRIDES = {
    'sf': 'leased_sf', 'size': 'leased_sf', 'sqft': 'leased_sf',
    'area leased': 'leased_sf', 'leased sf': 'leased_sf',
    'price': 'rate_psf',
}

SALE_OVERRIDES = {
    'sf': 'building_size', 'size': 'building_size', 'sqft': 'building_size',
    'building size': 'building_size',
    'price': 'sale_price',
}


# --- 6. SEMANTIC COLUMN MAPPER ---

def _find_override(cleaned_header, overrides, target_col):
    """Check if a cleaned header matches any override for a given target column.
    Uses exact match first, then substring containment for longer headers."""
    # Exact match
    if cleaned_header in overrides and overrides[cleaned_header] == target_col:
        return True
    # Substring: check if any override key is contained in the header
    for key, val in overrides.items():
        if val == target_col and len(key) >= 3 and key in cleaned_header:
            return True
    return False


def generate_standardized_df(df, schema_dict, file_type, threshold=0.45):
    """Map input columns to schema using overrides + semantic similarity.
    Returns (standardized_df, mapping_confidence_dict)."""
    input_headers = df.columns.tolist()
    clean_headers = [clean_header(h) for h in input_headers]
    target_cols = list(schema_dict.keys())
    col_profiles = [get_column_profile(df[col]) for col in input_headers]

    # Build override dict for this file type
    overrides = dict(BASE_OVERRIDES)
    if file_type == "LEASE":
        overrides.update(LEASE_OVERRIDES)
    else:
        overrides.update(SALE_OVERRIDES)

    # Encode vectors
    head_vecs = model.encode(clean_headers)
    target_vecs = model.encode([schema_dict[k]['desc'] for k in target_cols])

    mappings = {}
    confidence = {}
    claimed_columns = set()  # Prevent double-matching
    address_candidates = []

    for t_idx, target_col in enumerate(target_cols):
        target_conf = schema_dict[target_col]
        target_type = target_conf['type']
        best_score, best_match, best_h_idx = -1, None, -1

        for h_idx, h_vec in enumerate(head_vecs):
            if h_idx in claimed_columns:
                continue

            in_clean = clean_headers[h_idx]

            # Check overrides first
            if _find_override(in_clean, overrides, target_col):
                best_score, best_match, best_h_idx = 100.0, input_headers[h_idx], h_idx
                break

            # Semantic similarity + type bonus
            sem_score = 1 - cosine(h_vec, target_vecs[t_idx])
            input_type = col_profiles[h_idx]

            bonus = 0.0
            if target_type == input_type:
                bonus = 0.15
            elif target_type in ('numeric_money', 'numeric_clean') and input_type == 'text':
                bonus = -0.10
            elif target_type == 'date' and input_type != 'date':
                bonus = -0.20

            final_score = sem_score + bonus
            if final_score > best_score:
                best_score, best_match, best_h_idx = final_score, input_headers[h_idx], h_idx

            # Track address candidates for raw_address_data merging
            if target_col == 'address':
                addr_score = 1 - cosine(h_vec, target_vecs[t_idx])
                if addr_score > 0.35:
                    address_candidates.append((input_headers[h_idx], addr_score))

        if best_score > threshold and best_h_idx >= 0:
            mappings[target_col] = best_match
            confidence[target_col] = round(best_score if best_score <= 1.0 else 1.0, 3)
            claimed_columns.add(best_h_idx)
        else:
            confidence[target_col] = 0.0

    # Build output DataFrame
    out = pd.DataFrame()
    for t in target_cols:
        out[t] = df[mappings[t]] if t in mappings else None

    # Merge address candidates into raw_address_data
    if address_candidates:
        address_candidates.sort(key=lambda x: x[1], reverse=True)
        cand_cols = list(dict.fromkeys([x[0] for x in address_candidates]))
        out['raw_address_data'] = df[cand_cols].apply(
            lambda x: ' '.join(x.dropna().astype(str)), axis=1
        )
    else:
        out['raw_address_data'] = out.get('address', "")

    return out, confidence


# --- 7. RATE LOGIC ---

def _detect_rate_unit_from_header(rate_header):
    """Parse the original column header to determine if rate is monthly or annual."""
    if rate_header is None:
        return None
    h = str(rate_header).lower()
    monthly_hints = ['monthly', '/mo', 'per month', ' mo ', ' mo.']
    annual_hints = ['annual', 'yearly', '/yr', 'per year', ' yr ', ' yr.', 'annually']
    for hint in monthly_hints:
        if hint in h:
            return 'monthly'
    for hint in annual_hints:
        if hint in h:
            return 'annual'
    return None


def apply_rate_logic(clean_df, rate_header=None, threshold=HOUSTON_RATE_THRESHOLD):
    """Split rate_psf into rate_monthly and rate_annually.
    First checks header text for unit hints, then falls back to magnitude heuristic."""
    clean_df['rate_monthly'] = None
    clean_df['rate_annually'] = None
    clean_df['rate_basis'] = None

    if 'rate_psf' not in clean_df.columns:
        return clean_df

    header_unit = _detect_rate_unit_from_header(rate_header)

    monthly_list, annual_list, basis_list = [], [], []

    for val in clean_df['rate_psf']:
        f_val = _to_float(val)
        if f_val is None:
            monthly_list.append(None)
            annual_list.append(None)
            basis_list.append(None)
            continue

        if header_unit == 'monthly':
            monthly_list.append(round(f_val, 2))
            annual_list.append(round(f_val * 12, 2))
            basis_list.append('monthly_from_header')
        elif header_unit == 'annual':
            annual_list.append(round(f_val, 2))
            monthly_list.append(round(f_val / 12, 2))
            basis_list.append('annual_from_header')
        else:
            # Fallback: magnitude heuristic
            if f_val <= threshold:
                monthly_list.append(round(f_val, 2))
                annual_list.append(round(f_val * 12, 2))
                basis_list.append('monthly_inferred')
            else:
                annual_list.append(round(f_val, 2))
                monthly_list.append(round(f_val / 12, 2))
                basis_list.append('annual_inferred')

    clean_df['rate_monthly'] = monthly_list
    clean_df['rate_annually'] = annual_list
    clean_df['rate_basis'] = basis_list
    return clean_df


def _to_float(v):
    try:
        return float(str(v).replace(',', '').replace('$', '').replace('sf', '').strip())
    except (ValueError, TypeError):
        return None


# --- 8. GEOCODING ---

def fetch_google_data(raw_text, api_key):
    """Geocode an address using the Google Maps Geocoding API.
    Biases results toward Texas for accuracy on CRE comps."""
    if not isinstance(raw_text, str) or not raw_text.strip():
        return None, None, None
    if not api_key or "YOUR_KEY" in api_key:
        return raw_text, None, None

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
            # Bounding box bias: covers all of Texas
            "bounds": "25.84,-106.65|36.50,-93.51",
        }
        res = requests.get(url, params=params).json()
        if res['status'] == 'OK':
            top = res['results'][0]
            return (
                top['formatted_address'],
                top['geometry']['location']['lat'],
                top['geometry']['location']['lng'],
            )
        # Retry without component restriction if zero results
        if res['status'] == 'ZERO_RESULTS':
            params.pop("components", None)
            res = requests.get(url, params=params).json()
            if res['status'] == 'OK':
                top = res['results'][0]
                return (
                    top['formatted_address'],
                    top['geometry']['location']['lat'],
                    top['geometry']['location']['lng'],
                )
        return raw_text, None, None
    except Exception:
        return raw_text, None, None


# --- 9. MAIN PIPELINE ---

def process_file_to_clean_output(df, filename):
    """Full pipeline: classify → map columns → apply rate logic → return (df, confidence)."""
    ftype = classify_file_type(df.columns, filename)
    schema = LEASE_SCHEMA if ftype == "LEASE" else SALE_SCHEMA
    if ftype in ("BOTH", "UNKNOWN"):
        schema = {**LEASE_SCHEMA, **SALE_SCHEMA}

    clean_df, confidence = generate_standardized_df(df, schema, ftype)

    # Calculate price_per_sf for sales if missing
    if ftype == "SALE" and 'sale_price' in clean_df.columns and 'building_size' in clean_df.columns:
        calc_psf = clean_df['sale_price'].apply(_to_float) / clean_df['building_size'].apply(_to_float)
        clean_df['price_per_sf'] = clean_df['price_per_sf'].apply(_to_float).fillna(calc_psf.round(2))

    # Apply Houston lease rate logic
    if ftype == "LEASE":
        # Find the original header that mapped to rate_psf
        rate_header = None
        for col_name in df.columns:
            if clean_header(col_name) in ('rate psf', 'rent', 'base rent', 'base rent yearly',
                                           'base rent monthly', 'rental rate'):
                rate_header = col_name
                break
        # Also check the mappings via confidence dict
        if rate_header is None and 'rate_psf' in confidence and confidence['rate_psf'] > 0:
            # The mapped column header
            for orig_col in df.columns:
                if clean_df.get('rate_psf') is not None:
                    try:
                        if df[orig_col].equals(clean_df['rate_psf']):
                            rate_header = orig_col
                            break
                    except Exception:
                        pass

        clean_df = apply_rate_logic(clean_df, rate_header=rate_header)

    clean_df['latitude'] = None
    clean_df['longitude'] = None
    clean_df['source_type'] = ftype
    clean_df['source_file'] = filename
    return clean_df, confidence
