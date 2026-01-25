import pandas as pd
import re
import os
import numpy as np
import requests
from sentence_transformers import SentenceTransformer
from scipy.spatial.distance import cosine
from dateutil.parser import parse

# --- 1. CONFIGURATION ---
GOOGLE_API_KEY = "YOUR_GOOGLE_API_KEY_HERE"

model = SentenceTransformer('all-MiniLM-L6-v2')

LEASE_SCHEMA = {
    'address': {'desc': "address location property city state zip street", 'type': 'text'},
    'tenant_name': {'desc': "tenant name lessee company occupant business", 'type': 'text'},
    'leased_sf': {'desc': "size sqft square footage area dimensions rba leased space", 'type': 'numeric_clean_large'}, 
    'rate_psf': {'desc': "rent rate price cost base rent annual rent monthly", 'type': 'numeric_money'},
    'lease_type': {'desc': "lease type structure nnn gross full service", 'type': 'text'},
    'term_months': {'desc': "term months duration length years", 'type': 'numeric_clean_small'},
    'commencement_date': {'desc': "commencement start date move in possession", 'type': 'date'},
    'escalations': {'desc': "escalations bumps increases steps annual increase", 'type': 'text'},
    'ti_allowance': {'desc': "ti allowance work letter improvement allowance construction", 'type': 'numeric_money'},
    'free_rent': {'desc': "free rent abatement concessions months free", 'type': 'text'},
    'clear_height': {'desc': "clear height ceiling height clearance", 'type': 'numeric_clean_small'},
    'building_type': {'desc': "building type construction class metal tilt wall", 'type': 'text'},
    'notes': {'desc': "notes comments details observations", 'type': 'text'}
}

SALE_SCHEMA = {
    'address': {'desc': "address location property city state zip street", 'type': 'text'},
    'sale_price': {'desc': "sale price purchase price price cost transaction value", 'type': 'numeric_money'},
    'building_size': {'desc': "size sqft square footage area dimensions rba building sf", 'type': 'numeric_clean_large'}, 
    'price_per_sf': {'desc': "price per sf price/sf rate psf unit price", 'type': 'numeric_money'},
    'closing_date': {'desc': "closing date sold date transaction date", 'type': 'date'},
    'year_built': {'desc': "year built age renovated constructed", 'type': 'numeric_clean_large'}, 
    'cap_rate': {'desc': "cap rate capitalization yield return", 'type': 'numeric_mixed'},
    'buyer': {'desc': "buyer purchaser acquirer buying entity", 'type': 'text'},
    'seller': {'desc': "seller vendor grantor selling entity", 'type': 'text'},
    'notes': {'desc': "notes comments details observations", 'type': 'text'}
}

# --- LOADER & HELPERS ---
def robust_load_csv(file_path):
    print(f"   > Loading: {os.path.basename(file_path)}")
    try:
        df_raw = pd.read_csv(file_path, header=None, nrows=30)
    except: return None
    keywords = {'address', 'city', 'tenant', 'buyer', 'seller', 'date', 'price', 'sqft', 'size', 'rate', 'term'}
    best_row_idx, max_score = -1, 0
    for idx, row in df_raw.iterrows():
        row_text = " ".join(row.dropna().astype(str)).lower()
        score = sum(1 for k in keywords if k in row_text)
        if score > max_score: max_score, best_row_idx = score, idx
    if best_row_idx == -1: return pd.read_csv(file_path)
    df = pd.read_csv(file_path, header=best_row_idx)
    df = df.dropna(how='all') 
    return df

def clean_header(header):
    return re.sub(r'[^\w\s]', '', str(header).lower().replace('_', ' ').replace('.', ' ')).strip()

def get_column_profile(series):
    sample = series.dropna().astype(str).head(10).tolist()
    if not sample: return 'empty'
    joined = " ".join(sample).lower()
    if 'acre' in joined: return 'numeric_clean_small'
    
    clean = [re.sub(r'[$,%a-zA-Z]', '', x) for x in sample]
    nums = []
    for x in clean:
        try: nums.append(float(x.replace(',', '')))
        except: pass
    
    is_numeric = len(nums)/len(sample) > 0.5 if sample else False
    if not is_numeric:
        try: 
            parse(sample[0])
            if any(c in sample[0] for c in ['/', '-', ',']): return 'date'
        except: pass
    
    if is_numeric:
        if '$' in joined: return 'numeric_money'
        avg = sum(nums)/len(nums) if nums else 0
        return 'numeric_clean_large' if avg > 500 else 'numeric_clean_small'
    return 'text'

def classify_file_type(headers, filename=""):
    fname = str(filename).lower()
    lease_score = 10 if any(x in fname for x in ['lease', 'leasing', 'tenant']) else 0
    sale_score = 10 if any(x in fname for x in ['sale', 'sold', 'transaction']) else 0
    triggers_lease = {'tenant', 'lessee', 'term', 'commencement', 'rent'}
    triggers_sale = {'buyer', 'seller', 'closing', 'cap rate', 'purchase'}
    h_text = " ".join([str(h).lower() for h in headers])
    lease_score += sum(1 for t in triggers_lease if t in h_text)
    sale_score += sum(1 for t in triggers_sale if t in h_text)
    
    if lease_score > sale_score: return "LEASE"
    elif sale_score > lease_score: return "SALE"
    else: return "BOTH"

# --- CORE MAPPING LOGIC ---
def generate_standardized_df(df, schema_dict, file_type, threshold=0.20):
    input_headers = df.columns.tolist()
    clean_headers = [clean_header(h) for h in input_headers]
    target_cols = list(schema_dict.keys())
    col_profiles = [get_column_profile(df[col]) for col in input_headers]
    
    # --- OVERRIDES ---
    overrides = {
        'price per sf': 'price_per_sf', 'sale price psf': 'price_per_sf', 'pps': 'price_per_sf',
        'rent': 'rate_psf', 'base rent': 'rate_psf',
        'date closed': 'closing_date', 'closing date': 'closing_date',
        'esc': 'escalations', 'escalation': 'escalations', 'steps': 'escalations',
        'construction': 'building_type', 'months': 'term_months', 'loading': 'building_type',
        'comments': 'notes', 'notes': 'notes'
    }
    
    if file_type == "LEASE":
        overrides.update({'sf': 'leased_sf', 'size': 'leased_sf', 'sqft': 'leased_sf'})
    else:
        overrides.update({'sf': 'building_size', 'size': 'building_size', 'sqft': 'building_size', 
                          'sale price': 'sale_price', 'price': 'sale_price'})

    head_vecs = model.encode(clean_headers)
    target_vecs = model.encode([schema_dict[k]['desc'] for k in target_cols])
    
    mappings = {}
    addr_candidates = []
    
    for t_idx, target_col in enumerate(target_cols):
        target_conf = schema_dict[target_col]
        best_score, best_match = -1, None
        
        for h_idx, h_vec in enumerate(head_vecs):
            in_clean = clean_headers[h_idx]
            if in_clean in overrides and overrides[in_clean] == target_col:
                best_score, best_match = 100.0, input_headers[h_idx]
            
            if best_score < 50.0:
                sem_score = 1 - cosine(h_vec, target_vecs[t_idx])
                bonus = 0.25 if target_conf['type'] == col_profiles[h_idx] else 0.0
                if target_conf['type'] != col_profiles[h_idx] and 'numeric' in target_conf['type']: bonus = -0.5
                if (sem_score + bonus) > best_score:
                    best_score, best_match = (sem_score + bonus), input_headers[h_idx]
            
            addr_score = 1 - cosine(h_vec, target_vecs[target_cols.index('address')])
            if target_col == 'address' and addr_score > threshold:
                addr_candidates.append((input_headers[h_idx], addr_score))
        
        if best_score > threshold: mappings[target_col] = best_match

    out = pd.DataFrame()
    for t in target_cols: out[t] = df[mappings[t]] if t in mappings else None
    
    if addr_candidates:
        addr_candidates.sort(key=lambda x: x[1], reverse=True)
        cols = list(dict.fromkeys([x[0] for x in addr_candidates]))
        out['raw_address_data'] = df[cols].apply(lambda x: ' '.join(x.dropna().astype(str)), axis=1)
    else: out['raw_address_data'] = out.get('address', "")
    return out

# --- GOOGLE MAPS ---
def fetch_google_data(raw_text):
    if not isinstance(raw_text, str) or not raw_text.strip(): return None, None, None
    if "YOUR_GOOGLE" in GOOGLE_API_KEY: return raw_text, None, None
    try:
        url = "https://maps.googleapis.com/maps/api/geocode/json"
        res = requests.get(url, params={"address": raw_text, "key": GOOGLE_API_KEY}).json()
        if res['status'] == 'OK':
            top = res['results'][0]
            return top['formatted_address'], top['geometry']['location']['lat'], top['geometry']['location']['lng']
        return raw_text, None, None
    except: return raw_text, None, None

def process_file_to_clean_output(df, filename):
    print(f"Processing {filename}...")
    ftype = classify_file_type(df.columns, filename)
    schema = LEASE_SCHEMA if ftype == "LEASE" else SALE_SCHEMA
    
    clean_df = generate_standardized_df(df, schema, ftype)
    
    if ftype == "SALE" and 'sale_price' in clean_df.columns and 'building_size' in clean_df.columns:
        def to_f(v): 
            try: return float(str(v).replace(',','').replace('$','').replace('sf',''))
            except: return None
        calc_psf = clean_df['sale_price'].apply(to_f) / clean_df['building_size'].apply(to_f)
        clean_df['price_per_sf'] = clean_df['price_per_sf'].apply(to_f).fillna(calc_psf.round(2))

    print("   > Fetching Coordinates from Google...")
    if 'raw_address_data' in clean_df.columns:
        results = clean_df['raw_address_data'].apply(fetch_google_data)
        clean_df['address'] = [x[0] for x in results]
        clean_df['latitude'] = [x[1] for x in results]
        clean_df['longitude'] = [x[2] for x in results]

    clean_df['source_type'] = ftype
    clean_df['source_file'] = filename
    return clean_df