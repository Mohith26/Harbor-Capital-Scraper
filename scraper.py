import pandas as pd
import re
import os
from sentence_transformers import SentenceTransformer
from scipy.spatial.distance import cosine
from dateutil.parser import parse

# --- 1. SETUP & SCHEMAS ---

model = SentenceTransformer('all-MiniLM-L6-v2')

LEASE_SCHEMA = {
    'address': {'desc': "address location property city state zip street", 'type': 'text'},
    'tenant_name': {'desc': "tenant name lessee company occupant business", 'type': 'text'},
    'leased_sf': {'desc': "size sqft square footage area dimensions rba leased space", 'type': 'numeric_clean'},
    'rate_psf': {'desc': "rent rate price cost base rent annual rent monthly", 'type': 'numeric_money'},
    'lease_type': {'desc': "lease type structure nnn gross full service", 'type': 'text'},
    'term_months': {'desc': "term months duration length years", 'type': 'numeric_clean'},
    'commencement_date': {'desc': "commencement start date move in possession", 'type': 'date'},
    'escalations': {'desc': "escalations bumps increases steps annual increase", 'type': 'text'},
    'ti_allowance': {'desc': "ti allowance work letter improvement allowance construction", 'type': 'numeric_money'},
    'free_rent': {'desc': "free rent abatement concessions months free", 'type': 'text'},
    'clear_height': {'desc': "clear height ceiling height clearance", 'type': 'numeric_clean'},
    'building_type': {'desc': "building type construction class metal tilt wall", 'type': 'text'}
}

SALE_SCHEMA = {
    'address': {'desc': "address location property city state zip street", 'type': 'text'},
    'sale_price': {'desc': "sale price purchase price price cost transaction value", 'type': 'numeric_money'},
    'building_size': {'desc': "size sqft square footage area dimensions rba building sf", 'type': 'numeric_clean'},
    'price_per_sf': {'desc': "price per sf price/sf rate psf unit price", 'type': 'numeric_money'},
    'closing_date': {'desc': "closing date sold date transaction date", 'type': 'date'},
    'year_built': {'desc': "year built age renovated constructed", 'type': 'numeric_clean'},
    'cap_rate': {'desc': "cap rate capitalization yield return", 'type': 'numeric_mixed'},
    'buyer': {'desc': "buyer purchaser acquirer buying entity", 'type': 'text'},
    'seller': {'desc': "seller vendor grantor selling entity", 'type': 'text'}
}

# --- 2. ROBUST CSV LOADER (Improved) ---

def robust_load_csv(file_path):
    print(f"   > Loading: {os.path.basename(file_path)}")
    try:
        # Read first 30 rows to inspect
        df_raw = pd.read_csv(file_path, header=None, nrows=30)
    except Exception as e:
        print(f"   > Error reading file: {e}")
        return None

    # Score rows to find the best header
    keywords = {'address', 'city', 'tenant', 'buyer', 'seller', 'date', 'price', 'sqft', 'size', 'rate', 'term'}
    best_row_idx = -1
    max_score = 0
    
    for idx, row in df_raw.iterrows():
        row_text = " ".join(row.dropna().astype(str)).lower()
        score = sum(1 for k in keywords if k in row_text)
        if score > max_score:
            max_score = score
            best_row_idx = idx

    if best_row_idx == -1:
        print("   > No valid header found. Using default read.")
        return pd.read_csv(file_path)

    print(f"   > Detected Header at Row {best_row_idx}")
    
    # Reload with the detected header
    df = pd.read_csv(file_path, header=best_row_idx)
    
    # CHECK FOR SPLIT HEADERS (Merge Row 0 ONLY if it looks like a header, NOT data)
    try:
        sub_header_row = df.iloc[0]
        row_text = " ".join(sub_header_row.dropna().astype(str))
        
        # Heuristics to detect DATA (and avoid merging it)
        is_data = False
        if '$' in row_text: is_data = True  # Money usually means data
        if re.search(r'\d{1,2}/\d{1,2}/\d{2,4}', row_text): is_data = True # Dates usually mean data
        
        # Check if the "sub-header" has a lot of numbers (Data often has numbers, headers don't)
        numbers = sum(c.isdigit() for c in row_text)
        if len(row_text) > 0 and (numbers / len(row_text)) > 0.3: is_data = True

        non_empty_count = sub_header_row.dropna().astype(str).str.strip().apply(len).gt(0).sum()
        
        if (non_empty_count > len(df.columns) * 0.3) and not is_data:
            print("   > Merging split headers (Row 0 detected as sub-header)...")
            new_columns = []
            for col, sub in zip(df.columns, sub_header_row):
                col_str = str(col).strip()
                sub_str = str(sub).strip()
                if "Unnamed" in col_str: col_str = ""
                if "nan" in sub_str.lower(): sub_str = ""
                combined = f"{col_str} {sub_str}".strip()
                new_columns.append(combined if combined else "Unknown")
            
            df.columns = new_columns
            df = df.iloc[1:].reset_index(drop=True)
        else:
            print("   > Row 0 detected as DATA. Keeping headers as is.")

    except:
        pass

    df = df.dropna(how='all') 
    return df

# --- 3. HELPER FUNCTIONS & PROFILER ---

def clean_header(header):
    text = str(header).lower().replace('_', ' ').replace('.', ' ').replace('\n', ' ')
    return re.sub(r'[^\w\s]', '', text).strip()

def get_column_profile(series):
    sample = series.dropna().astype(str).head(10).tolist()
    if not sample: return 'empty'
    joined_sample = " ".join(sample).lower()
    has_money_sign = '$' in joined_sample
    clean_sample = [re.sub(r'[$,%]', '', x) for x in sample]
    is_numeric = False
    try:
        numeric_count = sum(1 for x in clean_sample if x.replace('.', '', 1).isdigit())
        if numeric_count / len(sample) > 0.7: is_numeric = True
    except: pass
    is_date = False
    if not is_numeric:
        try:
            parse(sample[0])
            if any(c in sample[0] for c in ['/', '-', ',']): is_date = True
        except: pass
    if is_date: return 'date'
    elif is_numeric: return 'numeric_money' if has_money_sign else 'numeric_clean'
    else: return 'text'

def classify_file_type(headers, filename=""):
    fname_clean = str(filename).lower()
    filename_lease_score = 10 if any(x in fname_clean for x in ['lease', 'leasing', 'tenant']) else 0
    filename_sale_score = 10 if any(x in fname_clean for x in ['sale', 'sold', 'transaction']) else 0
    clean_headers = [str(h).lower().strip() for h in headers]
    header_lease_triggers = {'tenant', 'lessee', 'term', 'commencement', 'base rent', 'rent'}
    header_sale_triggers = {'buyer', 'seller', 'closing', 'cap rate', 'purchase', 'sale price', 'deal'}
    
    total_lease = filename_lease_score + sum(1 for h in clean_headers if any(t in h for t in header_lease_triggers))
    total_sale = filename_sale_score + sum(1 for h in clean_headers if any(t in h for t in header_sale_triggers))
    
    if total_lease > total_sale: return "LEASE"
    elif total_sale > total_lease: return "SALE"
    elif total_lease > 0 and total_lease == total_sale: return "BOTH"
    else: return "UNKNOWN"

# --- 4. SEMANTIC MAPPER ---

def generate_standardized_df(df, schema_dict, threshold=0.20):
    input_headers = df.columns.tolist()
    clean_headers = [clean_header(h) for h in input_headers]
    target_cols = list(schema_dict.keys())
    
    print("   > Profiling columns...")
    col_profiles = [get_column_profile(df[col]) for col in input_headers]
    
    # MANUAL OVERRIDES (Updated)
    overrides = {
        'sale price': 'sale_price', 'purchase price': 'sale_price', 'price': 'sale_price', 
        'rentable area': 'building_size', 'size': 'building_size', 'sqft': 'building_size',
        'sizesf': 'building_size', 'size sf': 'building_size', # Handle newline in header
        'price per sf': 'price_per_sf', 'sale price psf': 'price_per_sf',
        'rent': 'rate_psf', 'base rent': 'rate_psf',
        'date closed': 'closing_date', 'closing date': 'closing_date', 'sale date': 'closing_date',
        'buyer': 'buyer', 'seller': 'seller', 
        'cap rate': 'cap_rate', 'in-place cap rate': 'cap_rate', 'goingin cap rate': 'cap_rate'
    }

    header_vectors = model.encode(clean_headers)
    target_descriptions = [schema_dict[k]['desc'] for k in target_cols]
    target_vectors = model.encode(target_descriptions)
    
    mappings = {}
    address_candidates = []
    
    for t_idx, target_col in enumerate(target_cols):
        target_conf = schema_dict[target_col]
        target_vec = target_vectors[t_idx]
        target_type = target_conf['type']
        
        best_score = -1
        best_match = None
        
        for h_idx, header_vec in enumerate(header_vectors):
            input_col_original = input_headers[h_idx]
            input_col_clean = clean_headers[h_idx]
            
            if input_col_clean in overrides and overrides[input_col_clean] == target_col:
                best_score = 100.0
                best_match = input_col_original
            
            if best_score < 50.0:
                sem_score = 1 - cosine(header_vec, target_vec)
                input_type = col_profiles[h_idx]
                data_bonus = 0.0
                if target_type == input_type: data_bonus = 0.15
                elif target_type == 'numeric_clean' and input_type == 'numeric_money': data_bonus = -0.10 
                elif target_type == 'date' and input_type != 'date': data_bonus = -0.20
                final_score = sem_score + data_bonus
                if final_score > best_score:
                    best_score = final_score
                    best_match = input_col_original
            
            sem_score_addr = 1 - cosine(header_vec, target_vectors[target_cols.index('address')])
            if target_col == 'address' and sem_score_addr > threshold:
                address_candidates.append((input_col_original, sem_score_addr))

        if best_score > threshold: 
            mappings[target_col] = best_match

    output = pd.DataFrame()
    for target_col in target_cols:
        output[target_col] = df[mappings[target_col]] if target_col in mappings else None

    if address_candidates:
        address_candidates.sort(key=lambda x: x[1], reverse=True)
        cand_cols = list(dict.fromkeys([x[0] for x in address_candidates]))
        output['raw_address_data'] = df[cand_cols].apply(lambda x: ' '.join(x.dropna().astype(str)), axis=1)
    else:
        output['raw_address_data'] = output.get('address', "")

    return output

def process_file_to_clean_output(df, filename):
    print(f"--- Processing File: {filename} ---")
    file_type = classify_file_type(df.columns, filename)
    print(f"--- Detected Type: {file_type} ---")
    
    if file_type == "LEASE": active_schema = LEASE_SCHEMA
    elif file_type == "SALE": active_schema = SALE_SCHEMA
    else: active_schema = {**LEASE_SCHEMA, **SALE_SCHEMA}

    clean_df = generate_standardized_df(df, active_schema)
    clean_df['source_type'] = file_type
    clean_df['source_file'] = filename
    return clean_df

# --- 5. EXECUTION ---

path_1 = "/Users/mohithgajjela/Harbor Capital Scraper/Aldine Westfield  Rankin - Comp Set - TM 10.9.xlsx - Tilt Wall Sale Comps.csv"

try:
    df_input = robust_load_csv(path_1)
    
    if df_input is not None:
        fname = os.path.basename(path_1)
        print(">>> OUTPUT 1: Clean Data")
        clean = process_file_to_clean_output(df_input, fname)
        
        clean.to_csv("clean_comps4.csv", index=False)
        print("\n>>> SUCCESS: File saved.")
        
        # Verify Columns
        cols = ['address', 'sale_price', 'building_size', 'cap_rate', 'buyer', 'seller']
        print(clean[[c for c in cols if c in clean.columns]].head())
    else:
        print("Failed to load file.")

except Exception as e:
    print(f"Error: {e}")