import streamlit as st
import pandas as pd
import os
import math
# Force reload of database schema
import database
import importlib
importlib.reload(database)
from database import Session, SaleComp, LeaseComp, engine
from comp_engine import robust_load_csv, process_file_to_clean_output, fetch_google_data

# --- HELPER: MATH ---
def haversine_miles(lat1, lon1, lat2, lon2):
    if any(x is None for x in [lat1, lon1, lat2, lon2]): return 99999
    R = 3958.8 
    try:
        dlat = math.radians(lat2 - lat1)
        dlon = math.radians(lon2 - lon1)
        a = math.sin(dlat/2)**2 + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon/2)**2
        c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
        return R * c
    except: return 99999

# --- HELPER: DATA CLEANING ---
def clean_currency_num(value):
    """Converts money/SF strings to pure Floats."""
    if pd.isna(value) or value == "": return None
    s = str(value).strip().replace(',', '').replace('$', '').replace('%', '').lower().replace('sf', '')
    try: return float(s)
    except: return None

def clean_text_val(value):
    """Converts objects to pure Strings (preserves text)."""
    if pd.isna(value) or value == "" or value is None: return None
    return str(value).strip()

# --- HELPER: FILTERS ---
def render_numeric_filter(df, column, label):
    if column not in df.columns: return pd.Series([True] * len(df))
    
    # Handle NaNs gracefully for min/max
    col_data = df[column].dropna()
    if col_data.empty:
        min_val, max_val = 0.0, 1000000.0
    else:
        min_val, max_val = float(col_data.min()), float(col_data.max())
    
    c1, c2 = st.sidebar.columns(2)
    val_min = c1.number_input(f"Min {label}", value=None, placeholder=f"{min_val:,.0f}")
    val_max = c2.number_input(f"Max {label}", value=None, placeholder=f"{max_val:,.0f}")
    
    mask = pd.Series([True] * len(df))
    if val_min is not None: mask &= (df[column] >= val_min)
    if val_max is not None: mask &= (df[column] <= val_max)
    return mask

def render_text_filter(df, column, label):
    if column not in df.columns: return pd.Series([True] * len(df))
    search = st.sidebar.text_input(f"{label} contains:", placeholder="Search...")
    if search: return df[column].astype(str).str.contains(search, case=False, na=False)
    return pd.Series([True] * len(df))

# --- APP CONFIG ---
st.set_page_config(page_title="Harbor Capital Comp Database", layout="wide")
st.title("🏢 Real Estate Comp Intelligence")
page = st.sidebar.radio("Navigate", ["Upload & Process", "Database View"])

# --- PAGE 1: UPLOAD ---
if page == "Upload & Process":
    st.header("1. Upload Raw Comp Sheets")
    uploaded_file = st.file_uploader("Upload Excel/CSV", type=['csv', 'xlsx'])
    
    if uploaded_file:
        with open(f"temp_{uploaded_file.name}", "wb") as f: f.write(uploaded_file.getbuffer())
        path = f"temp_{uploaded_file.name}"
        
        with st.spinner('AI is reading & geocoding...'):
            try:
                df_input = robust_load_csv(path)
                if df_input is not None:
                    clean_df = process_file_to_clean_output(df_input, uploaded_file.name)
                    st.success(f"Processed {uploaded_file.name} successfully!")
                    
                    st.subheader("2. Preview & Edit Data")
                    edited_df = st.data_editor(clean_df, num_rows="dynamic")
                    
                    if st.button("💾 Save to Database", type="primary"):
                        session = Session()
                        records = []
                        stype = clean_df['source_type'].iloc[0]
                        
                        # DEBUG: Show user what is happening
                        progress_bar = st.progress(0)
                        
                        for i, row in edited_df.iterrows():
                            # Common fields
                            common = {
                                'address': clean_text_val(row.get('address')),
                                'latitude': row.get('latitude'),
                                'longitude': row.get('longitude'),
                                'raw_address_data': clean_text_val(row.get('raw_address_data')),
                                'source_file': uploaded_file.name,
                                'notes': clean_text_val(row.get('notes'))
                            }
                            
                            if stype == "SALE":
                                rec = SaleComp(
                                    **common,
                                    sale_price=clean_currency_num(row.get('sale_price')),
                                    building_size=clean_currency_num(row.get('building_size')),
                                    price_per_sf=clean_currency_num(row.get('price_per_sf')),
                                    closing_date=clean_text_val(row.get('closing_date')),
                                    year_built=clean_currency_num(row.get('year_built')),
                                    cap_rate=clean_currency_num(row.get('cap_rate')),
                                    buyer=clean_text_val(row.get('buyer')), 
                                    seller=clean_text_val(row.get('seller'))
                                )
                            elif stype == "LEASE":
                                # Explicitly forcing text values here
                                rec = LeaseComp(
                                    **common,
                                    tenant_name=clean_text_val(row.get('tenant_name')),
                                    leased_sf=clean_currency_num(row.get('leased_sf')),
                                    rate_psf=clean_currency_num(row.get('rate_psf')),
                                    term_months=clean_currency_num(row.get('term_months')),
                                    commencement_date=clean_text_val(row.get('commencement_date')),
                                    ti_allowance=clean_currency_num(row.get('ti_allowance')),
                                    free_rent=clean_text_val(row.get('free_rent')),
                                    lease_type=clean_text_val(row.get('lease_type')),
                                    
                                    # THE FIX: Using clean_text_val ensures these don't get dropped
                                    escalations=clean_text_val(row.get('escalations')),
                                    building_type=clean_text_val(row.get('building_type')),
                                    clear_height=clean_currency_num(row.get('clear_height'))
                                )
                            records.append(rec)
                            progress_bar.progress((i + 1) / len(edited_df))

                        session.add_all(records)
                        session.commit()
                        session.close()
                        st.balloons()
                        st.success(f"Saved {len(records)} records to database.")
                        
                        if os.path.exists(path): os.remove(path)
            except Exception as e: st.error(f"Error: {e}")

# --- PAGE 2: DATABASE VIEW ---
if page == "Database View":
    st.header("🗄️ Database Explorer")
    view_type = st.radio("Select Data Type", ["Sales Comps", "Lease Comps"], horizontal=True)
    
    session = Session()
    model = SaleComp if view_type == "Sales Comps" else LeaseComp
    df = pd.read_sql(session.query(model).statement, session.bind)
    session.close()
    
    if df.empty:
        st.info("Database is empty.")
    else:
        # Search UI
        st.sidebar.header("📍 Location Search")
        center_addr = st.sidebar.text_input("Address (Nearby)", placeholder="e.g. 123 Main St")
        radius = st.sidebar.slider("Radius (Miles)", 1, 50, 5)
        
        st.sidebar.markdown("---")
        st.sidebar.header("🔍 Filters")
        
        mask = pd.Series([True] * len(df))
        
        # --- SALES FILTERS ---
        if view_type == "Sales Comps":
            with st.sidebar.expander("💰 Financials", expanded=True):
                mask &= render_numeric_filter(df, 'sale_price', 'Price')
                mask &= render_numeric_filter(df, 'price_per_sf', '$/SF')
                mask &= render_numeric_filter(df, 'cap_rate', 'Cap Rate')
            
            with st.sidebar.expander("🏢 Property Details", expanded=False):
                mask &= render_numeric_filter(df, 'building_size', 'Size (SF)')
                mask &= render_numeric_filter(df, 'year_built', 'Year Built')
                mask &= render_text_filter(df, 'address', 'Address')
                
            with st.sidebar.expander("🤝 Deal Info", expanded=False):
                mask &= render_text_filter(df, 'buyer', 'Buyer')
                mask &= render_text_filter(df, 'seller', 'Seller')
                mask &= render_text_filter(df, 'closing_date', 'Date')
                mask &= render_text_filter(df, 'notes', 'Notes')

        # --- LEASE FILTERS ---
        elif view_type == "Lease Comps":
            with st.sidebar.expander("💰 Economics", expanded=True):
                mask &= render_numeric_filter(df, 'rate_psf', 'Rate ($/SF)')
                mask &= render_numeric_filter(df, 'ti_allowance', 'TI ($)')
                mask &= render_numeric_filter(df, 'term_months', 'Term (Mos)')
                mask &= render_text_filter(df, 'escalations', 'Escalations') # Filter for the new column
                
            with st.sidebar.expander("🏢 Property & Tenant", expanded=False):
                mask &= render_numeric_filter(df, 'leased_sf', 'Leased SF')
                mask &= render_numeric_filter(df, 'clear_height', 'Clear Height')
                mask &= render_text_filter(df, 'tenant_name', 'Tenant')
                mask &= render_text_filter(df, 'building_type', 'Bldg Type') # Filter for the new column
                mask &= render_text_filter(df, 'address', 'Address')
                
            with st.sidebar.expander("📅 Dates & Notes", expanded=False):
                mask &= render_text_filter(df, 'commencement_date', 'Start Date')
                mask &= render_text_filter(df, 'free_rent', 'Free Rent')
                mask &= render_text_filter(df, 'notes', 'Notes')

        # --- APPLY DISTANCE FILTER ---
        if center_addr:
            with st.spinner("Calculating distances..."):
                _, lat_c, lon_c = fetch_google_data(center_addr)
                if lat_c:
                    df['distance_miles'] = df.apply(lambda x: haversine_miles(lat_c, lon_c, x['latitude'], x['longitude']), axis=1)
                    mask &= (df['distance_miles'] <= radius)
                else:
                    st.error("Could not find that address.")
        
        # --- DISPLAY RESULTS ---
        df_filtered = df[mask]
        st.subheader(f"Showing {len(df_filtered)} / {len(df)} Records")
        
        if not df_filtered.empty:
            if 'distance_miles' in df_filtered.columns:
                df_filtered = df_filtered.sort_values('distance_miles')
            st.dataframe(df_filtered, use_container_width=True)
            if 'latitude' in df_filtered.columns and df_filtered['latitude'].notnull().any():
                st.map(df_filtered[['latitude', 'longitude']].dropna())
        else:
            st.warning("No records match your filters.")
            
        st.sidebar.markdown("---")
        if st.sidebar.button("⚠️ Clear All Data"):
            session = Session()
            session.query(SaleComp).delete()
            session.query(LeaseComp).delete()
            session.commit()
            session.close()
            st.rerun()