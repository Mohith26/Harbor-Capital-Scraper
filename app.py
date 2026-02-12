import streamlit as st
import pandas as pd
import os
import math
import database
import importlib
importlib.reload(database)
from database import Session, SaleComp, LeaseComp, engine
from comp_engine import robust_load_csv, process_file_to_clean_output, fetch_google_data

# --- !!! PASTE YOUR API KEY HERE !!! ---
GOOGLE_API_KEY = "AIzaSyBmUCJx-ufGcel4r-SDv_mTZ_Dc9BbgYX4"
# ---------------------------------------

# --- HELPERS ---
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

def clean_currency_num(value):
    if pd.isna(value) or value == "": return None
    s = str(value).strip().replace(',', '').replace('$', '').replace('%', '').lower().replace('sf', '')
    try: return float(s)
    except: return None

def clean_text_val(value):
    if pd.isna(value) or value == "" or value is None: return None
    return str(value).strip()

def generate_kml(df):
    """Generates KML file content for Google Earth."""
    kml = ['<?xml version="1.0" encoding="UTF-8"?>']
    kml.append('<kml xmlns="http://www.opengis.net/kml/2.2">')
    kml.append('<Document>')
    for idx, row in df.iterrows():
        if pd.notnull(row.get('latitude')) and pd.notnull(row.get('longitude')):
            kml.append('<Placemark>')
            kml.append(f"<name>{row.get('address', 'Unknown Property')}</name>")
            desc = f"Size: {row.get('building_size') or row.get('leased_sf') or 'N/A'}\n"
            desc += f"Price/Rate: {row.get('sale_price') or row.get('rate_monthly') or 'N/A'}"
            kml.append(f"<description>{desc}</description>")
            kml.append('<Point>')
            kml.append(f"<coordinates>{row['longitude']},{row['latitude']},0</coordinates>")
            kml.append('</Point>')
            kml.append('</Placemark>')
    kml.append('</Document>')
    kml.append('</kml>')
    return "\n".join(kml)

# --- APP CONFIG ---
st.set_page_config(page_title="Harbor Capital Comp Database", layout="wide")
st.title("Harbor Capital Comp Database")

# Reset Filter Logic
if 'reset_filters' not in st.session_state:
    st.session_state.reset_filters = False

def reset_callback():
    for key in st.session_state.keys():
        if "filter_" in key:
            st.session_state[key] = None
        if "radius" in key:
            st.session_state[key] = 5

# --- FILTER WIDGETS ---
def render_numeric_filter(df, column, label):
    if column not in df.columns: return pd.Series([True] * len(df))
    col_data = df[column].dropna()
    min_v = float(col_data.min()) if not col_data.empty else 0.0
    max_v = float(col_data.max()) if not col_data.empty else 1000000.0
    c1, c2 = st.sidebar.columns(2)
    val_min = c1.number_input(f"Min {label}", value=None, placeholder=f"{min_v:,.0f}", key=f"filter_min_{column}")
    val_max = c2.number_input(f"Max {label}", value=None, placeholder=f"{max_v:,.0f}", key=f"filter_max_{column}")
    mask = pd.Series([True] * len(df))
    if val_min is not None: mask &= (df[column] >= val_min)
    if val_max is not None: mask &= (df[column] <= val_max)
    return mask

def render_text_filter(df, column, label):
    if column not in df.columns: return pd.Series([True] * len(df))
    search = st.sidebar.text_input(f"{label} contains:", placeholder="Search...", key=f"filter_txt_{column}")
    if search: return df[column].astype(str).str.contains(search, case=False, na=False)
    return pd.Series([True] * len(df))

# --- SESSION STATE INIT ---
if 'clean_df' not in st.session_state: st.session_state.clean_df = None
if 'current_filename' not in st.session_state: st.session_state.current_filename = ""

page = st.sidebar.radio("Navigate", ["Upload & Process", "Database View"])

# =====================================================================
# PAGE 1: UPLOAD & PROCESS
# =====================================================================
if page == "Upload & Process":
    st.header("Upload Raw Comp Sheets")
    uploaded_file = st.file_uploader("Upload Excel/CSV", type=['csv', 'xlsx'])

    if uploaded_file:
        with open(f"temp_{uploaded_file.name}", "wb") as f: f.write(uploaded_file.getbuffer())
        path = f"temp_{uploaded_file.name}"

        if st.session_state.current_filename != uploaded_file.name:
            with st.spinner('Analyzing columns...'):
                df_input = robust_load_csv(path)
                if df_input is not None:
                    st.session_state.clean_df = process_file_to_clean_output(df_input, uploaded_file.name)
                    st.session_state.current_filename = uploaded_file.name
                    st.success("File parsed successfully!")

        if st.session_state.clean_df is not None:
            df = st.session_state.clean_df
            stype = df['source_type'].iloc[0]
            missing_geos = df['latitude'].isna().sum()

            st.markdown("---")
            st.subheader("Geocoding")

            if missing_geos > 0:
                st.warning(f"File contains **{len(df)}** properties. {missing_geos} need geocoding.")
                if st.button("Approve & Geocode"):
                    progress_bar = st.progress(0)
                    results = []
                    for i, row in df.iterrows():
                        addr, lat, lng = fetch_google_data(row['raw_address_data'], GOOGLE_API_KEY)
                        results.append((addr, lat, lng))
                        progress_bar.progress((i + 1) / len(df))
                    df['address'] = [x[0] for x in results]
                    df['latitude'] = [x[1] for x in results]
                    df['longitude'] = [x[2] for x in results]
                    st.session_state.clean_df = df
                    st.rerun()
            else:
                st.success("All addresses have been geocoded.")

            st.markdown("---")
            st.subheader("Preview & Save")

            cols_to_show = list(df.columns)
            if stype == "LEASE" and 'rate_monthly' in cols_to_show:
                cols_to_show = ['address', 'rate_monthly', 'rate_annually'] + [c for c in cols_to_show if c not in ['address', 'rate_monthly', 'rate_annually']]
            elif stype == "SALE":
                cols_to_show = [c for c in cols_to_show if c not in ['rate_monthly', 'rate_annually']]

            edited_df = st.data_editor(st.session_state.clean_df[cols_to_show], num_rows="dynamic")

            if st.button("Save to Database", type="primary"):
                session = Session()
                records = []
                bar = st.progress(0)
                for i, row in edited_df.iterrows():
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
                        rec = LeaseComp(
                            **common,
                            tenant_name=clean_text_val(row.get('tenant_name')),
                            leased_sf=clean_currency_num(row.get('leased_sf')),
                            rate_monthly=clean_currency_num(row.get('rate_monthly')),
                            rate_annually=clean_currency_num(row.get('rate_annually')),
                            term_months=clean_currency_num(row.get('term_months')),
                            commencement_date=clean_text_val(row.get('commencement_date')),
                            ti_allowance=clean_currency_num(row.get('ti_allowance')),
                            free_rent=clean_text_val(row.get('free_rent')),
                            lease_type=clean_text_val(row.get('lease_type')),
                            escalations=clean_text_val(row.get('escalations')),
                            building_type=clean_text_val(row.get('building_type')),
                            clear_height=clean_currency_num(row.get('clear_height'))
                        )
                    records.append(rec)
                    bar.progress((i + 1) / len(edited_df))

                session.add_all(records)
                session.commit()
                session.close()
                st.success(f"Saved {len(records)} records.")
                if os.path.exists(path): os.remove(path)
                st.session_state.clean_df = None

# =====================================================================
# PAGE 2: DATABASE VIEW
# =====================================================================
if page == "Database View":
    st.header("Database Explorer")
    view_type = st.radio("Select Data Type", ["Sales Comps", "Lease Comps"], horizontal=True)

    session = Session()
    model = SaleComp if view_type == "Sales Comps" else LeaseComp
    df = pd.read_sql(session.query(model).statement, session.bind)
    session.close()

    if df.empty:
        st.info("Database is empty. Upload files on the Upload page.")
    else:
        # --- SIDEBAR ---
        st.sidebar.button("Reset All Filters", on_click=reset_callback)
        st.sidebar.markdown("---")

        st.sidebar.header("Location Search")
        center_addr = st.sidebar.text_input("Address (Nearby)", placeholder="e.g. 123 Main St, Houston TX", key="filter_loc_center")
        radius = st.sidebar.slider("Radius (Miles)", 1, 50, 5, key="filter_loc_radius")

        st.sidebar.markdown("---")
        st.sidebar.header("Filters")

        mask = pd.Series([True] * len(df))

        if view_type == "Sales Comps":
            with st.sidebar.expander("Financials", expanded=True):
                mask &= render_numeric_filter(df, 'sale_price', 'Price')
                mask &= render_numeric_filter(df, 'price_per_sf', '$/SF')
            with st.sidebar.expander("Property Details", expanded=False):
                mask &= render_numeric_filter(df, 'building_size', 'Size (SF)')
                mask &= render_numeric_filter(df, 'year_built', 'Year Built')
                mask &= render_text_filter(df, 'address', 'Address')
            with st.sidebar.expander("Deal Info", expanded=False):
                mask &= render_text_filter(df, 'buyer', 'Buyer')
                mask &= render_text_filter(df, 'notes', 'Notes')

        elif view_type == "Lease Comps":
            with st.sidebar.expander("Economics", expanded=True):
                mask &= render_numeric_filter(df, 'rate_monthly', '$/SF/Mo')
                mask &= render_numeric_filter(df, 'rate_annually', '$/SF/Yr')
                mask &= render_numeric_filter(df, 'ti_allowance', 'TI ($)')
            with st.sidebar.expander("Property & Tenant", expanded=False):
                mask &= render_numeric_filter(df, 'leased_sf', 'Leased SF')
                mask &= render_numeric_filter(df, 'clear_height', 'Clear Height')
                mask &= render_text_filter(df, 'tenant_name', 'Tenant')
                mask &= render_text_filter(df, 'address', 'Address')

        # Distance calculation
        if center_addr:
            with st.spinner("Calculating distances..."):
                _, lat_c, lon_c = fetch_google_data(center_addr, GOOGLE_API_KEY)
                if lat_c:
                    df['distance_miles'] = df.apply(lambda x: haversine_miles(lat_c, lon_c, x['latitude'], x['longitude']), axis=1)
                    mask &= (df['distance_miles'] <= radius)
                else:
                    st.error("Could not find that address.")

        # --- RESULTS ---
        df_filtered = df[mask].copy()
        df_filtered.insert(0, "Select", False)

        st.subheader(f"Showing {len(df_filtered)} / {len(df)} Records")

        if view_type == "Lease Comps":
            cols = list(df_filtered.columns)
            priority = ['Select', 'address', 'rate_monthly', 'rate_annually', 'leased_sf', 'tenant_name']
            cols = priority + [c for c in cols if c not in priority]
            df_filtered = df_filtered[cols]

        edited_view = st.data_editor(
            df_filtered,
            hide_index=True,
            column_config={"Select": st.column_config.CheckboxColumn(required=True)}
        )

        # --- EXPORT ---
        selected_rows = edited_view[edited_view["Select"] == True]

        if not selected_rows.empty:
            st.success(f"Selected {len(selected_rows)} properties.")
            kml_data = generate_kml(selected_rows)
            st.download_button(
                label="Download Selection as KML (Google Earth)",
                data=kml_data,
                file_name="selected_properties.kml",
                mime="application/vnd.google-earth.kml+xml"
            )

        if not df_filtered.empty and 'latitude' in df_filtered.columns and df_filtered['latitude'].notnull().any():
            st.map(df_filtered[['latitude', 'longitude']].dropna())

        # --- CLEAR DATA (bottom of sidebar) ---
        st.sidebar.markdown("---")
        confirm = st.sidebar.checkbox("I want to delete all data")
        if confirm:
            if st.sidebar.button("Confirm: Clear All Data"):
                session = Session()
                session.query(SaleComp).delete()
                session.query(LeaseComp).delete()
                session.commit()
                session.close()
                st.rerun()
