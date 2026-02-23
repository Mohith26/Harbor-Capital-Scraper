import streamlit as st
import pandas as pd
import os
import io
import math
import base64
import tempfile
import yaml
import streamlit_authenticator as stauth
import folium
from streamlit_folium import st_folium
import plotly.express as px
from database import Session, SaleComp, LeaseComp, engine
from comp_engine import robust_load_file, process_file_to_clean_output, fetch_google_data
from comp_finder import compute_match_scores, compute_ai_scores, blend_scores, load_comps
from storage import upload_file as upload_to_storage
from utils import normalize_address, find_duplicates, haversine_miles

# --- SECRETS ---
def get_secret(key, default=None):
    try:
        return st.secrets[key]
    except Exception:
        return os.environ.get(key, default)

GOOGLE_API_KEY = get_secret("GOOGLE_API_KEY", "")

# --- HELPERS ---
def clean_currency_num(value):
    if pd.isna(value) or value == "":
        return None
    s = str(value).strip().replace(',', '').replace('$', '').replace('%', '').lower().replace('sf', '')
    try:
        return round(float(s), 2)
    except Exception:
        return None

def clean_text_val(value):
    if pd.isna(value) or value == "" or value is None:
        return None
    return str(value).strip()

def generate_kml(df):
    kml = ['<?xml version="1.0" encoding="UTF-8"?>']
    kml.append('<kml xmlns="http://www.opengis.net/kml/2.2">')
    kml.append('<Document>')
    for _, row in df.iterrows():
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

def to_excel_bytes(df):
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine='xlsxwriter') as writer:
        df.to_excel(writer, index=False, sheet_name='Comps')
    return buf.getvalue()

# --- APP CONFIG ---
st.set_page_config(page_title="Harbor Capital Comp Database", layout="wide")

# --- LOAD LOGO IMAGES AS BASE64 ---
def _load_image_b64(path):
    try:
        with open(path, "rb") as f:
            return base64.b64encode(f.read()).decode()
    except Exception:
        return None

_logo_b64 = _load_image_b64("HC-Logo-Stacked-Left-Charcoal@2000w.png")
_icon_b64 = _load_image_b64("Slate@512w.png")

# --- GLOBAL CSS ---
st.markdown("""
<style>
    .section-header {
        color: #333333;
        font-size: 1.3rem;
        font-weight: 700;
        margin: 1.2rem 0 0.3rem 0;
        padding-bottom: 0.4rem;
        border-bottom: 2px solid #F5A623;
    }
    .section-subtitle {
        color: #666;
        font-size: 0.85rem;
        margin-top: -0.2rem;
        margin-bottom: 0.8rem;
    }
    .metric-card {
        background: linear-gradient(135deg, #333333 0%, #4a4a4a 100%);
        border-radius: 10px;
        padding: 1rem 1.2rem;
        color: white;
        text-align: center;
        margin-bottom: 0.5rem;
        border-left: 4px solid #F5A623;
    }
    .metric-card .metric-value {
        font-size: 1.6rem;
        font-weight: 700;
        line-height: 1.2;
        color: #F5A623;
    }
    .metric-card .metric-label {
        font-size: 0.8rem;
        opacity: 0.85;
        margin-top: 0.2rem;
    }
    .step-row {
        display: flex;
        align-items: center;
        gap: 0.6rem;
        margin: 0.6rem 0;
    }
    .step-circle {
        width: 30px;
        height: 30px;
        border-radius: 50%;
        display: flex;
        align-items: center;
        justify-content: center;
        font-weight: 700;
        font-size: 0.85rem;
        flex-shrink: 0;
    }
    .step-active {
        background-color: #F5A623;
        color: #333333;
    }
    .step-done {
        background-color: #333333;
        color: #F5A623;
    }
    .step-pending {
        background-color: #e0e0e0;
        color: #999;
    }
    .step-label {
        font-weight: 600;
        font-size: 1.05rem;
    }
    .step-label-active { color: #F5A623; }
    .step-label-done { color: #333333; }
    .step-label-pending { color: #999; }
    .badge-filter {
        display: inline-block;
        padding: 0.25em 0.7em;
        border-radius: 12px;
        font-size: 0.78rem;
        font-weight: 600;
        background-color: #FFF3DC;
        color: #333333;
        border: 1px solid #F5A623;
    }
    .record-count {
        color: #555;
        font-size: 0.95rem;
        margin-bottom: 0.5rem;
    }
    .record-count b {
        color: #333333;
        font-size: 1.1rem;
    }
    /* Sidebar logo styling */
    .sidebar-logo {
        padding: 0.5rem 0 1rem 0;
        border-bottom: 2px solid #F5A623;
        margin-bottom: 1rem;
    }
    /* Plotly chart accent override */
    .js-plotly-plot .plotly .modebar-btn path {
        fill: #333333 !important;
    }
</style>
""", unsafe_allow_html=True)

# --- BRAND COLORS FOR CHARTS ---
HC_COLORS = ["#F5A623", "#333333", "#D4910E", "#666666", "#FFC75F", "#999999", "#B37A00", "#CCCCCC"]
HC_SCALE = ["#FFF3DC", "#F5A623", "#333333"]

def section_header(title, subtitle=None):
    st.markdown(f'<div class="section-header">{title}</div>', unsafe_allow_html=True)
    if subtitle:
        st.markdown(f'<div class="section-subtitle">{subtitle}</div>', unsafe_allow_html=True)

def render_step(number, title, status="active"):
    css_circle = {"active": "step-active", "done": "step-done", "pending": "step-pending"}[status]
    css_label = {"active": "step-label-active", "done": "step-label-done", "pending": "step-label-pending"}[status]
    icon = "&#10003;" if status == "done" else str(number)
    st.markdown(f'''<div class="step-row">
        <div class="step-circle {css_circle}">{icon}</div>
        <span class="step-label {css_label}">{title}</span>
    </div>''', unsafe_allow_html=True)

def render_metric_card(label, value):
    st.markdown(f'''<div class="metric-card">
        <div class="metric-value">{value}</div>
        <div class="metric-label">{label}</div>
    </div>''', unsafe_allow_html=True)

# --- DATA CACHING ---
@st.cache_data(ttl=30)
def load_data(model_name):
    from utils import extract_zip_from_address, extract_city_from_address
    session = Session()
    model_cls = SaleComp if model_name == "SaleComp" else LeaseComp
    df = pd.read_sql(session.query(model_cls).statement, session.bind)
    session.close()
    # Pre-convert numeric columns
    if model_name == "SaleComp":
        for col in ['sale_price', 'price_per_sf', 'building_size', 'year_built', 'cap_rate']:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
    else:
        for col in ['rate_monthly', 'rate_annually', 'leased_sf', 'ti_allowance', 'clear_height', 'term_months']:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
    # Round all floats to 2 decimal places
    for col in df.select_dtypes(include=['float64', 'float32']).columns:
        df[col] = df[col].round(2)
    # Backfill city/zip from address for legacy records
    if 'address' in df.columns:
        if 'zip_code' not in df.columns or df['zip_code'].isna().all():
            df['zip_code'] = df['address'].apply(extract_zip_from_address)
        if 'city' not in df.columns or df['city'].isna().all():
            df['city'] = df['address'].apply(extract_city_from_address)
    return df

@st.cache_data(ttl=30)
def get_record_counts():
    session = Session()
    sale_count = session.query(SaleComp).count()
    lease_count = session.query(LeaseComp).count()
    session.close()
    return sale_count, lease_count

# --- AUTHENTICATION ---
with open("auth_config.yaml") as f:
    auth_config = yaml.safe_load(f)

authenticator = stauth.Authenticate(
    auth_config['credentials'],
    auth_config['cookie']['name'],
    auth_config['cookie']['key'],
    auth_config['cookie']['expiry_days'],
)

authenticator.login()

if st.session_state.get("authentication_status") is None:
    st.warning("Please enter your username and password.")
    st.stop()
elif st.session_state.get("authentication_status") is False:
    st.error("Username/password is incorrect.")
    st.stop()

# --- AUTHENTICATED APP ---
user_role = auth_config['credentials']['usernames'].get(
    st.session_state.get("username", ""), {}
).get('role', 'analyst')

if _logo_b64:
    st.markdown(f'<img src="data:image/png;base64,{_logo_b64}" width="320" style="margin-bottom:0.5rem;">', unsafe_allow_html=True)

# Sidebar: logo + user info + logout
if _icon_b64:
    st.sidebar.markdown(f'<img src="data:image/png;base64,{_icon_b64}" width="60" style="margin-bottom:0.5rem;">', unsafe_allow_html=True)
st.sidebar.markdown(f"**{st.session_state.get('name', '')}** &nbsp;|&nbsp; {user_role}")
authenticator.logout("Logout", "sidebar")

# --- SESSION STATE ---
if 'clean_df' not in st.session_state:
    st.session_state.clean_df = None
if 'mapping_confidence' not in st.session_state:
    st.session_state.mapping_confidence = None
if 'current_filename' not in st.session_state:
    st.session_state.current_filename = ""
if 'comparison_ids' not in st.session_state:
    st.session_state.comparison_ids = []
if 'geocoding_done' not in st.session_state:
    st.session_state.geocoding_done = False

# Reset Filter Logic
def reset_callback():
    for key in list(st.session_state.keys()):
        if "filter_" in key:
            del st.session_state[key]

# --- FILTER WIDGETS ---
def render_numeric_filter(df, column, label, container=None):
    sb = container if container is not None else st
    if column not in df.columns:
        return pd.Series([True] * len(df))
    col_data = df[column].dropna()
    if not col_data.empty:
        min_v, max_v = float(col_data.min()), float(col_data.max())
        sb.caption(f"Range: {min_v:,.0f} -- {max_v:,.0f}")
    c1, c2 = sb.columns(2)
    val_min = c1.number_input(f"Min {label}", value=None, placeholder=f"{min_v:,.0f}" if not col_data.empty else "0", key=f"filter_min_{column}")
    val_max = c2.number_input(f"Max {label}", value=None, placeholder=f"{max_v:,.0f}" if not col_data.empty else "0", key=f"filter_max_{column}")
    mask = pd.Series([True] * len(df))
    if val_min is not None:
        mask &= (df[column] >= val_min)
    if val_max is not None:
        mask &= (df[column] <= val_max)
    return mask

def render_text_filter(df, column, label, container=None):
    sb = container if container is not None else st
    if column not in df.columns:
        return pd.Series([True] * len(df))
    search = sb.text_input(f"{label} contains:", placeholder="Search...", key=f"filter_txt_{column}")
    if search:
        return df[column].astype(str).str.contains(search, case=False, na=False)
    return pd.Series([True] * len(df))

def render_categorical_filter(df, column, label, container=None):
    sb = container if container is not None else st
    if column not in df.columns:
        return pd.Series([True] * len(df))
    unique_vals = sorted(df[column].dropna().astype(str).unique().tolist())
    unique_vals = [v for v in unique_vals if v.strip()]
    if not unique_vals:
        return pd.Series([True] * len(df))
    selected = sb.multiselect(label, unique_vals, key=f"filter_cat_{column}")
    if selected:
        return df[column].astype(str).isin(selected)
    return pd.Series([True] * len(df))

def count_active_filters(prefix):
    return sum(1 for k, v in st.session_state.items()
               if k.startswith(prefix) and v is not None and v != [] and v != "" and v != ())

def apply_sidebar_filters(df, view_type, include_proximity=False):
    """Shared filter logic for Database View and Analytics pages. Returns filtered mask."""
    mask = pd.Series([True] * len(df))

    # Location filters
    loc_count = count_active_filters("filter_cat_city") + count_active_filters("filter_cat_zip")
    has_proximity = bool(st.session_state.get("filter_loc_center"))
    total_loc = loc_count + (1 if has_proximity else 0)
    loc_label = f"Location ({total_loc} active)" if total_loc else "Location"
    with st.sidebar.expander(loc_label, expanded=total_loc > 0):
        mask &= render_categorical_filter(df, 'city', 'City')
        mask &= render_categorical_filter(df, 'zip_code', 'Zip Code')
        if include_proximity:
            st.caption("Proximity Search")
            st.text_input("Near address", placeholder="e.g. 123 Main St, Houston TX", key="filter_loc_center")
            st.slider("Radius (mi)", 1, 50, 5, key="filter_loc_radius")

    if view_type == "Sales Comps":
        fin_count = count_active_filters("filter_min_sale") + count_active_filters("filter_max_sale")
        fin_label = f"Financials ({fin_count} active)" if fin_count else "Financials"
        with st.sidebar.expander(fin_label, expanded=True):
            mask &= render_numeric_filter(df, 'sale_price', 'Price ($)')
            mask &= render_numeric_filter(df, 'price_per_sf', '$/SF')
        prop_count = count_active_filters("filter_min_building") + count_active_filters("filter_min_year") + count_active_filters("filter_txt_address")
        prop_label = f"Property Details ({prop_count} active)" if prop_count else "Property Details"
        with st.sidebar.expander(prop_label, expanded=False):
            mask &= render_numeric_filter(df, 'building_size', 'Size (SF)')
            mask &= render_numeric_filter(df, 'year_built', 'Year Built')
            mask &= render_text_filter(df, 'address', 'Address')
        deal_count = count_active_filters("filter_cat_buyer") + count_active_filters("filter_cat_seller") + count_active_filters("filter_txt_notes")
        deal_label = f"Deal Info ({deal_count} active)" if deal_count else "Deal Info"
        with st.sidebar.expander(deal_label, expanded=False):
            mask &= render_categorical_filter(df, 'buyer', 'Buyer')
            mask &= render_categorical_filter(df, 'seller', 'Seller')
            mask &= render_text_filter(df, 'notes', 'Notes')
        with st.sidebar.expander("Date Range", expanded=False):
            if 'closing_date' in df.columns:
                date_range = st.date_input("Closing Date", value=(), key="filter_date_sale")
                if isinstance(date_range, tuple) and len(date_range) == 2:
                    mask &= df['closing_date'].astype(str) >= str(date_range[0])
                    mask &= df['closing_date'].astype(str) <= str(date_range[1])

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
            mask &= render_categorical_filter(df, 'building_type', 'Building Type')
            mask &= render_categorical_filter(df, 'lease_type', 'Lease Type')
        with st.sidebar.expander("Date Range", expanded=False):
            if 'commencement_date' in df.columns:
                date_range = st.date_input("Commencement Date", value=(), key="filter_date_lease")
                if isinstance(date_range, tuple) and len(date_range) == 2:
                    mask &= df['commencement_date'].astype(str) >= str(date_range[0])
                    mask &= df['commencement_date'].astype(str) <= str(date_range[1])

    return mask

# --- NAVIGATION ---
page = st.sidebar.radio("Navigate", ["Upload & Process", "Database View", "Analytics", "Comp Finder"])

# Global filter indicator
active_filter_count = sum(1 for k, v in st.session_state.items()
                          if "filter_" in k and v is not None and v != [] and v != "" and v != ()
                          and not k.endswith("_radius"))
if active_filter_count > 0:
    st.sidebar.markdown(
        f'<div class="badge-filter" style="margin-top:0.5rem;">{active_filter_count} filter(s) active</div>',
        unsafe_allow_html=True
    )
    st.sidebar.button("Clear All Filters", on_click=reset_callback, use_container_width=True)

# =====================================================================
# PAGE 1: UPLOAD & PROCESS
# =====================================================================
if page == "Upload & Process":
    # Determine step states
    has_data = st.session_state.clean_df is not None
    geocoded = has_data and st.session_state.clean_df['latitude'].notna().any()

    step1_status = "done" if has_data else "active"
    step2_status = "done" if geocoded else ("active" if has_data else "pending")
    step3_status = "active" if geocoded else "pending"

    render_step(1, "Upload & Parse", step1_status)
    render_step(2, "Geocode Addresses", step2_status)
    render_step(3, "Preview & Save", step3_status)

    st.markdown("")
    uploaded_file = st.file_uploader("Upload Excel/CSV", type=['csv', 'xlsx', 'xls'])

    if uploaded_file:
        # Save to temp file
        suffix = os.path.splitext(uploaded_file.name)[1]
        tmp = tempfile.NamedTemporaryFile(delete=False, suffix=suffix)
        tmp.write(uploaded_file.getbuffer())
        tmp.close()
        path = tmp.name

        if st.session_state.current_filename != uploaded_file.name:
            with st.spinner('AI is analyzing columns...'):
                df_input = robust_load_file(path)
                if df_input is not None:
                    if len(df_input) >= 500:
                        st.warning(f"Large file detected ({len(df_input)} rows). Truncated to 500 rows for processing.")
                    result_df, conf = process_file_to_clean_output(df_input, uploaded_file.name)
                    st.session_state.clean_df = result_df
                    st.session_state.mapping_confidence = conf
                    st.session_state.current_filename = uploaded_file.name
                    st.session_state.geocoding_done = False
                    st.success("File parsed successfully!")
                else:
                    st.error("Could not read the file. Check the format.")

        if st.session_state.clean_df is not None:
            df = st.session_state.clean_df
            conf = st.session_state.mapping_confidence
            stype = df['source_type'].iloc[0]

            # --- MAPPING CONFIDENCE DISPLAY ---
            if conf:
                section_header("Column Mapping", "AI confidence scores for each mapped field")
                conf_df = pd.DataFrame([
                    {"Target Field": k, "Confidence": v, "Status": "Override" if v >= 1.0 else ("High" if v >= 0.60 else ("Medium" if v >= 0.45 else "Not Mapped"))}
                    for k, v in conf.items()
                ])
                conf_df = conf_df[conf_df['Confidence'] > 0].sort_values('Confidence', ascending=False)

                def highlight_confidence(row):
                    if row['Status'] == 'Not Mapped':
                        return ['background-color: #ffcccc'] * len(row)
                    elif row['Status'] == 'Medium':
                        return ['background-color: #fff3cd'] * len(row)
                    return [''] * len(row)

                st.dataframe(
                    conf_df.style.apply(highlight_confidence, axis=1),
                    use_container_width=True,
                    hide_index=True,
                )

            # --- AUTO GEOCODING ---
            section_header("Geocoding", "Standardizing addresses via Google Maps")
            missing_geos = df['latitude'].isna().sum()

            if missing_geos > 0 and not st.session_state.geocoding_done:
                api_key = get_secret("GOOGLE_API_KEY", "")
                if not api_key:
                    st.error("Google API Key not configured. Add GOOGLE_API_KEY to your secrets.")
                else:
                    progress_bar = st.progress(0)
                    status_text = st.empty()
                    status_text.text(f"Geocoding {missing_geos} addresses...")
                    results = []
                    warnings = []
                    for i, row in df.iterrows():
                        raw_addr = str(row.get('raw_address_data', '') or row.get('address', '') or '')
                        status_text.text(f"Geocoding {i+1}/{len(df)}: {raw_addr[:60]}...")
                        addr, lat, lng, city, zip_code, warn = fetch_google_data(raw_addr, api_key)
                        results.append((addr, lat, lng, city, zip_code))
                        if warn:
                            warnings.append(f"Row {i+1}: {warn}")
                        progress_bar.progress((i + 1) / len(df))
                    df['address'] = [x[0] for x in results]
                    df['latitude'] = [x[1] for x in results]
                    df['longitude'] = [x[2] for x in results]
                    df['city'] = [x[3] for x in results]
                    df['zip_code'] = [x[4] for x in results]
                    st.session_state.clean_df = df
                    st.session_state.geocoding_done = True
                    geocoded = sum(1 for r in results if r[1] is not None)
                    status_text.text(f"Done! Geocoded {geocoded}/{len(results)} addresses.")
                    if warnings:
                        with st.expander(f"Geocoding warnings ({len(warnings)})", expanded=True):
                            for w in warnings:
                                st.warning(w)
                            st.info("Tip: Check that addresses include street numbers and city names. Broad addresses like 'Texas' will produce inaccurate results.")
            else:
                st.success("All addresses have been geocoded!")

            # --- PREVIEW & SAVE ---
            section_header("Preview & Save", f"{len(df)} records ready -- review and save to database")

            cols_to_show = list(df.columns)
            hide_cols = ['source_type', 'source_file', 'rate_basis']
            if stype == "LEASE" and 'rate_monthly' in cols_to_show:
                priority = ['address', 'rate_monthly', 'rate_annually', 'rate_basis']
                cols_to_show = priority + [c for c in cols_to_show if c not in priority and c not in hide_cols]
            elif stype == "SALE":
                cols_to_show = [c for c in cols_to_show if c not in ['rate_monthly', 'rate_annually', 'rate_basis'] and c not in hide_cols]
            else:
                cols_to_show = [c for c in cols_to_show if c not in hide_cols]

            edited_df = st.data_editor(st.session_state.clean_df[cols_to_show], num_rows="dynamic")

            if st.button("Save to Database", type="primary", use_container_width=True):
                # Upload original file to Supabase Storage
                file_url = None
                try:
                    file_url = upload_to_storage(uploaded_file.getvalue(), uploaded_file.name)
                except Exception as e:
                    st.warning(f"Could not upload source file to storage: {e}")

                # Fetch existing records for duplicate detection
                model_cls = SaleComp if stype == "SALE" else LeaseComp
                session_dup = Session()
                existing_records = []
                try:
                    existing_records = [(r.id, r.address) for r in session_dup.query(model_cls.id, model_cls.address).all()]
                except Exception:
                    pass
                session_dup.close()

                session = Session()
                records = []
                skipped = 0
                skipped_details = []
                bar = st.progress(0)

                for i, row in edited_df.iterrows():
                    addr = clean_text_val(row.get('address'))

                    # Auto-skip duplicates
                    if addr and existing_records:
                        matches = find_duplicates(addr, existing_records)
                        if matches:
                            skipped += 1
                            skipped_details.append(f"Row {i+1}: {addr[:50]} (matched: {matches[0][1][:50]}, {matches[0][2]:.0%})")
                            bar.progress((i + 1) / len(edited_df))
                            continue

                    common = {
                        'address': addr,
                        'latitude': row.get('latitude'),
                        'longitude': row.get('longitude'),
                        'raw_address_data': clean_text_val(row.get('raw_address_data')),
                        'source_file': uploaded_file.name,
                        'source_file_url': file_url,
                        'city': clean_text_val(row.get('city')),
                        'zip_code': clean_text_val(row.get('zip_code')),
                        'notes': clean_text_val(row.get('notes')),
                    }

                    if stype == "SALE":
                        specific = {
                            'sale_price': clean_currency_num(row.get('sale_price')),
                            'building_size': clean_currency_num(row.get('building_size')),
                            'price_per_sf': clean_currency_num(row.get('price_per_sf')),
                            'closing_date': clean_text_val(row.get('closing_date')),
                            'year_built': clean_currency_num(row.get('year_built')),
                            'cap_rate': clean_currency_num(row.get('cap_rate')),
                            'buyer': clean_text_val(row.get('buyer')),
                            'seller': clean_text_val(row.get('seller')),
                        }
                    elif stype == "LEASE":
                        specific = {
                            'tenant_name': clean_text_val(row.get('tenant_name')),
                            'leased_sf': clean_currency_num(row.get('leased_sf')),
                            'rate_monthly': clean_currency_num(row.get('rate_monthly')),
                            'rate_annually': clean_currency_num(row.get('rate_annually')),
                            'term_months': clean_currency_num(row.get('term_months')),
                            'commencement_date': clean_text_val(row.get('commencement_date')),
                            'ti_allowance': clean_currency_num(row.get('ti_allowance')),
                            'free_rent': clean_text_val(row.get('free_rent')),
                            'lease_type': clean_text_val(row.get('lease_type')),
                            'escalations': clean_text_val(row.get('escalations')),
                            'building_type': clean_text_val(row.get('building_type')),
                            'clear_height': clean_currency_num(row.get('clear_height')),
                        }
                    else:
                        continue

                    records.append(model_cls(**{**common, **specific}))
                    bar.progress((i + 1) / len(edited_df))

                if records:
                    session.add_all(records)
                session.commit()
                session.close()
                load_data.clear()
                get_record_counts.clear()

                msg_parts = []
                if records:
                    msg_parts.append(f"Saved {len(records)} new records")
                if skipped:
                    msg_parts.append(f"skipped {skipped} duplicates")
                msg = " | ".join(msg_parts) if msg_parts else "No records to save."
                st.toast(msg, icon="\u2705")
                st.success(msg)
                if skipped_details:
                    with st.expander(f"View {skipped} skipped duplicates"):
                        for detail in skipped_details:
                            st.text(detail)

                # Cleanup
                try:
                    os.unlink(path)
                except Exception:
                    pass
                st.session_state.clean_df = None
                st.session_state.mapping_confidence = None

# =====================================================================
# PAGE 2: DATABASE VIEW
# =====================================================================
elif page == "Database View":
    section_header("Database Explorer")

    # Record counts for type selector
    sale_count, lease_count = get_record_counts()
    view_type = st.radio(
        "Select Data Type",
        [f"Sales Comps ({sale_count})", f"Lease Comps ({lease_count})"],
        horizontal=True
    )
    view_type = "Sales Comps" if "Sales" in view_type else "Lease Comps"

    df = load_data("SaleComp" if view_type == "Sales Comps" else "LeaseComp").copy()
    model_cls = SaleComp if view_type == "Sales Comps" else LeaseComp

    if df.empty:
        st.info("Database is empty. Upload files on the Upload page.")
    else:
        # --- SIDEBAR FILTERS ---
        st.sidebar.markdown("---")
        mask = apply_sidebar_filters(df, view_type, include_proximity=True)

        # Distance calculation for proximity search
        center_addr = st.session_state.get("filter_loc_center", "")
        radius = st.session_state.get("filter_loc_radius", 5)
        lat_c, lon_c = None, None
        if center_addr:
            with st.spinner("Calculating distances..."):
                _, lat_c, lon_c, _, _, _ = fetch_google_data(center_addr, get_secret("GOOGLE_API_KEY", ""))
                if lat_c:
                    df['distance_miles'] = df.apply(
                        lambda x: haversine_miles(lat_c, lon_c, x['latitude'], x['longitude']), axis=1
                    )
                    mask &= (df['distance_miles'] <= radius)
                else:
                    st.error("Could not find that address.")

        # --- SORTING ---
        sort_col1, sort_col2 = st.columns([2, 1])
        with sort_col1:
            sort_options = ['id'] + [c for c in df.columns if c not in ('id',)]
            sort_col = st.selectbox("Sort by", sort_options, index=0, key="sort_col")
        with sort_col2:
            sort_order = st.radio("Order", ["Ascending", "Descending"], horizontal=True, key="sort_order")

        # --- APPLY FILTERS ---
        df_filtered = df[mask].copy()
        df_filtered = df_filtered.sort_values(sort_col, ascending=(sort_order == "Ascending"))
        df_filtered.insert(0, "Select", False)

        st.markdown(
            f'<div class="record-count">Showing <b>{len(df_filtered)}</b> of {len(df)} records</div>',
            unsafe_allow_html=True
        )

        # Column ordering for leases
        if view_type == "Lease Comps":
            cols = list(df_filtered.columns)
            priority = ['Select', 'address', 'rate_monthly', 'rate_annually', 'leased_sf', 'tenant_name']
            cols = priority + [c for c in cols if c not in priority]
            df_filtered = df_filtered[cols]

        # Column config for formatting
        col_config = {"Select": st.column_config.CheckboxColumn(required=True)}
        if 'source_file_url' in df_filtered.columns:
            col_config["source_file_url"] = st.column_config.LinkColumn("Source File", display_text="View")
        if view_type == "Sales Comps":
            col_config["sale_price"] = st.column_config.NumberColumn("Sale Price", format="$%,.0f")
            col_config["price_per_sf"] = st.column_config.NumberColumn("$/SF", format="$%.2f")
            col_config["building_size"] = st.column_config.NumberColumn("Size (SF)", format="%,.0f")
            col_config["cap_rate"] = st.column_config.NumberColumn("Cap Rate", format="%.2f%%")
        else:
            col_config["rate_monthly"] = st.column_config.NumberColumn("$/SF/Mo", format="$%.2f")
            col_config["rate_annually"] = st.column_config.NumberColumn("$/SF/Yr", format="$%.2f")
            col_config["leased_sf"] = st.column_config.NumberColumn("Leased SF", format="%,.0f")
            col_config["ti_allowance"] = st.column_config.NumberColumn("TI", format="$%.2f")

        # ---- TABS: Data Table | Map | Export & Actions ----
        tab_table, tab_map, tab_export = st.tabs(["Data Table", "Map View", "Export & Actions"])

        with tab_table:
            # Pagination
            PAGE_SIZE = 100
            total_pages = max(1, math.ceil(len(df_filtered) / PAGE_SIZE))
            if 'page_num' not in st.session_state:
                st.session_state.page_num = 1
            st.session_state.page_num = min(st.session_state.page_num, total_pages)

            if total_pages > 1:
                col1, col2, col3 = st.columns([1, 2, 1])
                with col1:
                    if st.button("Previous", use_container_width=True) and st.session_state.page_num > 1:
                        st.session_state.page_num -= 1
                with col2:
                    st.markdown(f"<div style='text-align:center; padding:0.4rem;'><b>Page {st.session_state.page_num} of {total_pages}</b></div>", unsafe_allow_html=True)
                with col3:
                    if st.button("Next", use_container_width=True) and st.session_state.page_num < total_pages:
                        st.session_state.page_num += 1

            start_idx = (st.session_state.page_num - 1) * PAGE_SIZE
            df_page = df_filtered.iloc[start_idx:start_idx + PAGE_SIZE].copy()

            # Selection controls — searchable multiselect + buttons
            row_labels = df_page.apply(
                lambda r: f"{int(r['id'])}: {str(r.get('address', 'N/A'))[:60]}", axis=1
            ).tolist()

            sel_col1, sel_col2 = st.columns([3, 1])
            with sel_col1:
                selected_labels = st.multiselect(
                    "Select rows (type to search)",
                    row_labels,
                    default=st.session_state.get('_selected_labels', []),
                    key="row_selector",
                    placeholder="Search by address or ID..."
                )
                st.session_state['_selected_labels'] = selected_labels
            with sel_col2:
                if st.button("Select All", use_container_width=True):
                    st.session_state['_selected_labels'] = row_labels
                    st.rerun()
                if st.button("Clear", use_container_width=True):
                    st.session_state['_selected_labels'] = []
                    st.rerun()

            # Sync multiselect to the Select column
            selected_ids = set()
            for lbl in selected_labels:
                try:
                    selected_ids.add(int(lbl.split(":")[0]))
                except (ValueError, IndexError):
                    pass
            df_page["Select"] = df_page["id"].astype(int).isin(selected_ids)

            edited_view = st.data_editor(
                df_page,
                hide_index=True,
                column_config=col_config,
                use_container_width=True,
            )

            # Save edits
            if st.button("Save Changes to Database", use_container_width=True):
                session = Session()
                save_count = 0
                for _, row in edited_view.iterrows():
                    if 'id' not in row or pd.isna(row['id']):
                        continue
                    record_id = int(row['id'])
                    update_dict = {}
                    skip_cols = {'Select', 'id', 'distance_miles', 'created_at'}
                    for col in edited_view.columns:
                        if col in skip_cols:
                            continue
                        val = row[col]
                        if pd.isna(val):
                            val = None
                        update_dict[col] = val
                    session.query(model_cls).filter_by(id=record_id).update(update_dict)
                    save_count += 1
                session.commit()
                session.close()
                load_data.clear()
                get_record_counts.clear()
                st.toast(f"Saved changes to {save_count} records", icon="\u2705")
                st.rerun()

        with tab_map:
            from folium.plugins import MarkerCluster
            map_df = df_filtered[df_filtered['latitude'].notnull() & df_filtered['longitude'].notnull()]
            if not map_df.empty:
                center_lat = map_df['latitude'].mean()
                center_lon = map_df['longitude'].mean()
                m = folium.Map(location=[center_lat, center_lon], zoom_start=11)
                cluster = MarkerCluster().add_to(m)

                for _, row in map_df.iterrows():
                    color = 'orange' if view_type == "Sales Comps" else 'darkred'
                    if view_type == "Sales Comps":
                        popup_text = f"<b>{row.get('address', 'N/A')}</b><br>Price: ${row.get('sale_price', 0):,.0f}<br>Size: {row.get('building_size', 0):,.0f} SF"
                    else:
                        popup_text = f"<b>{row.get('address', 'N/A')}</b><br>Rate: ${row.get('rate_monthly', 0):.2f}/SF/Mo<br>Size: {row.get('leased_sf', 0):,.0f} SF"

                    folium.Marker(
                        location=[row['latitude'], row['longitude']],
                        popup=folium.Popup(popup_text, max_width=300),
                        icon=folium.Icon(color=color, icon='home', prefix='fa'),
                    ).add_to(cluster)

                # Draw radius circle if searching
                if center_addr and lat_c and lon_c:
                    folium.Circle(
                        location=[lat_c, lon_c],
                        radius=radius * 1609.34,
                        color='#F5A623',
                        fill=True,
                        fill_opacity=0.1,
                    ).add_to(m)

                st_folium(m, height=600, use_container_width=True)
            else:
                st.info("No geocoded properties to display on map.")

        with tab_export:
            selected_rows = edited_view[edited_view["Select"] == True]

            if not selected_rows.empty:
                section_header("Export", f"{len(selected_rows)} properties selected")
                export_df = selected_rows
            else:
                section_header("Export", f"All {len(df_filtered)} filtered properties")
                export_df = df_filtered

            st.dataframe(
                export_df.drop(columns=['Select'], errors='ignore'),
                column_config=col_config,
                use_container_width=True,
                hide_index=True,
            )

            exp1, exp2, exp3 = st.columns(3)
            with exp1:
                st.download_button(
                    "KML", generate_kml(export_df.drop(columns=['Select'], errors='ignore')),
                    "comps.kml", "application/vnd.google-earth.kml+xml",
                    use_container_width=True,
                )
            with exp2:
                st.download_button(
                    "Excel", to_excel_bytes(export_df.drop(columns=['Select'], errors='ignore')),
                    "comps.xlsx", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    use_container_width=True,
                )
            with exp3:
                st.download_button(
                    "CSV", export_df.drop(columns=['Select'], errors='ignore').to_csv(index=False),
                    "comps.csv", "text/csv",
                    use_container_width=True,
                )

            # Admin actions
            if user_role == "admin":
                st.markdown("")
                section_header("Admin Actions")

                if not selected_rows.empty and 'id' in selected_rows.columns:
                    confirm_sel = st.checkbox(f"I confirm deletion of {len(selected_rows)} selected records", key="confirm_delete_selected")
                    if confirm_sel and st.button(f"Delete {len(selected_rows)} Selected Records", type="secondary", use_container_width=True):
                        session = Session()
                        ids_to_delete = selected_rows['id'].dropna().astype(int).tolist()
                        session.query(model_cls).filter(model_cls.id.in_(ids_to_delete)).delete(synchronize_session=False)
                        session.commit()
                        session.close()
                        load_data.clear()
                        get_record_counts.clear()
                        st.success(f"Deleted {len(ids_to_delete)} records.")
                        st.rerun()

                confirm_delete = st.checkbox("I want to delete ALL data", key="confirm_delete")
                if confirm_delete:
                    if st.button("Confirm: Clear All Data", type="secondary"):
                        session = Session()
                        session.query(SaleComp).delete()
                        session.query(LeaseComp).delete()
                        session.commit()
                        session.close()
                        load_data.clear()
                        get_record_counts.clear()
                        st.rerun()

# =====================================================================
# PAGE 3: ANALYTICS
# =====================================================================
elif page == "Analytics":
    section_header("Analytics Dashboard")

    a_sale_count, a_lease_count = get_record_counts()

    # Type selector with counts
    analytics_type = st.radio(
        "Analyze",
        [f"Sales Comps ({a_sale_count})", f"Lease Comps ({a_lease_count})"],
        horizontal=True, key="analytics_type"
    )
    analytics_type = "Sales Comps" if "Sales" in analytics_type else "Lease Comps"

    # Only load the selected type
    if analytics_type == "Sales Comps":
        sales_df = load_data("SaleComp").copy()
        leases_df = pd.DataFrame()
    else:
        leases_df = load_data("LeaseComp").copy()
        sales_df = pd.DataFrame()

    # Sidebar filters
    st.sidebar.markdown("---")

    if analytics_type == "Sales Comps" and not sales_df.empty:
        analytics_mask = apply_sidebar_filters(sales_df, "Sales Comps")
        filtered_sales = sales_df[analytics_mask]
        filtered_leases = pd.DataFrame()
    elif analytics_type == "Lease Comps" and not leases_df.empty:
        analytics_mask = apply_sidebar_filters(leases_df, "Lease Comps")
        filtered_leases = leases_df[analytics_mask]
        filtered_sales = pd.DataFrame()
    else:
        filtered_sales = sales_df
        filtered_leases = leases_df

    # --- SUMMARY METRICS ---
    section_header("Portfolio Overview")
    if analytics_type == "Sales Comps":
        df_a = filtered_sales
        r1c1, r1c2 = st.columns(2)
        r2c1, r2c2 = st.columns(2)
        with r1c1:
            render_metric_card("Total Comps", f"{len(df_a):,}")
        if not df_a.empty:
            avg_price = df_a['sale_price'].dropna().mean()
            avg_psf = df_a['price_per_sf'].dropna().mean()
            avg_size = df_a['building_size'].dropna().mean()
            with r1c2:
                render_metric_card("Avg Sale Price", f"${avg_price:,.0f}" if pd.notna(avg_price) else "N/A")
            with r2c1:
                render_metric_card("Avg $/SF", f"${avg_psf:,.2f}" if pd.notna(avg_psf) else "N/A")
            with r2c2:
                render_metric_card("Avg Size (SF)", f"{avg_size:,.0f}" if pd.notna(avg_size) else "N/A")
    else:
        df_a = filtered_leases
        r1c1, r1c2 = st.columns(2)
        r2c1, r2c2 = st.columns(2)
        with r1c1:
            render_metric_card("Total Comps", f"{len(df_a):,}")
        if not df_a.empty:
            avg_monthly = df_a['rate_monthly'].dropna().mean()
            avg_annual = df_a['rate_annually'].dropna().mean()
            avg_sf = df_a['leased_sf'].dropna().mean()
            with r1c2:
                render_metric_card("Avg $/SF/Mo", f"${avg_monthly:.2f}" if pd.notna(avg_monthly) else "N/A")
            with r2c1:
                render_metric_card("Avg $/SF/Yr", f"${avg_annual:.2f}" if pd.notna(avg_annual) else "N/A")
            with r2c2:
                render_metric_card("Avg Leased SF", f"{avg_sf:,.0f}" if pd.notna(avg_sf) else "N/A")

    st.markdown("")

    # --- CHARTS ---
    if analytics_type == "Sales Comps":
        tab1, tab2, tab3, tab4 = st.tabs(["Distributions", "Price vs Size", "Trends", "By Zip Code"])

        with tab1:
            if not filtered_sales.empty:
                col1, col2 = st.columns(2)
                with col1:
                    price_data = filtered_sales['sale_price'].dropna()
                    if not price_data.empty:
                        fig = px.histogram(price_data, nbins=20, title="Sale Price Distribution",
                                           labels={'value': 'Sale Price ($)', 'count': 'Count'},
                                           color_discrete_sequence=HC_COLORS)
                        fig.update_layout(showlegend=False)
                        st.plotly_chart(fig, use_container_width=True)
                with col2:
                    psf_data = filtered_sales['price_per_sf'].dropna()
                    if not psf_data.empty:
                        fig = px.histogram(psf_data, nbins=20, title="$/SF Distribution",
                                           labels={'value': '$/SF', 'count': 'Count'},
                                           color_discrete_sequence=HC_COLORS)
                        fig.update_layout(showlegend=False)
                        st.plotly_chart(fig, use_container_width=True)
                size_data = filtered_sales['building_size'].dropna()
                if not size_data.empty:
                    fig = px.histogram(size_data, nbins=20, title="Building Size Distribution",
                                       labels={'value': 'Building Size (SF)', 'count': 'Count'},
                                       color_discrete_sequence=HC_COLORS)
                    fig.update_layout(showlegend=False)
                    st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("No sales data matching filters.")

        with tab2:
            scatter_data = filtered_sales.dropna(subset=['building_size', 'sale_price'])
            if not scatter_data.empty:
                fig = px.scatter(scatter_data, x='building_size', y='sale_price',
                                hover_data=['address'], trendline='ols',
                                title="Sale Price vs Building Size",
                                labels={'building_size': 'Building Size (SF)', 'sale_price': 'Sale Price ($)'},
                                color_discrete_sequence=HC_COLORS)
                st.plotly_chart(fig, use_container_width=True)
                st.caption(f"n = {len(scatter_data)} properties | OLS trendline")
            else:
                st.info("Not enough data for scatter plot.")

        with tab3:
            ts_data = filtered_sales.dropna(subset=['closing_date', 'price_per_sf']).copy()
            if not ts_data.empty:
                ts_data['closing_date'] = pd.to_datetime(ts_data['closing_date'], errors='coerce')
                ts_data = ts_data.dropna(subset=['closing_date'])
                if not ts_data.empty:
                    fig = px.scatter(ts_data.sort_values('closing_date'), x='closing_date', y='price_per_sf',
                                    hover_data=['address'], trendline='lowess',
                                    title="$/SF Over Time",
                                    labels={'closing_date': 'Closing Date', 'price_per_sf': '$/SF'},
                                    color_discrete_sequence=HC_COLORS)
                    st.plotly_chart(fig, use_container_width=True)
                    st.caption(f"n = {len(ts_data)} properties | LOWESS trendline")
                else:
                    st.info("No valid date data for trend analysis.")
            else:
                st.info("Not enough data for trend analysis.")

        with tab4:
            from utils import extract_zip_from_address
            zip_df = filtered_sales.copy()
            if 'zip_code' not in zip_df.columns or zip_df['zip_code'].isna().all():
                zip_df['zip_code'] = zip_df['address'].apply(extract_zip_from_address)
            zip_df = zip_df.dropna(subset=['zip_code'])
            if not zip_df.empty:
                zip_stats = zip_df.groupby('zip_code').agg(
                    count=('sale_price', 'count'),
                    avg_price=('sale_price', 'mean'),
                    avg_psf=('price_per_sf', 'mean'),
                    avg_size=('building_size', 'mean'),
                ).round(2).reset_index()
                zip_stats.columns = ['Zip Code', 'Count', 'Avg Price', 'Avg $/SF', 'Avg Size (SF)']
                st.dataframe(zip_stats, hide_index=True, use_container_width=True)

                fig = px.bar(zip_stats, x='Zip Code', y='Avg $/SF', color='Count',
                            title="Average $/SF by Zip Code",
                            color_continuous_scale=HC_SCALE)
                st.plotly_chart(fig, use_container_width=True)

                zips = st.multiselect("Compare Zip Codes (2-5)", zip_stats['Zip Code'].tolist(),
                                      max_selections=5, key="zip_compare_sale")
                if len(zips) >= 2:
                    compare = zip_stats[zip_stats['Zip Code'].isin(zips)]
                    st.dataframe(compare, hide_index=True, use_container_width=True)
            else:
                st.info("No zip code data available.")

    else:  # Lease Comps analytics
        tab1, tab2, tab3, tab4 = st.tabs(["Distributions", "Rate vs Size", "Trends", "By Zip Code"])

        with tab1:
            if not filtered_leases.empty:
                col1, col2 = st.columns(2)
                with col1:
                    rate_data = filtered_leases['rate_monthly'].dropna()
                    if not rate_data.empty:
                        fig = px.histogram(rate_data, nbins=20, title="Monthly Rate Distribution ($/SF/Mo)",
                                           labels={'value': '$/SF/Mo', 'count': 'Count'},
                                           color_discrete_sequence=HC_COLORS)
                        fig.update_layout(showlegend=False)
                        st.plotly_chart(fig, use_container_width=True)
                with col2:
                    sf_data = filtered_leases['leased_sf'].dropna()
                    if not sf_data.empty:
                        fig = px.histogram(sf_data, nbins=20, title="Leased SF Distribution",
                                           labels={'value': 'Leased SF', 'count': 'Count'},
                                           color_discrete_sequence=HC_COLORS)
                        fig.update_layout(showlegend=False)
                        st.plotly_chart(fig, use_container_width=True)
                ti_data = filtered_leases['ti_allowance'].dropna()
                if not ti_data.empty:
                    fig = px.histogram(ti_data, nbins=15, title="TI Allowance Distribution",
                                       labels={'value': 'TI Allowance ($)', 'count': 'Count'},
                                       color_discrete_sequence=HC_COLORS)
                    fig.update_layout(showlegend=False)
                    st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("No lease data matching filters.")

        with tab2:
            scatter_data = filtered_leases.dropna(subset=['leased_sf', 'rate_monthly'])
            if not scatter_data.empty:
                fig = px.scatter(scatter_data, x='leased_sf', y='rate_monthly',
                                hover_data=['address', 'tenant_name'], trendline='ols',
                                title="Monthly Rate vs Leased SF",
                                labels={'leased_sf': 'Leased SF', 'rate_monthly': '$/SF/Mo'},
                                color_discrete_sequence=HC_COLORS)
                st.plotly_chart(fig, use_container_width=True)
                st.caption(f"n = {len(scatter_data)} properties | OLS trendline")
            else:
                st.info("Not enough data for scatter plot.")

        with tab3:
            ts_data = filtered_leases.dropna(subset=['commencement_date', 'rate_monthly']).copy()
            if not ts_data.empty:
                ts_data['commencement_date'] = pd.to_datetime(ts_data['commencement_date'], errors='coerce')
                ts_data = ts_data.dropna(subset=['commencement_date'])
                if not ts_data.empty:
                    fig = px.scatter(ts_data.sort_values('commencement_date'), x='commencement_date', y='rate_monthly',
                                    hover_data=['address', 'tenant_name'], trendline='lowess',
                                    title="Monthly Rate Over Time",
                                    labels={'commencement_date': 'Commencement Date', 'rate_monthly': '$/SF/Mo'},
                                    color_discrete_sequence=HC_COLORS)
                    st.plotly_chart(fig, use_container_width=True)
                    st.caption(f"n = {len(ts_data)} properties | LOWESS trendline")
                else:
                    st.info("No valid date data for trend analysis.")
            else:
                st.info("Not enough data for trend analysis.")

        with tab4:
            from utils import extract_zip_from_address
            zip_df = filtered_leases.copy()
            if 'zip_code' not in zip_df.columns or zip_df['zip_code'].isna().all():
                zip_df['zip_code'] = zip_df['address'].apply(extract_zip_from_address)
            zip_df = zip_df.dropna(subset=['zip_code'])
            if not zip_df.empty:
                zip_stats = zip_df.groupby('zip_code').agg(
                    count=('rate_monthly', 'count'),
                    avg_monthly=('rate_monthly', 'mean'),
                    avg_annual=('rate_annually', 'mean'),
                    avg_sf=('leased_sf', 'mean'),
                ).round(2).reset_index()
                zip_stats.columns = ['Zip Code', 'Count', 'Avg $/Mo', 'Avg $/Yr', 'Avg Leased SF']
                st.dataframe(zip_stats, hide_index=True, use_container_width=True)

                fig = px.bar(zip_stats, x='Zip Code', y='Avg $/Mo', color='Count',
                            title="Average $/SF/Mo by Zip Code",
                            color_continuous_scale=HC_SCALE)
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("No zip code data available.")

    # --- HEAT MAP ---
    section_header("Geographic Heat Map")
    heat_df = df_a if not df_a.empty else pd.DataFrame()
    if not heat_df.empty:
        geo_data = heat_df.dropna(subset=['latitude', 'longitude'])
        if analytics_type == "Sales Comps":
            value_col = 'price_per_sf'
        else:
            value_col = 'rate_monthly'
        geo_data = geo_data.dropna(subset=[value_col])
        if not geo_data.empty:
            fig = px.density_mapbox(geo_data, lat='latitude', lon='longitude', z=value_col,
                                    radius=20, zoom=9, mapbox_style='open-street-map',
                                    hover_data=['address'],
                                    title=f"{value_col.replace('_', ' ').title()} Heat Map")
            fig.update_layout(height=500)
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("Not enough geocoded data for heat map.")
    else:
        st.info("No data for heat map.")

    # --- PROPERTY COMPARISON ---
    section_header("Property Comparison")

    if analytics_type == "Sales Comps" and not sales_df.empty:
        options = sales_df.apply(
            lambda r: f"{r['id']}: {r.get('address', 'N/A')} - ${r.get('sale_price', 0):,.0f}", axis=1
        ).tolist()
        selected = st.multiselect("Select properties to compare (2-5)", options, max_selections=5)
        if len(selected) >= 2:
            ids = [int(s.split(":")[0]) for s in selected]
            compare_raw = sales_df[sales_df['id'].isin(ids)].copy()
            display_fields = ['address', 'sale_price', 'price_per_sf', 'building_size',
                             'year_built', 'cap_rate', 'closing_date', 'buyer', 'seller', 'city', 'zip_code']
            available = [f for f in display_fields if f in compare_raw.columns]
            compare_df = compare_raw[available].set_index('address').T
            compare_df.index = compare_df.index.map(lambda x: x.replace('_', ' ').title())
            st.dataframe(compare_df, use_container_width=True)
    elif analytics_type == "Lease Comps" and not leases_df.empty:
        options = leases_df.apply(
            lambda r: f"{r['id']}: {r.get('address', 'N/A')} - {r.get('tenant_name', 'N/A')}", axis=1
        ).tolist()
        selected = st.multiselect("Select properties to compare (2-5)", options, max_selections=5)
        if len(selected) >= 2:
            ids = [int(s.split(":")[0]) for s in selected]
            compare_raw = leases_df[leases_df['id'].isin(ids)].copy()
            display_fields = ['address', 'rate_monthly', 'rate_annually', 'leased_sf',
                             'tenant_name', 'term_months', 'ti_allowance', 'lease_type',
                             'building_type', 'commencement_date', 'city', 'zip_code']
            available = [f for f in display_fields if f in compare_raw.columns]
            compare_df = compare_raw[available].set_index('address').T
            compare_df.index = compare_df.index.map(lambda x: x.replace('_', ' ').title())
            st.dataframe(compare_df, use_container_width=True)
    else:
        st.info("Add data to use the comparison tool.")

# =====================================================================
# PAGE 4: COMP FINDER
# =====================================================================
elif page == "Comp Finder":
    section_header("Comp Finder", "Input subject property details to find comparable properties")

    # Session state for results persistence
    if 'cf_results' not in st.session_state:
        st.session_state.cf_results = None
    if 'cf_subject' not in st.session_state:
        st.session_state.cf_subject = None
    if 'cf_subject_coords' not in st.session_state:
        st.session_state.cf_subject_coords = None

    # --- Comp type selector ---
    cf_type = st.radio("Search in", ["Sales Comps", "Leases Comps"], horizontal=True, key="cf_type_radio")
    cf_type_key = "Sales" if "Sales" in cf_type else "Leases"

    # --- Sidebar: Comp Finder Settings ---
    st.sidebar.markdown("---")
    st.sidebar.markdown("**Comp Finder Settings**")
    w_proximity = st.sidebar.slider("Proximity Weight", 0.0, 1.0, 0.30, 0.05, key="cf_w_prox")
    w_size = st.sidebar.slider("Size Weight", 0.0, 1.0, 0.25, 0.05, key="cf_w_size")
    w_price = st.sidebar.slider("Price / Rate Weight", 0.0, 1.0, 0.20, 0.05, key="cf_w_price")
    w_recency = st.sidebar.slider("Recency Weight", 0.0, 1.0, 0.15, 0.05, key="cf_w_recency")
    w_other = st.sidebar.slider("Other Attributes Weight", 0.0, 1.0, 0.10, 0.05, key="cf_w_other")
    max_radius = st.sidebar.slider("Max Radius (miles)", 1, 50, 25, key="cf_max_radius")
    max_results = st.sidebar.slider("Max Results", 5, 50, 20, key="cf_max_results")
    use_ai = st.sidebar.checkbox("AI Enhancement", value=False, key="cf_use_ai")
    ai_blend = 0.3
    if use_ai:
        ai_blend = st.sidebar.slider("AI Blend Ratio", 0.1, 0.9, 0.3, 0.05, key="cf_ai_blend")

    # --- Subject property form ---
    st.markdown("")
    col_left, col_right = st.columns(2)

    with col_left:
        st.markdown("**Required**")
        cf_address = st.text_input("Subject Address", placeholder="e.g. 123 Main St, Houston TX", key="cf_address")
        geo_col1, geo_col2 = st.columns([1, 2])
        with geo_col1:
            geocode_btn = st.button("Geocode", key="cf_geocode_btn")
        with geo_col2:
            if st.session_state.cf_subject_coords:
                lat, lng = st.session_state.cf_subject_coords
                st.success(f"{lat:.4f}, {lng:.4f}")

        if geocode_btn and cf_address:
            api_key = get_secret("GOOGLE_API_KEY", "")
            if api_key:
                with st.spinner("Geocoding..."):
                    addr, lat, lng, city, zip_code, warn = fetch_google_data(cf_address, api_key)
                    if lat and lng:
                        st.session_state.cf_subject_coords = (lat, lng)
                        st.toast(f"Geocoded: {addr}", icon="\u2705")
                        st.rerun()
                    else:
                        st.error("Could not geocode this address. Try a more specific address.")
            else:
                st.error("Google API Key not configured.")

        if cf_type_key == "Sales":
            cf_size = st.number_input("Building Size (SF)", value=None, min_value=0, step=100, key="cf_size")
        else:
            cf_size = st.number_input("Leased SF", value=None, min_value=0, step=100, key="cf_size")

    with col_right:
        st.markdown("**Optional**")
        if cf_type_key == "Sales":
            cf_price = st.number_input("Sale Price ($)", value=None, min_value=0, step=10000, key="cf_price")
            cf_psf = st.number_input("Price per SF ($)", value=None, min_value=0.0, step=1.0, key="cf_psf")
            cf_year = st.number_input("Year Built", value=None, min_value=1900, max_value=2030, step=1, key="cf_year")
        else:
            cf_rate_mo = st.number_input("Rate $/SF/Mo", value=None, min_value=0.0, step=0.25, key="cf_rate_mo")
            cf_rate_yr = st.number_input("Rate $/SF/Yr", value=None, min_value=0.0, step=1.0, key="cf_rate_yr")
            cf_btype = st.text_input("Building Type", placeholder="e.g. Industrial, Office", key="cf_btype")
        cf_city = st.text_input("City", placeholder="e.g. Houston", key="cf_city")
        cf_zip = st.text_input("Zip Code", placeholder="e.g. 77001", key="cf_zip")

    # --- Build subject dict ---
    subject = {}
    if st.session_state.cf_subject_coords:
        subject["lat"] = st.session_state.cf_subject_coords[0]
        subject["lng"] = st.session_state.cf_subject_coords[1]
    subject["address"] = cf_address or None
    subject["city"] = cf_city or None
    subject["zip_code"] = cf_zip or None

    if cf_type_key == "Sales":
        subject["building_size"] = cf_size
        subject["sale_price"] = cf_price
        subject["price_per_sf"] = cf_psf
        subject["year_built"] = cf_year
    else:
        subject["leased_sf"] = cf_size
        subject["rate_monthly"] = cf_rate_mo
        subject["rate_annually"] = cf_rate_yr
        subject["building_type"] = cf_btype or None

    # --- Weights dict ---
    if cf_type_key == "Sales":
        weights = {
            "proximity": w_proximity,
            "size": w_size,
            "price": w_price,
            "price_psf": w_other,
            "year_built": w_other,
            "recency": w_recency,
        }
    else:
        weights = {
            "proximity": w_proximity,
            "size": w_size,
            "rate_monthly": w_price,
            "rate_annually": w_other,
            "building_type": w_other,
            "recency": w_recency,
        }

    # --- Find Comps button ---
    st.markdown("")
    can_search = st.session_state.cf_subject_coords is not None
    if not can_search:
        st.warning("Geocode the subject address first to enable search.")

    if st.button("Find Comparable Properties", type="primary", use_container_width=True, disabled=not can_search):
        comps_df = load_comps(cf_type_key)
        if comps_df.empty:
            st.info(f"No {cf_type_key.lower()} comps in database yet. Upload comps first.")
        else:
            with st.spinner("Scoring comparables..."):
                results = compute_match_scores(subject, comps_df, cf_type_key, weights, max_radius)

                # AI enhancement
                if use_ai:
                    try:
                        ai_scores = compute_ai_scores(subject, results, cf_type_key)
                        results["match_score"] = blend_scores(results["match_score"], ai_scores, ai_blend)
                        results = results.sort_values("match_score", ascending=False).reset_index(drop=True)
                    except Exception as e:
                        st.toast(f"AI scoring failed, using weighted scores only: {e}", icon="\u26a0\ufe0f")

                # Filter to non-zero scores and limit results
                results = results[results["match_score"] > 0].head(max_results)

                st.session_state.cf_results = results
                st.session_state.cf_subject = subject

    # --- Display Results ---
    if st.session_state.cf_results is not None and not st.session_state.cf_results.empty:
        results = st.session_state.cf_results
        subject = st.session_state.cf_subject

        st.markdown("")
        section_header("Results", f"{len(results)} comparable properties found")

        tab_ranked, tab_map, tab_breakdown = st.tabs(["Ranked Results", "Map", "Score Breakdown"])

        with tab_ranked:
            display_df = results.copy()
            display_df.insert(0, "Rank", range(1, len(display_df) + 1))
            display_df["match_score"] = (display_df["match_score"] * 100).round(1)

            if cf_type_key == "Sales":
                show_cols = ["Rank", "address", "match_score", "distance_miles",
                             "sale_price", "price_per_sf", "building_size", "year_built",
                             "closing_date", "city", "zip_code"]
                col_cfg = {
                    "match_score": st.column_config.ProgressColumn("Match %", min_value=0, max_value=100, format="%.1f%%"),
                    "distance_miles": st.column_config.NumberColumn("Distance (mi)", format="%.1f"),
                    "sale_price": st.column_config.NumberColumn("Sale Price", format="$%,.0f"),
                    "price_per_sf": st.column_config.NumberColumn("$/SF", format="$%.2f"),
                    "building_size": st.column_config.NumberColumn("Size (SF)", format="%,.0f"),
                }
            else:
                show_cols = ["Rank", "address", "match_score", "distance_miles",
                             "rate_monthly", "rate_annually", "leased_sf", "tenant_name",
                             "building_type", "commencement_date", "city", "zip_code"]
                col_cfg = {
                    "match_score": st.column_config.ProgressColumn("Match %", min_value=0, max_value=100, format="%.1f%%"),
                    "distance_miles": st.column_config.NumberColumn("Distance (mi)", format="%.1f"),
                    "rate_monthly": st.column_config.NumberColumn("$/SF/Mo", format="$%.2f"),
                    "rate_annually": st.column_config.NumberColumn("$/SF/Yr", format="$%.2f"),
                    "leased_sf": st.column_config.NumberColumn("Leased SF", format="%,.0f"),
                }

            available_cols = [c for c in show_cols if c in display_df.columns]
            st.dataframe(
                display_df[available_cols],
                column_config=col_cfg,
                use_container_width=True,
                hide_index=True,
            )

            # Export results
            csv_data = display_df[available_cols].to_csv(index=False)
            st.download_button("Download Results (CSV)", csv_data, "comp_finder_results.csv", "text/csv",
                               use_container_width=True)

        with tab_map:
            map_results = results.dropna(subset=["latitude", "longitude"])
            if not map_results.empty and subject.get("lat") and subject.get("lng"):
                m = folium.Map(location=[subject["lat"], subject["lng"]], zoom_start=11)

                # Subject marker
                folium.Marker(
                    location=[subject["lat"], subject["lng"]],
                    popup=folium.Popup(f"<b>Subject Property</b><br>{subject.get('address', 'N/A')}", max_width=300),
                    icon=folium.Icon(color="red", icon="star", prefix="fa"),
                ).add_to(m)

                # Comp markers color-coded by score
                for _, row in map_results.iterrows():
                    score = row["match_score"]
                    if score >= 0.7:
                        color = "green"
                    elif score >= 0.4:
                        color = "orange"
                    else:
                        color = "lightred"

                    if cf_type_key == "Sales":
                        popup_html = f"<b>{row.get('address', 'N/A')}</b><br>Match: {score:.0%}<br>Price: ${row.get('sale_price', 0):,.0f}<br>Size: {row.get('building_size', 0):,.0f} SF<br>Distance: {row.get('distance_miles', 0):.1f} mi"
                    else:
                        popup_html = f"<b>{row.get('address', 'N/A')}</b><br>Match: {score:.0%}<br>Rate: ${row.get('rate_monthly', 0):.2f}/SF/Mo<br>Size: {row.get('leased_sf', 0):,.0f} SF<br>Distance: {row.get('distance_miles', 0):.1f} mi"

                    folium.Marker(
                        location=[row["latitude"], row["longitude"]],
                        popup=folium.Popup(popup_html, max_width=300),
                        icon=folium.Icon(color=color, icon="building", prefix="fa"),
                    ).add_to(m)

                    # Line from subject to comp
                    folium.PolyLine(
                        locations=[[subject["lat"], subject["lng"]], [row["latitude"], row["longitude"]]],
                        color="#F5A623",
                        weight=1.5,
                        opacity=0.4,
                    ).add_to(m)

                # Radius circle
                folium.Circle(
                    location=[subject["lat"], subject["lng"]],
                    radius=max_radius * 1609.34,
                    color="#F5A623",
                    fill=True,
                    fill_opacity=0.05,
                ).add_to(m)

                st_folium(m, height=600, use_container_width=True)
            else:
                st.info("No geocoded results to display on map.")

        with tab_breakdown:
            score_suffix = "_score"
            score_columns = [c for c in results.columns if c.endswith(score_suffix) and c != "match_score"]
            if score_columns:
                breakdown_df = results[["address"] + score_columns].head(min(10, len(results))).copy()
                # Rename columns for display
                breakdown_df.columns = ["Address"] + [c.replace("_score", "").replace("_", " ").title() for c in score_columns]

                melted = breakdown_df.melt(id_vars=["Address"], var_name="Category", value_name="Score")
                melted["Score"] = melted["Score"].fillna(0)
                # Truncate long addresses
                melted["Address"] = melted["Address"].apply(lambda x: str(x)[:40] if x else "N/A")

                fig = px.bar(
                    melted, y="Address", x="Score", color="Category",
                    orientation="h", barmode="group",
                    title="Score Breakdown by Category",
                    color_discrete_sequence=HC_COLORS,
                )
                fig.update_layout(
                    xaxis_title="Score (0 = no match, 1 = perfect)",
                    yaxis_title="",
                    height=max(400, len(breakdown_df) * 60),
                    legend=dict(orientation="h", y=-0.15),
                )
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("No score breakdown available.")

    elif st.session_state.cf_results is not None and st.session_state.cf_results.empty:
        st.info("No comparable properties found within the specified radius. Try increasing the max radius or adjusting weights.")
