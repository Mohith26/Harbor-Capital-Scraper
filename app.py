import streamlit as st
import pandas as pd
import os
import io
import math
import tempfile
import yaml
import streamlit_authenticator as stauth
import folium
from streamlit_folium import st_folium
import plotly.express as px
import plotly.io as pio
from database import Session, SaleComp, LeaseComp, engine
from comp_engine import robust_load_file, process_file_to_clean_output, fetch_google_data
from storage import upload_file as upload_to_storage
from utils import normalize_address, find_duplicates

# --- SECRETS ---
def get_secret(key, default=None):
    try:
        return st.secrets[key]
    except Exception:
        return os.environ.get(key, default)

GOOGLE_API_KEY = get_secret("GOOGLE_API_KEY", "")

# --- HELPERS ---
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

def clean_currency_num(value):
    if pd.isna(value) or value == "":
        return None
    s = str(value).strip().replace(',', '').replace('$', '').replace('%', '').lower().replace('sf', '')
    try:
        return float(s)
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
st.set_page_config(page_title="Harbor Capital Comp Database", layout="wide", page_icon="Slate@512w.png")

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

# --- PLOTLY BRAND TEMPLATE ---
_hc_template = pio.templates["plotly_white"]
_hc_template.layout.colorway = [
    "#F5A623", "#333333", "#D4910E", "#666666",
    "#FFC75F", "#999999", "#B37A00", "#CCCCCC",
]
_hc_template.layout.font = dict(color="#333333")
pio.templates.default = _hc_template

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
    session = Session()
    model_cls = SaleComp if model_name == "SaleComp" else LeaseComp
    df = pd.read_sql(session.query(model_cls).statement, session.bind)
    session.close()
    return df

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

st.image("HC-Logo-Stacked-Left-Charcoal@2000w.png", width=320)

# Sidebar: logo + user info + logout
st.sidebar.image("Slate@512w.png", width=60)
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

# Reset Filter Logic
def reset_callback():
    for key in list(st.session_state.keys()):
        if "filter_" in key:
            del st.session_state[key]

# --- FILTER WIDGETS ---
def render_numeric_filter(df, column, label, container=None):
    sb = container or st.sidebar
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
    sb = container or st.sidebar
    if column not in df.columns:
        return pd.Series([True] * len(df))
    search = sb.text_input(f"{label} contains:", placeholder="Search...", key=f"filter_txt_{column}")
    if search:
        return df[column].astype(str).str.contains(search, case=False, na=False)
    return pd.Series([True] * len(df))

def render_categorical_filter(df, column, label, container=None):
    sb = container or st.sidebar
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
    from utils import extract_zip_from_address, extract_city_from_address

    mask = pd.Series([True] * len(df))

    # Ensure city/zip columns exist (backfill from address if needed)
    if 'zip_code' not in df.columns or df['zip_code'].isna().all():
        if 'address' in df.columns:
            df['zip_code'] = df['address'].apply(extract_zip_from_address)
    if 'city' not in df.columns or df['city'].isna().all():
        if 'address' in df.columns:
            df['city'] = df['address'].apply(extract_city_from_address)

    # Location filters
    loc_count = count_active_filters("filter_cat_city") + count_active_filters("filter_cat_zip")
    has_proximity = bool(st.session_state.get("filter_loc_center"))
    total_loc = loc_count + (1 if has_proximity else 0)
    loc_label = f"Location ({total_loc} active)" if total_loc else "Location"
    with st.sidebar.expander(loc_label, expanded=total_loc > 0):
        mask &= render_categorical_filter(df, 'city', 'City')
        mask &= render_categorical_filter(df, 'zip_code', 'Zip Code')
        if include_proximity:
            st.sidebar.caption("Proximity Search")
            st.sidebar.text_input("Near address", placeholder="e.g. 123 Main St, Houston TX", key="filter_loc_center")
            st.sidebar.slider("Radius (mi)", 1, 50, 5, key="filter_loc_radius")

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
                date_range = st.sidebar.date_input("Closing Date", value=(), key="filter_date_sale")
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
                date_range = st.sidebar.date_input("Commencement Date", value=(), key="filter_date_lease")
                if isinstance(date_range, tuple) and len(date_range) == 2:
                    mask &= df['commencement_date'].astype(str) >= str(date_range[0])
                    mask &= df['commencement_date'].astype(str) <= str(date_range[1])

    return mask

# --- NAVIGATION ---
page = st.sidebar.radio("Navigate", ["Upload & Process", "Database View", "Analytics"])

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
                    result_df, conf = process_file_to_clean_output(df_input, uploaded_file.name)
                    st.session_state.clean_df = result_df
                    st.session_state.mapping_confidence = conf
                    st.session_state.current_filename = uploaded_file.name
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

            if missing_geos > 0:
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
                    geocoded = sum(1 for r in results if r[1] is not None)
                    status_text.text(f"Done! Geocoded {geocoded}/{len(results)} addresses.")
                    if warnings:
                        with st.expander(f"Geocoding warnings ({len(warnings)})"):
                            for w in warnings:
                                st.text(w)
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

                msg_parts = []
                if records:
                    msg_parts.append(f"Saved {len(records)} new records")
                if skipped:
                    msg_parts.append(f"skipped {skipped} duplicates")
                st.success(" | ".join(msg_parts) if msg_parts else "No records to save.")
                if skipped_details:
                    with st.expander(f"View {skipped} skipped duplicates"):
                        for detail in skipped_details:
                            st.text(detail)
                if records:
                    st.balloons()

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
    sale_count = len(load_data("SaleComp"))
    lease_count = len(load_data("LeaseComp"))
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
        # Ensure numeric columns are proper dtype
        numeric_cols_sale = ['sale_price', 'price_per_sf', 'building_size', 'year_built', 'cap_rate']
        numeric_cols_lease = ['rate_monthly', 'rate_annually', 'leased_sf', 'ti_allowance', 'clear_height', 'term_months']
        for col in (numeric_cols_sale if view_type == "Sales Comps" else numeric_cols_lease):
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')

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

            # Selection controls — simplified
            sel_col1, sel_col2, sel_col3 = st.columns([1, 1, 2])
            with sel_col1:
                if st.button("Select All", use_container_width=True):
                    st.session_state['_force_select'] = True
            with sel_col2:
                if st.button("Clear Selection", use_container_width=True):
                    st.session_state['_force_select'] = False
            with sel_col3:
                if len(df_page) > 1:
                    range_val = st.slider(
                        "Select row range", 1, len(df_page), (1, len(df_page)),
                        key="sel_range_slider", label_visibility="collapsed"
                    )
                    if range_val != (1, len(df_page)):
                        for idx in range(range_val[0] - 1, range_val[1]):
                            df_page.iloc[idx, df_page.columns.get_loc("Select")] = True

            # Apply forced selection state
            force = st.session_state.pop('_force_select', None)
            if force is True:
                df_page["Select"] = True
            elif force is False:
                df_page["Select"] = False

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
                st.success(f"Saved changes to {save_count} records.")
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
                    if st.button(f"Delete {len(selected_rows)} Selected Records", type="secondary", use_container_width=True):
                        session = Session()
                        ids_to_delete = selected_rows['id'].dropna().astype(int).tolist()
                        session.query(model_cls).filter(model_cls.id.in_(ids_to_delete)).delete(synchronize_session=False)
                        session.commit()
                        session.close()
                        load_data.clear()
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
                        st.rerun()

# =====================================================================
# PAGE 3: ANALYTICS
# =====================================================================
elif page == "Analytics":
    section_header("Analytics Dashboard")

    sales_df = load_data("SaleComp").copy()
    leases_df = load_data("LeaseComp").copy()

    # Type selector with counts
    analytics_type = st.radio(
        "Analyze",
        [f"Sales Comps ({len(sales_df)})", f"Lease Comps ({len(leases_df)})"],
        horizontal=True, key="analytics_type"
    )
    analytics_type = "Sales Comps" if "Sales" in analytics_type else "Lease Comps"

    # Sidebar filters
    st.sidebar.markdown("---")

    if analytics_type == "Sales Comps" and not sales_df.empty:
        for col in ['sale_price', 'price_per_sf', 'building_size', 'year_built', 'cap_rate']:
            if col in sales_df.columns:
                sales_df[col] = pd.to_numeric(sales_df[col], errors='coerce')
        analytics_mask = apply_sidebar_filters(sales_df, "Sales Comps")
        filtered_sales = sales_df[analytics_mask]
        filtered_leases = pd.DataFrame()
    elif analytics_type == "Lease Comps" and not leases_df.empty:
        for col in ['rate_monthly', 'rate_annually', 'leased_sf', 'ti_allowance', 'clear_height', 'term_months']:
            if col in leases_df.columns:
                leases_df[col] = pd.to_numeric(leases_df[col], errors='coerce')
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
                                           labels={'value': 'Sale Price ($)', 'count': 'Count'})
                        fig.update_layout(showlegend=False)
                        st.plotly_chart(fig, use_container_width=True)
                with col2:
                    psf_data = filtered_sales['price_per_sf'].dropna()
                    if not psf_data.empty:
                        fig = px.histogram(psf_data, nbins=20, title="$/SF Distribution",
                                           labels={'value': '$/SF', 'count': 'Count'})
                        fig.update_layout(showlegend=False)
                        st.plotly_chart(fig, use_container_width=True)
                size_data = filtered_sales['building_size'].dropna()
                if not size_data.empty:
                    fig = px.histogram(size_data, nbins=20, title="Building Size Distribution",
                                       labels={'value': 'Building Size (SF)', 'count': 'Count'})
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
                                labels={'building_size': 'Building Size (SF)', 'sale_price': 'Sale Price ($)'})
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
                                    labels={'closing_date': 'Closing Date', 'price_per_sf': '$/SF'})
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
                ).round(0).reset_index()
                zip_stats.columns = ['Zip Code', 'Count', 'Avg Price', 'Avg $/SF', 'Avg Size (SF)']
                st.dataframe(zip_stats, hide_index=True, use_container_width=True)

                fig = px.bar(zip_stats, x='Zip Code', y='Avg $/SF', color='Count',
                            title="Average $/SF by Zip Code",
                            color_continuous_scale=["#FFF3DC", "#F5A623", "#333333"])
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
                                           labels={'value': '$/SF/Mo', 'count': 'Count'})
                        fig.update_layout(showlegend=False)
                        st.plotly_chart(fig, use_container_width=True)
                with col2:
                    sf_data = filtered_leases['leased_sf'].dropna()
                    if not sf_data.empty:
                        fig = px.histogram(sf_data, nbins=20, title="Leased SF Distribution",
                                           labels={'value': 'Leased SF', 'count': 'Count'})
                        fig.update_layout(showlegend=False)
                        st.plotly_chart(fig, use_container_width=True)
                ti_data = filtered_leases['ti_allowance'].dropna()
                if not ti_data.empty:
                    fig = px.histogram(ti_data, nbins=15, title="TI Allowance Distribution",
                                       labels={'value': 'TI Allowance ($)', 'count': 'Count'})
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
                                labels={'leased_sf': 'Leased SF', 'rate_monthly': '$/SF/Mo'})
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
                                    labels={'commencement_date': 'Commencement Date', 'rate_monthly': '$/SF/Mo'})
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
                            color_continuous_scale=["#FFF3DC", "#F5A623", "#333333"])
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
