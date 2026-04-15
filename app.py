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
from comp_engine import robust_load_file, robust_load_file_segmented, process_file_to_clean_output, fetch_google_data, get_sheet_names, process_all_sheets, apply_manual_mapping, LEASE_SCHEMA, SALE_SCHEMA
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

def build_preview_col_config(ftype):
    """Return column_config dict for numeric formatting in preview tables."""
    cfg = {}
    # Sales numerics
    cfg["sale_price"] = st.column_config.NumberColumn("Sale Price", format="$%,.0f")
    cfg["price_per_sf"] = st.column_config.NumberColumn("$/SF", format="$%.2f")
    cfg["building_size"] = st.column_config.NumberColumn("Size (SF)", format="%,.0f")
    cfg["cap_rate"] = st.column_config.NumberColumn("Cap Rate", format="%.2f")
    cfg["year_built"] = st.column_config.NumberColumn("Year Built", format="%d")
    # Lease numerics
    cfg["rate_monthly"] = st.column_config.NumberColumn("$/SF/Mo", format="$%.2f")
    cfg["rate_annually"] = st.column_config.NumberColumn("$/SF/Yr", format="$%.2f")
    cfg["leased_sf"] = st.column_config.NumberColumn("Leased SF", format="%,.0f")
    cfg["ti_allowance"] = st.column_config.NumberColumn("TI", format="$%.2f")
    cfg["term_months"] = st.column_config.NumberColumn("Term (mo)", format="%d")
    cfg["clear_height"] = st.column_config.NumberColumn("Clear Height", format="%.1f")
    cfg["latitude"] = st.column_config.NumberColumn("Lat", format="%.4f")
    cfg["longitude"] = st.column_config.NumberColumn("Lng", format="%.4f")
    return cfg


def merge_duplicate_rows(df, addr_col='address'):
    """Merge rows with same normalized address. Later row wins for non-null fields.
    Returns (merged_df, merge_count)."""
    if df.empty or addr_col not in df.columns:
        return df, 0
    df = df.copy().reset_index(drop=True)
    norm = df[addr_col].astype(str).str.lower().str.strip().replace('nan', '')
    df['_norm_addr'] = norm
    # Only merge non-empty addrs
    mask_has_addr = df['_norm_addr'].astype(bool) & (df['_norm_addr'] != '')
    with_addr = df[mask_has_addr]
    without_addr = df[~mask_has_addr]

    merged_groups = []
    merge_count = 0
    for _, group in with_addr.groupby('_norm_addr', sort=False):
        if len(group) == 1:
            merged_groups.append(group.iloc[0])
            continue
        merge_count += len(group) - 1
        # Combine: newest (last) row wins for non-null, older fills gaps
        combined = group.iloc[0].copy()
        for _, row in group.iloc[1:].iterrows():
            for col in group.columns:
                val = row[col]
                if pd.notna(val) and str(val).strip() not in ('', 'None', 'nan'):
                    combined[col] = val
        merged_groups.append(combined)

    if merged_groups:
        merged_df = pd.DataFrame(merged_groups)
    else:
        merged_df = pd.DataFrame(columns=df.columns)
    merged_df = pd.concat([merged_df, without_addr], ignore_index=True)
    merged_df = merged_df.drop(columns=['_norm_addr'], errors='ignore')
    return merged_df, merge_count

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

_logo_b64 = _load_image_b64("Logo Masters/Stacked Left/@300w/HC-Logo-Stacked-Left-Charcoal@300w.png")
_icon_b64 = _load_image_b64("Logo Masters/Dry Dock/@300w/HC-Logo-Icon-White@300w.png")

# --- GLOBAL CSS ---
st.markdown("""
<style>
/* ── Hide Streamlit chrome ── */
#MainMenu {visibility: hidden;}
header {visibility: hidden;}
footer {visibility: hidden;}
.block-container {padding-top: 0 !important; padding-left: 74px !important;}
section[data-testid="stSidebar"] {display: none;}

/* ── Custom icon sidebar ── */
.hc-sidebar {
    position: fixed;
    top: 0; left: 0;
    width: 58px;
    height: 100vh;
    background: #333333;
    display: flex;
    flex-direction: column;
    align-items: center;
    padding: 12px 0;
    z-index: 200;
    gap: 0;
}
.hc-sidebar-logo {
    width: 36px;
    height: 36px;
    margin-bottom: 20px;
    object-fit: contain;
}
.hc-nav-item {
    display: flex;
    flex-direction: column;
    align-items: center;
    justify-content: center;
    width: 100%;
    padding: 10px 0;
    cursor: pointer;
    text-decoration: none;
    color: #aaa;
    font-size: 9px;
    font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
    font-weight: 600;
    letter-spacing: 0.5px;
    gap: 4px;
    transition: color 0.15s, background 0.15s;
}
.hc-nav-item:hover {color: #fff; background: rgba(255,255,255,0.06);}
.hc-nav-item.active {color: #F5A623; border-left: 2px solid #F5A623;}
.hc-nav-item svg {width: 18px; height: 18px; fill: currentColor;}
.hc-nav-spacer {flex: 1;}
.hc-nav-user {
    font-size: 9px;
    color: #888;
    text-align: center;
    padding: 6px 4px;
    word-break: break-all;
    line-height: 1.3;
}
.hc-nav-logout {
    font-size: 9px;
    color: #aaa;
    padding: 8px 0;
    cursor: pointer;
    text-decoration: none;
    text-align: center;
    width: 100%;
    display: block;
}
.hc-nav-logout:hover {color: #F5A623;}

/* ── Page topbar ── */
.hc-topbar {
    position: sticky;
    top: 0;
    z-index: 100;
    background: #ffffff;
    border-bottom: 1px solid #e8e8e8;
    padding: 10px 20px;
    display: flex;
    align-items: center;
    gap: 10px;
    font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
    margin-bottom: 1rem;
}
.hc-topbar-logo {height: 28px; object-fit: contain;}
.hc-topbar-divider {width: 1px; height: 24px; background: #e0e0e0; flex-shrink: 0;}
.hc-topbar-title {
    font-size: 14px;
    font-weight: 700;
    color: #333333;
    white-space: nowrap;
    letter-spacing: 0.3px;
}
.hc-topbar-right {margin-left: auto; display: flex; align-items: center; gap: 8px;}
.hc-selection-badge {
    background: #333333;
    color: #fff;
    border-radius: 12px;
    padding: 3px 10px;
    font-size: 11px;
    font-weight: 700;
    white-space: nowrap;
}
.hc-export-btn {
    background: #F5A623;
    color: #fff;
    border: none;
    border-radius: 6px;
    padding: 6px 14px;
    font-size: 12px;
    font-weight: 700;
    cursor: pointer;
    font-family: inherit;
    letter-spacing: 0.3px;
}
.hc-export-btn:hover {background: #D4910E;}
.hc-export-menu {
    position: relative;
    display: inline-block;
}
.hc-export-dropdown {
    position: absolute;
    top: 100%;
    right: 0;
    background: #fff;
    border: 1px solid #e0e0e0;
    border-radius: 8px;
    box-shadow: 0 4px 16px rgba(0,0,0,0.12);
    z-index: 300;
    min-width: 140px;
    padding: 4px 0;
}

/* ── Filter chips ── */
.hc-chip {
    display: inline-flex;
    align-items: center;
    gap: 4px;
    padding: 3px 10px 3px 10px;
    background: #FFF3DC;
    border: 1px solid #F5A623;
    border-radius: 12px;
    font-size: 11px;
    font-weight: 600;
    color: #333333;
    white-space: nowrap;
}
.hc-filter-bar {
    display: flex;
    flex-wrap: wrap;
    gap: 6px;
    align-items: center;
    margin: 0 8px;
}

/* ── Metric cards ── */
.hc-metric-card {
    background: #ffffff;
    border-radius: 9px;
    padding: 1rem 1.2rem;
    border-left: 4px solid #F5A623;
    box-shadow: 0 1px 4px rgba(0,0,0,0.07);
    margin-bottom: 0.5rem;
}
.hc-metric-value {
    font-size: 1.5rem;
    font-weight: 700;
    color: #333333;
    line-height: 1.2;
}
.hc-metric-label {
    font-size: 0.78rem;
    color: #777;
    margin-top: 0.2rem;
    font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
}

/* ── Table controls bar ── */
.hc-table-controls {
    display: flex;
    align-items: center;
    gap: 10px;
    margin-bottom: 8px;
    padding: 0 2px;
}
.hc-record-count {
    font-size: 12px;
    color: #777;
    white-space: nowrap;
}
.hc-record-count b {color: #333; font-weight: 700;}

/* ── Mapping status bar ── */
.hc-status-bar {
    display: flex;
    flex-wrap: wrap;
    gap: 6px;
    align-items: center;
    padding: 8px 12px;
    background: #f8f8f8;
    border-radius: 8px;
    margin-bottom: 1rem;
    border: 1px solid #e8e8e8;
}
.hc-tag-mapped {
    background: #E8F5E9;
    color: #2E7D32;
    border-radius: 6px;
    padding: 2px 8px;
    font-size: 11px;
    font-weight: 600;
}
.hc-tag-unmapped {
    background: #FFEBEE;
    color: #C62828;
    border-radius: 6px;
    padding: 2px 8px;
    font-size: 11px;
    font-weight: 600;
}

/* ── Comp finder result card ── */
.cf-card {
    background: #fff;
    border-radius: 8px;
    padding: 10px 14px;
    margin-bottom: 8px;
    border: 1px solid #eee;
    font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
}
.cf-rank-badge {
    display: inline-block;
    width: 22px;
    height: 22px;
    border-radius: 50%;
    text-align: center;
    line-height: 22px;
    font-size: 11px;
    font-weight: 700;
    margin-right: 6px;
}
.cf-rank-top {background: #F5A623; color: #fff;}
.cf-rank-rest {background: #e0e0e0; color: #555;}
.cf-match-bar {
    height: 4px;
    border-radius: 2px;
    background: linear-gradient(to right, #F5A623, #FFC75F);
    margin-top: 4px;
}

/* ── General layout ── */
.hc-main {background: #f4f5f7; min-height: 100vh;}
.hc-content {padding: 0 16px 24px 16px;}

/* ── Legacy helpers (section_header, render_metric_card) ── */
.section-header {
    color: #333333;
    font-size: 1.1rem;
    font-weight: 700;
    margin: 1.2rem 0 0.3rem 0;
    padding-bottom: 0.4rem;
    border-bottom: 2px solid #F5A623;
}
.section-subtitle {
    color: #666;
    font-size: 0.82rem;
    margin-top: -0.2rem;
    margin-bottom: 0.8rem;
}
.metric-card {
    background: #ffffff;
    border-radius: 9px;
    padding: 1rem 1.2rem;
    border-left: 4px solid #F5A623;
    box-shadow: 0 1px 4px rgba(0,0,0,0.07);
    margin-bottom: 0.5rem;
}
.metric-value {
    font-size: 1.5rem;
    font-weight: 700;
    color: #333333;
    line-height: 1.2;
}
.metric-label {
    font-size: 0.78rem;
    color: #777;
    margin-top: 0.2rem;
    font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
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
.step-active { background-color: #F5A623; color: #333333; }
.step-done { background-color: #333333; color: #F5A623; }
.step-pending { background-color: #e0e0e0; color: #999; }
.step-label { font-weight: 600; font-size: 1.05rem; }
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

def render_sidebar(current_page, username, user_role):
    """Render fixed HTML icon sidebar. Query-param routing: each icon links to ?page=<slug>."""
    icon_b64_src = f'data:image/png;base64,{_icon_b64}' if _icon_b64 else ''
    logo_img = f'<img class="hc-sidebar-logo" src="{icon_b64_src}" alt="HC">' if icon_b64_src else '<div style="width:36px;height:36px;background:#F5A623;border-radius:4px;margin-bottom:20px;"></div>'

    def nav_item(slug, label, svg_path, page_name):
        active_cls = ' active' if current_page == page_name else ''
        return f'''<a class="hc-nav-item{active_cls}" href="?page={slug}">
            <svg viewBox="0 0 24 24" xmlns="http://www.w3.org/2000/svg"><path d="{svg_path}"/></svg>
            {label}
        </a>'''

    # SVG icon paths (simple 24x24 Material-style)
    SVG_DB    = "M4 6h16v2H4zm0 5h16v2H4zm0 5h16v2H4z"
    SVG_UP    = "M9 16h6v-6h4l-7-7-7 7h4zm-4 2h14v2H5z"
    SVG_AN    = "M19 3H5c-1.1 0-2 .9-2 2v14c0 1.1.9 2 2 2h14c1.1 0 2-.9 2-2V5c0-1.1-.9-2-2-2zM9 17H7v-7h2v7zm4 0h-2V7h2v10zm4 0h-2v-4h2v4z"
    SVG_CF    = "M12 2C8.13 2 5 5.13 5 9c0 5.25 7 13 7 13s7-7.75 7-13c0-3.87-3.13-7-7-7zm0 9.5c-1.38 0-2.5-1.12-2.5-2.5s1.12-2.5 2.5-2.5 2.5 1.12 2.5 2.5-1.12 2.5-2.5 2.5z"

    initials = (username or "U")[0].upper()
    short_role = user_role[:5].upper() if user_role else ""

    html = f'''
    <div class="hc-sidebar">
        {logo_img}
        {nav_item("database", "DB", SVG_DB, "Database View")}
        {nav_item("upload", "UPLOAD", SVG_UP, "Upload & Process")}
        {nav_item("analytics", "STATS", SVG_AN, "Analytics")}
        {nav_item("finder", "FINDER", SVG_CF, "Comp Finder")}
        <div class="hc-nav-spacer"></div>
        <div class="hc-nav-user">{initials}<br>{short_role}</div>
        <a class="hc-nav-logout" href="?action=logout">OUT</a>
    </div>
    '''
    st.markdown(html, unsafe_allow_html=True)

def render_topbar(page_title, filter_chips_html="", right_html=""):
    """Render sticky page topbar with logo, title, optional filter chips, and right-side controls."""
    logo_src = f'data:image/png;base64,{_logo_b64}' if _logo_b64 else ''
    logo_img = f'<img class="hc-topbar-logo" src="{logo_src}" alt="Harbor Capital">' if logo_src else '<span style="font-weight:800;font-size:13px;color:#333;">HARBOR CAPITAL</span>'

    chips_section = f'<div class="hc-filter-bar">{filter_chips_html}</div>' if filter_chips_html else ''

    st.markdown(f'''
    <div class="hc-topbar">
        {logo_img}
        <div class="hc-topbar-divider"></div>
        <div class="hc-topbar-title">{page_title}</div>
        {chips_section}
        <div class="hc-topbar-right">{right_html}</div>
    </div>
    ''', unsafe_allow_html=True)

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

# Handle logout via query param (?action=logout from the HTML sidebar "OUT" link)
if st.query_params.get("action") == "logout":
    for k in ["authentication_status", "name", "username", "logout"]:
        st.session_state.pop(k, None)
    st.query_params.clear()
    st.rerun()

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
if 'sheet_data' not in st.session_state:
    st.session_state.sheet_data = {}  # {sheet_name: {raw_df, clean_df, mappings, confidence, file_type, input_columns}}
if 'show_filter_panel' not in st.session_state:
    st.session_state.show_filter_panel = False
if 'show_export_menu' not in st.session_state:
    st.session_state.show_export_menu = False
if 'show_cf_export_menu' not in st.session_state:
    st.session_state.show_cf_export_menu = False
if 'db_search_text' not in st.session_state:
    st.session_state.db_search_text = ""

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

# --- NAVIGATION: query-param routing ---
_PAGE_MAP = {
    "database": "Database View",
    "upload": "Upload & Process",
    "analytics": "Analytics",
    "finder": "Comp Finder",
}
_qp = st.query_params.get("page")
if _qp and _qp in _PAGE_MAP:
    target = _PAGE_MAP[_qp]
    if st.session_state.get("page") != target:
        st.session_state["page"] = target
    st.query_params.clear()
    st.rerun()

if "page" not in st.session_state:
    st.session_state["page"] = "Database View"

page = st.session_state["page"]
username = st.session_state.get("username", "")

# Render custom icon sidebar on every page
render_sidebar(page, username, user_role)

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

        # Detect sheets for multi-sheet Excel files
        sheets = get_sheet_names(path)
        selected_sheets = sheets  # default: all sheets
        if len(sheets) > 1:
            selected_sheets = st.multiselect(
                f"This file has {len(sheets)} sheets. Select which to process:",
                sheets,
                default=sheets,
                key="sheet_selector",
            )
            if not selected_sheets:
                st.warning("Select at least one sheet to process.")

        if st.session_state.current_filename != uploaded_file.name and selected_sheets:
            with st.spinner('AI is analyzing columns...'):
                sheet_data = {}
                for sheet in selected_sheets:
                    base_label = sheet or "Sheet1"
                    segments = robust_load_file_segmented(path, sheet_name=sheet)
                    if not segments:
                        st.warning(f"Sheet '{base_label}': could not read or empty")
                        continue
                    for seg in segments:
                        # Build label
                        if len(segments) == 1:
                            seg_label = base_label
                        else:
                            title = seg.get('segment_title')
                            sec_num = seg['segment_index'] + 1
                            seg_label = f"{base_label} - {title or f'Section {sec_num}'}"

                        raw_df = seg['df']
                        if raw_df.empty:
                            continue
                        if len(raw_df) >= 500:
                            st.warning(f"'{seg_label}': {len(raw_df)} rows, truncated to 500.")
                        try:
                            clean_df, conf, mappings = process_file_to_clean_output(raw_df, uploaded_file.name, sheet_name=sheet)
                            ftype = clean_df['source_type'].iloc[0] if not clean_df.empty else "UNKNOWN"
                            sheet_data[seg_label] = {
                                'raw_df': raw_df,
                                'clean_df': clean_df,
                                'mappings': mappings,
                                'confidence': conf,
                                'file_type': ftype,
                                'input_columns': list(raw_df.columns),
                            }
                        except Exception as e:
                            st.warning(f"'{seg_label}': {e}")

                if sheet_data:
                    # Combine all clean_dfs for the unified pipeline
                    all_clean = []
                    for sl, sd in sheet_data.items():
                        cdf = sd['clean_df'].copy()
                        cdf['source_sheet'] = sl
                        all_clean.append(cdf)
                    combined = pd.concat(all_clean, ignore_index=True)
                    st.session_state.clean_df = combined
                    st.session_state.mapping_confidence = None  # per-sheet now
                    st.session_state.current_filename = uploaded_file.name
                    st.session_state.geocoding_done = False
                    st.session_state.sheet_data = sheet_data
                    st.success(f"Parsed {len(combined)} records from {len(sheet_data)} sheet(s)!")
                else:
                    st.error("Could not read any sheets. Check the file format.")

        if st.session_state.clean_df is not None:
            df = st.session_state.clean_df
            sheet_data = st.session_state.sheet_data
            sheet_names = list(sheet_data.keys())

            # --- PER-SHEET TABS: Mapping + Preview ---
            if len(sheet_names) > 1:
                section_header("Sheets", f"{len(sheet_names)} sheets loaded — review each tab")
                sheet_tabs = st.tabs(sheet_names)
            else:
                sheet_tabs = [st.container()]

            for tab_idx, (sheet_tab, sheet_name) in enumerate(zip(sheet_tabs, sheet_names)):
                sd = sheet_data[sheet_name]
                with sheet_tab:
                    if len(sheet_names) > 1:
                        st.markdown(f'<div class="section-subtitle">{sheet_name} — {len(sd["clean_df"])} records ({sd["file_type"]})</div>', unsafe_allow_html=True)

                    # --- COLUMN MAPPING for this sheet ---
                    ftype = sd['file_type']
                    if ftype == "SALE":
                        mapping_schema = SALE_SCHEMA
                    elif ftype in ("LEASE", "BOTH"):
                        mapping_schema = LEASE_SCHEMA
                    else:
                        mapping_schema = {**LEASE_SCHEMA, **SALE_SCHEMA}

                    current_maps = sd['mappings'] or {}
                    input_cols = sd['input_columns']
                    conf = sd['confidence']
                    skip_option = "-- Skip --"
                    selectbox_options = [skip_option] + input_cols

                    # --- RAW INPUT DATA PREVIEW ---
                    raw_df = sd['raw_df']
                    raw_preview = raw_df.head(5)

                    st.markdown(f'<div class="section-header" style="font-size:1.1rem;">Raw Input Data</div>', unsafe_allow_html=True)
                    st.markdown(f'<p style="color:#666;font-size:0.82rem;margin-bottom:0.5rem;">{len(raw_df)} rows, {len(raw_df.columns)} columns — showing first 5 rows</p>', unsafe_allow_html=True)

                    # Build a color-coded version: highlight columns that are mapped
                    mapped_input_cols = set(current_maps.values())

                    # Build column highlights HTML table
                    def _col_badge(col_name):
                        """Return which target field this input column is mapped to, if any."""
                        for target, src in current_maps.items():
                            if src == col_name:
                                return target
                        return None

                    # Show raw data with column header badges
                    header_html = '<div style="overflow-x:auto;border:1px solid #e0e0e0;border-radius:8px;margin-bottom:1rem;">'
                    header_html += '<table style="width:100%;border-collapse:collapse;font-size:0.82rem;">'

                    # Column name row with mapping badges
                    header_html += '<tr style="background:#FAF7F2;">'
                    for col in raw_preview.columns:
                        target = _col_badge(col)
                        if target:
                            badge = f'<div style="background:#F5A623;color:#fff;padding:2px 6px;border-radius:4px;font-size:0.7rem;margin-top:2px;display:inline-block;">{target.replace("_"," ").title()}</div>'
                        else:
                            badge = '<div style="background:#e0e0e0;color:#999;padding:2px 6px;border-radius:4px;font-size:0.7rem;margin-top:2px;display:inline-block;">unmapped</div>'
                        col_display = str(col)[:25]
                        header_html += f'<th style="padding:8px 10px;border-bottom:2px solid #F5A623;text-align:left;white-space:nowrap;">{col_display}<br>{badge}</th>'
                    header_html += '</tr>'

                    # Data rows
                    for _, row in raw_preview.iterrows():
                        header_html += '<tr>'
                        for col in raw_preview.columns:
                            val = row[col]
                            cell_text = str(val)[:30] if pd.notna(val) else '<span style="color:#ccc;">—</span>'
                            bg = '' if col not in mapped_input_cols else 'background:#FFF8EC;'
                            header_html += f'<td style="padding:6px 10px;border-bottom:1px solid #eee;{bg}white-space:nowrap;">{cell_text}</td>'
                        header_html += '</tr>'

                    header_html += '</table></div>'
                    st.markdown(header_html, unsafe_allow_html=True)

                    # --- COLUMN MAPPING CONTROLS ---
                    st.markdown(f'<div class="section-header" style="font-size:1.1rem;">Column Mapping</div>', unsafe_allow_html=True)
                    st.markdown('<p style="color:#666;font-size:0.82rem;margin-bottom:0.5rem;">Select which input column maps to each target field. Amber-highlighted columns above are currently mapped.</p>', unsafe_allow_html=True)

                    # Render mapping rows in a compact 2-column grid
                    target_fields = list(mapping_schema.items())
                    mid = (len(target_fields) + 1) // 2
                    left_fields = target_fields[:mid]
                    right_fields = target_fields[mid:]

                    map_col_left, map_col_right = st.columns(2)

                    for col_container, fields in [(map_col_left, left_fields), (map_col_right, right_fields)]:
                        with col_container:
                            for target_field, field_info in fields:
                                mapped_col = current_maps.get(target_field)
                                field_conf = conf.get(target_field, 0.0)

                                if field_conf >= 1.0:
                                    dot_color = "#4CAF50"
                                elif field_conf >= 0.60:
                                    dot_color = "#4CAF50"
                                elif field_conf >= 0.45:
                                    dot_color = "#F5A623"
                                else:
                                    dot_color = "#EF5350"

                                label = target_field.replace('_', ' ').title()
                                # Show sample value from mapped column
                                sample = ""
                                if mapped_col and mapped_col in raw_df.columns:
                                    first_val = raw_df[mapped_col].dropna().head(1)
                                    if not first_val.empty:
                                        sample = str(first_val.iloc[0])[:25]

                                st.markdown(
                                    f'<span style="display:inline-block;width:8px;height:8px;border-radius:50%;background:{dot_color};margin-right:6px;vertical-align:middle;"></span>'
                                    f'<b style="font-size:0.9rem;">{label}</b>'
                                    f'{f" — <i style=&quot;color:#888;font-size:0.78rem;&quot;>{sample}</i>" if sample else ""}',
                                    unsafe_allow_html=True,
                                )

                                default_idx = 0
                                if mapped_col and mapped_col in input_cols:
                                    default_idx = selectbox_options.index(mapped_col)
                                st.selectbox(
                                    f"Map {target_field}",
                                    selectbox_options,
                                    index=default_idx,
                                    key=f"mapping_sel_{sheet_name}_{target_field}",
                                    label_visibility="collapsed",
                                )

                    st.markdown("")
                    if st.button("Re-Map Columns", type="primary", use_container_width=True, key=f"remap_btn_{sheet_name}"):
                        user_mappings = {}
                        for target_field in mapping_schema:
                            sel = st.session_state.get(f"mapping_sel_{sheet_name}_{target_field}", skip_option)
                            if sel != skip_option:
                                user_mappings[target_field] = sel

                        new_df, new_conf = apply_manual_mapping(
                            sd['raw_df'], user_mappings, mapping_schema, ftype, uploaded_file.name
                        )
                        sd['clean_df'] = new_df
                        sd['confidence'] = new_conf
                        sd['mappings'] = user_mappings
                        st.session_state.sheet_data[sheet_name] = sd
                        # Rebuild combined clean_df
                        all_clean = []
                        for sl, s in st.session_state.sheet_data.items():
                            cdf = s['clean_df'].copy()
                            cdf['source_sheet'] = sl
                            all_clean.append(cdf)
                        st.session_state.clean_df = pd.concat(all_clean, ignore_index=True)
                        st.session_state.geocoding_done = False
                        st.toast(f"Mapping updated for {sheet_name}!", icon="\u2705")
                        st.rerun()

                    # --- MAPPED OUTPUT PREVIEW ---
                    st.markdown(f'<div class="section-header" style="font-size:1.1rem;">Mapped Output</div>', unsafe_allow_html=True)
                    sheet_df = sd['clean_df']
                    cols_to_show = list(sheet_df.columns)
                    hide_cols = ['source_type', 'source_file', 'rate_basis']
                    if ftype == "LEASE" and 'rate_monthly' in cols_to_show:
                        priority = ['address', 'rate_monthly', 'rate_annually']
                        cols_to_show = priority + [c for c in cols_to_show if c not in priority and c not in hide_cols]
                    elif ftype == "SALE":
                        cols_to_show = [c for c in cols_to_show if c not in ['rate_monthly', 'rate_annually', 'rate_basis'] and c not in hide_cols]
                    else:
                        cols_to_show = [c for c in cols_to_show if c not in hide_cols]

                    st.dataframe(sheet_df[cols_to_show], use_container_width=True, hide_index=True, height=250, column_config=build_preview_col_config(ftype))

            # --- GEOCODING (runs on combined df) ---
            df = st.session_state.clean_df
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
                            st.info("Tip: Check that addresses include street numbers and city names.")
            else:
                st.success("All addresses have been geocoded!")

            # --- FINAL PREVIEW & SAVE (per-sheet tabs when multi) ---
            section_header("Save to Database", f"{len(df)} total records ready")
            types_in_file = df['source_type'].unique().tolist()
            has_leases = any(t in ('LEASE', 'BOTH') for t in types_in_file)
            has_sales = any(t in ('SALE', 'BOTH') for t in types_in_file)
            stype = types_in_file[0] if len(types_in_file) == 1 else "MIXED"

            def _select_cols(src_df, ftype_local):
                c2s = list(src_df.columns)
                hc = ['source_file', 'rate_basis']
                if ftype_local in ("LEASE", "BOTH") and 'rate_monthly' in c2s:
                    hc.append('source_type')
                    pr = ['address', 'rate_monthly', 'rate_annually', 'rate_basis']
                    return pr + [c for c in c2s if c not in pr and c not in hc]
                if ftype_local == "SALE":
                    hc.append('source_type')
                    return [c for c in c2s if c not in ['rate_monthly', 'rate_annually', 'rate_basis'] and c not in hc]
                return [c for c in c2s if c not in hc]

            edited_frames = {}  # sheet_name -> edited sub-df
            if len(sheet_names) > 1:
                save_tabs = st.tabs([f"{sn} ({len(sheet_data[sn]['clean_df'])})" for sn in sheet_names])
                for save_tab, sn in zip(save_tabs, sheet_names):
                    with save_tab:
                        sd_local = sheet_data[sn]
                        ftype_local = sd_local['file_type']
                        # Slice combined df by source_sheet to pick up geocoded values
                        sub_df = df[df['source_sheet'] == sn].copy() if 'source_sheet' in df.columns else sd_local['clean_df'].copy()
                        if sub_df.empty:
                            sub_df = sd_local['clean_df'].copy()
                        cols_show = _select_cols(sub_df, ftype_local)
                        edited_frames[sn] = st.data_editor(
                            sub_df[cols_show],
                            num_rows="dynamic",
                            column_config=build_preview_col_config(ftype_local),
                            key=f"final_editor_{sn}",
                            use_container_width=True,
                        )
                # Recombine for save
                edited_df = pd.concat(list(edited_frames.values()), ignore_index=True)
            else:
                cols_show = _select_cols(df, stype)
                edited_df = st.data_editor(
                    df[cols_show],
                    num_rows="dynamic",
                    column_config=build_preview_col_config(stype),
                    key="final_editor_single",
                    use_container_width=True,
                )

            if st.button("Save to Database", type="primary", use_container_width=True):
                # Upload original file to Supabase Storage
                file_url = None
                try:
                    file_url = upload_to_storage(uploaded_file.getvalue(), uploaded_file.name)
                except Exception as e:
                    st.warning(f"Could not upload source file to storage: {e}")

                # Fetch existing records for duplicate detection (both tables)
                session_dup = Session()
                existing_sale_records = []
                existing_lease_records = []
                try:
                    if has_sales:
                        existing_sale_records = [(r.id, r.address) for r in session_dup.query(SaleComp.id, SaleComp.address).all()]
                    if has_leases:
                        existing_lease_records = [(r.id, r.address) for r in session_dup.query(LeaseComp.id, LeaseComp.address).all()]
                except Exception:
                    pass
                session_dup.close()

                # Merge intra-upload duplicates (same address within upload) — newest non-null wins
                edited_df, intra_merged = merge_duplicate_rows(edited_df, addr_col='address')
                if intra_merged > 0:
                    st.info(f"Combined {intra_merged} duplicate row(s) within this upload (same address).")

                session = Session()
                records = []
                replaced = 0
                replaced_details = []
                sale_count = 0
                lease_count = 0
                bar = st.progress(0)

                for i, row in edited_df.iterrows():
                    addr = clean_text_val(row.get('address'))
                    row_type = row.get('source_type', stype)
                    is_sale = row_type == "SALE"
                    row_model = SaleComp if is_sale else LeaseComp
                    existing_records = existing_sale_records if is_sale else existing_lease_records

                    # Auto-replace duplicates: update existing record with new data
                    dup_id = None
                    if addr and existing_records:
                        matches = find_duplicates(addr, existing_records)
                        if matches:
                            dup_id = matches[0][0]
                            replaced += 1
                            replaced_details.append(f"Row {i+1}: {addr[:50]} (replaced ID {dup_id}: {matches[0][1][:50]})")

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

                    if is_sale:
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
                        sale_count += 1
                    else:
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
                        lease_count += 1

                    all_fields = {**common, **specific}
                    if dup_id:
                        # Update existing record
                        session.query(row_model).filter_by(id=dup_id).update(all_fields)
                    else:
                        records.append(row_model(**all_fields))
                    bar.progress((i + 1) / len(edited_df))

                if records:
                    session.add_all(records)
                session.commit()
                session.close()
                load_data.clear()
                get_record_counts.clear()

                msg_parts = []
                new_count = len(records)
                if new_count:
                    type_detail = []
                    if sale_count:
                        type_detail.append(f"{sale_count} sales")
                    if lease_count:
                        type_detail.append(f"{lease_count} leases")
                    msg_parts.append(f"{new_count} new records ({', '.join(type_detail)})")
                if replaced:
                    msg_parts.append(f"{replaced} updated (replaced duplicates)")
                msg = "Saved: " + " | ".join(msg_parts) if msg_parts else "No records to save."
                st.toast(msg, icon="\u2705")
                st.success(msg)
                if replaced_details:
                    with st.expander(f"View {replaced} replaced duplicates"):
                        for detail in replaced_details:
                            st.text(detail)

                # Cleanup
                try:
                    os.unlink(path)
                except Exception:
                    pass
                st.session_state.clean_df = None
                st.session_state.mapping_confidence = None
                st.session_state.sheet_data = {}

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

        # --- APPLY FILTERS ---
        df_filtered = df[mask].copy()

        st.markdown(
            f'<div class="record-count">Showing <b>{len(df_filtered)}</b> of {len(df)} records</div>',
            unsafe_allow_html=True
        )

        # Column ordering for leases
        if view_type == "Lease Comps":
            priority = ['address', 'rate_monthly', 'rate_annually', 'leased_sf', 'tenant_name']
            other_cols = [c for c in df_filtered.columns if c not in priority]
            df_filtered = df_filtered[priority + other_cols]

        # Column config
        col_config = {"Select": st.column_config.CheckboxColumn("Select", default=False)}
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
            # Hide internal columns from display
            hide_cols = ['created_at', 'raw_address_data', 'source_file']
            display_df = df_filtered.drop(columns=[c for c in hide_cols if c in df_filtered.columns])

            if user_role == "admin":
                # Admin: editable data_editor with checkbox selection
                display_df.insert(0, "Select", False)
                edited_view = st.data_editor(
                    display_df,
                    hide_index=True,
                    column_config=col_config,
                    use_container_width=True,
                    height=600,
                )
                selected_rows = edited_view[edited_view["Select"] == True].drop(columns=["Select"], errors="ignore")
            else:
                # Non-admin: read-only with native row selection
                event = st.dataframe(
                    display_df,
                    hide_index=True,
                    column_config=col_config,
                    use_container_width=True,
                    height=600,
                    on_select="rerun",
                    selection_mode="multi-row",
                )
                sel_indices = event.selection.rows if event.selection else []
                selected_rows = display_df.iloc[sel_indices] if sel_indices else pd.DataFrame()

            # Save edits (admin only)
            if user_role == "admin" and st.button("Save Changes to Database", use_container_width=True):
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
            if not selected_rows.empty:
                section_header("Export", f"{len(selected_rows)} properties selected")
                export_df = selected_rows.drop(columns=["Select"], errors="ignore")
            else:
                section_header("Export", f"All {len(df_filtered)} filtered properties")
                export_df = df_filtered

            st.dataframe(
                export_df,
                column_config=col_config,
                use_container_width=True,
                hide_index=True,
            )

            exp1, exp2, exp3 = st.columns(3)
            with exp1:
                st.download_button(
                    "KML", generate_kml(export_df),
                    "comps.kml", "application/vnd.google-earth.kml+xml",
                    use_container_width=True,
                )
            with exp2:
                st.download_button(
                    "Excel", to_excel_bytes(export_df),
                    "comps.xlsx", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    use_container_width=True,
                )
            with exp3:
                st.download_button(
                    "CSV", export_df.to_csv(index=False),
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
