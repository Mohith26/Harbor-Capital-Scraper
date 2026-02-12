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
import database
import importlib
importlib.reload(database)
from database import Session, SaleComp, LeaseComp, engine
from comp_engine import robust_load_file, process_file_to_clean_output, fetch_google_data

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
st.set_page_config(page_title="Harbor Capital Comp Database", layout="wide")

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

st.title("Harbor Capital Comp Intelligence")

# Sidebar: user info + logout
st.sidebar.markdown(f"**Logged in as:** {st.session_state.get('name', '')} ({user_role})")
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
            st.session_state[key] = None
        if "radius" in key:
            st.session_state[key] = 5

# --- FILTER WIDGETS ---
def render_numeric_filter(df, column, label):
    if column not in df.columns:
        return pd.Series([True] * len(df))
    col_data = df[column].dropna()
    min_v = float(col_data.min()) if not col_data.empty else 0.0
    max_v = float(col_data.max()) if not col_data.empty else 1000000.0
    c1, c2 = st.sidebar.columns(2)
    val_min = c1.number_input(f"Min {label}", value=None, placeholder=f"{min_v:,.0f}", key=f"filter_min_{column}")
    val_max = c2.number_input(f"Max {label}", value=None, placeholder=f"{max_v:,.0f}", key=f"filter_max_{column}")
    mask = pd.Series([True] * len(df))
    if val_min is not None:
        mask &= (df[column] >= val_min)
    if val_max is not None:
        mask &= (df[column] <= val_max)
    return mask

def render_text_filter(df, column, label):
    if column not in df.columns:
        return pd.Series([True] * len(df))
    search = st.sidebar.text_input(f"{label} contains:", placeholder="Search...", key=f"filter_txt_{column}")
    if search:
        return df[column].astype(str).str.contains(search, case=False, na=False)
    return pd.Series([True] * len(df))

# --- NAVIGATION ---
page = st.sidebar.radio("Navigate", ["Upload & Process", "Database View", "Analytics"])

# =====================================================================
# PAGE 1: UPLOAD & PROCESS
# =====================================================================
if page == "Upload & Process":
    st.header("1. Upload Raw Comp Sheets")
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
                st.markdown("---")
                st.subheader("Column Mapping Confidence")
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
                    width="stretch",
                    hide_index=True,
                )

            # --- GEOCODING ---
            st.markdown("---")
            st.subheader("2. Geocoding & Standardization")
            missing_geos = df['latitude'].isna().sum()

            if missing_geos > 0:
                api_key = get_secret("GOOGLE_API_KEY", "")
                if not api_key:
                    st.error("Google API Key not configured. Add GOOGLE_API_KEY to your secrets.")
                else:
                    st.warning(f"**Approval Required:** {len(df)} properties, {missing_geos} need geocoding.")
                    if st.button("Approve & Geocode"):
                        progress_bar = st.progress(0)
                        status_text = st.empty()
                        results = []
                        for i, row in df.iterrows():
                            raw_addr = str(row.get('raw_address_data', '') or row.get('address', '') or '')
                            status_text.text(f"Geocoding {i+1}/{len(df)}: {raw_addr[:60]}...")
                            addr, lat, lng = fetch_google_data(raw_addr, api_key)
                            results.append((addr, lat, lng))
                            progress_bar.progress((i + 1) / len(df))
                        df['address'] = [x[0] for x in results]
                        df['latitude'] = [x[1] for x in results]
                        df['longitude'] = [x[2] for x in results]
                        st.session_state.clean_df = df
                        geocoded = sum(1 for r in results if r[1] is not None)
                        status_text.text(f"Done! Geocoded {geocoded}/{len(results)} addresses.")
                        st.rerun()
            else:
                st.success("All addresses have been geocoded!")

            # --- PREVIEW & SAVE ---
            st.markdown("---")
            st.subheader("3. Preview & Save")

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

            if st.button("Save to Database", type="primary"):
                # Duplicate detection
                session = Session()
                existing = set()
                try:
                    model_cls = SaleComp if stype == "SALE" else LeaseComp
                    existing_records = session.query(model_cls.address, model_cls.source_file).all()
                    existing = {(r.address, r.source_file) for r in existing_records}
                except Exception:
                    pass

                records = []
                duplicates = 0
                bar = st.progress(0)

                for i, row in edited_df.iterrows():
                    addr = clean_text_val(row.get('address'))
                    if (addr, uploaded_file.name) in existing:
                        duplicates += 1
                        continue

                    common = {
                        'address': addr,
                        'latitude': row.get('latitude'),
                        'longitude': row.get('longitude'),
                        'raw_address_data': clean_text_val(row.get('raw_address_data')),
                        'source_file': uploaded_file.name,
                        'notes': clean_text_val(row.get('notes')),
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
                            seller=clean_text_val(row.get('seller')),
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
                            clear_height=clean_currency_num(row.get('clear_height')),
                        )
                    else:
                        continue

                    records.append(rec)
                    bar.progress((i + 1) / len(edited_df))

                session.add_all(records)
                session.commit()
                session.close()

                if duplicates > 0:
                    st.warning(f"Skipped {duplicates} duplicate records (same address + source file).")
                st.success(f"Saved {len(records)} records to database.")
                st.balloons()

                # Cleanup temp file
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
    st.header("Database Explorer")
    view_type = st.radio("Select Data Type", ["Sales Comps", "Lease Comps"], horizontal=True)

    session = Session()
    model_cls = SaleComp if view_type == "Sales Comps" else LeaseComp
    df = pd.read_sql(session.query(model_cls).statement, session.bind)
    session.close()

    if df.empty:
        st.info("Database is empty. Upload files on the Upload page.")
    else:
        # --- SIDEBAR CONTROLS ---
        st.sidebar.button("Reset All Filters", on_click=reset_callback)
        st.sidebar.markdown("---")

        # Location search
        st.sidebar.header("Location Search")
        center_addr = st.sidebar.text_input("Address (Nearby)", placeholder="e.g. 123 Main St, Houston TX", key="filter_loc_center")
        radius = st.sidebar.slider("Radius (Miles)", 1, 50, 5, key="filter_loc_radius")

        st.sidebar.markdown("---")
        st.sidebar.header("Filters")

        mask = pd.Series([True] * len(df))

        if view_type == "Sales Comps":
            with st.sidebar.expander("Financials", expanded=True):
                mask &= render_numeric_filter(df, 'sale_price', 'Price ($)')
                mask &= render_numeric_filter(df, 'price_per_sf', '$/SF')
            with st.sidebar.expander("Property Details", expanded=False):
                mask &= render_numeric_filter(df, 'building_size', 'Size (SF)')
                mask &= render_numeric_filter(df, 'year_built', 'Year Built')
                mask &= render_text_filter(df, 'address', 'Address')
            with st.sidebar.expander("Deal Info", expanded=False):
                mask &= render_text_filter(df, 'buyer', 'Buyer')
                mask &= render_text_filter(df, 'seller', 'Seller')
                mask &= render_text_filter(df, 'notes', 'Notes')
            # Date range filter
            with st.sidebar.expander("Date Range", expanded=False):
                date_min = st.sidebar.date_input("Closing Date From", value=None, key="filter_date_min")
                date_max = st.sidebar.date_input("Closing Date To", value=None, key="filter_date_max")
                if date_min and 'closing_date' in df.columns:
                    mask &= df['closing_date'].astype(str) >= str(date_min)
                if date_max and 'closing_date' in df.columns:
                    mask &= df['closing_date'].astype(str) <= str(date_max)

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
                mask &= render_text_filter(df, 'building_type', 'Building Type')
            with st.sidebar.expander("Date Range", expanded=False):
                date_min = st.sidebar.date_input("Commencement From", value=None, key="filter_lease_date_min")
                date_max = st.sidebar.date_input("Commencement To", value=None, key="filter_lease_date_max")
                if date_min and 'commencement_date' in df.columns:
                    mask &= df['commencement_date'].astype(str) >= str(date_min)
                if date_max and 'commencement_date' in df.columns:
                    mask &= df['commencement_date'].astype(str) <= str(date_max)

        # Distance calculation
        if center_addr:
            with st.spinner("Calculating distances..."):
                _, lat_c, lon_c = fetch_google_data(center_addr, get_secret("GOOGLE_API_KEY", ""))
                if lat_c:
                    df['distance_miles'] = df.apply(
                        lambda x: haversine_miles(lat_c, lon_c, x['latitude'], x['longitude']), axis=1
                    )
                    mask &= (df['distance_miles'] <= radius)
                else:
                    st.error("Could not find that address.")

        # --- SORTING ---
        sort_options = ['id'] + [c for c in df.columns if c not in ('id',)]
        sort_col = st.selectbox("Sort by", sort_options, index=0, key="sort_col")
        sort_order = st.radio("Order", ["Ascending", "Descending"], horizontal=True, key="sort_order")

        # --- DISPLAY ---
        df_filtered = df[mask].copy()
        df_filtered = df_filtered.sort_values(sort_col, ascending=(sort_order == "Ascending"))

        # Add select column
        df_filtered.insert(0, "Select", False)

        st.subheader(f"Showing {len(df_filtered)} / {len(df)} Records")

        # Column ordering for leases
        if view_type == "Lease Comps":
            cols = list(df_filtered.columns)
            priority = ['Select', 'address', 'rate_monthly', 'rate_annually', 'leased_sf', 'tenant_name']
            cols = priority + [c for c in cols if c not in priority]
            df_filtered = df_filtered[cols]

        # Column config for formatting
        col_config = {"Select": st.column_config.CheckboxColumn(required=True)}
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

        # Pagination
        PAGE_SIZE = 100
        total_pages = max(1, math.ceil(len(df_filtered) / PAGE_SIZE))
        if 'page_num' not in st.session_state:
            st.session_state.page_num = 1
        st.session_state.page_num = min(st.session_state.page_num, total_pages)

        if total_pages > 1:
            col1, col2, col3 = st.columns([1, 2, 1])
            with col1:
                if st.button("Previous") and st.session_state.page_num > 1:
                    st.session_state.page_num -= 1
            with col2:
                st.markdown(f"**Page {st.session_state.page_num} of {total_pages}**")
            with col3:
                if st.button("Next") and st.session_state.page_num < total_pages:
                    st.session_state.page_num += 1

        start_idx = (st.session_state.page_num - 1) * PAGE_SIZE
        df_page = df_filtered.iloc[start_idx:start_idx + PAGE_SIZE]

        edited_view = st.data_editor(
            df_page,
            hide_index=True,
            column_config=col_config,
            width="stretch",
        )

        # --- EXPORTS ---
        selected_rows = edited_view[edited_view["Select"] == True]
        export_df = selected_rows if not selected_rows.empty else None
        export_label = f"Selected ({len(selected_rows)})" if export_df is not None else ""

        # Always show "Export All Filtered" + show selected if any
        st.markdown("---")
        st.subheader("Export")

        if not selected_rows.empty:
            st.success(f"{len(selected_rows)} properties selected — exporting selection.")
        else:
            st.info(f"No rows selected — use checkboxes above to select specific properties, or export all {len(df_filtered)} filtered results below.")
            export_df = df_filtered
            export_label = f"All Filtered ({len(df_filtered)})"

        exp1, exp2, exp3 = st.columns(3)
        with exp1:
            kml_data = generate_kml(export_df)
            st.download_button(
                label=f"KML for Google Earth — {export_label}",
                data=kml_data,
                file_name="comps_export.kml",
                mime="application/vnd.google-earth.kml+xml",
            )
        with exp2:
            clean_export = export_df.drop(columns=['Select'], errors='ignore')
            excel_data = to_excel_bytes(clean_export)
            st.download_button(
                label=f"Excel — {export_label}",
                data=excel_data,
                file_name="comps_export.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )
        with exp3:
            clean_export = export_df.drop(columns=['Select'], errors='ignore')
            csv_data = clean_export.to_csv(index=False)
            st.download_button(
                label=f"CSV — {export_label}",
                data=csv_data,
                file_name="comps_export.csv",
                mime="text/csv",
            )

        # --- FOLIUM MAP ---
        map_df = df_filtered[df_filtered['latitude'].notnull() & df_filtered['longitude'].notnull()]
        if not map_df.empty:
            center_lat = map_df['latitude'].mean()
            center_lon = map_df['longitude'].mean()
            m = folium.Map(location=[center_lat, center_lon], zoom_start=11)

            for _, row in map_df.iterrows():
                color = 'green' if view_type == "Sales Comps" else 'blue'
                if view_type == "Sales Comps":
                    popup_text = f"<b>{row.get('address', 'N/A')}</b><br>Price: ${row.get('sale_price', 0):,.0f}<br>Size: {row.get('building_size', 0):,.0f} SF"
                else:
                    popup_text = f"<b>{row.get('address', 'N/A')}</b><br>Rate: ${row.get('rate_monthly', 0):.2f}/SF/Mo<br>Size: {row.get('leased_sf', 0):,.0f} SF"

                folium.Marker(
                    location=[row['latitude'], row['longitude']],
                    popup=folium.Popup(popup_text, max_width=300),
                    icon=folium.Icon(color=color, icon='home', prefix='fa'),
                ).add_to(m)

            # Draw radius circle if searching
            if center_addr and 'lat_c' in dir() and lat_c:
                folium.Circle(
                    location=[lat_c, lon_c],
                    radius=radius * 1609.34,  # miles to meters
                    color='red',
                    fill=True,
                    fill_opacity=0.1,
                ).add_to(m)

            st_folium(m, height=500, width="stretch")

        # --- ADMIN: DELETE ---
        if user_role == "admin":
            st.sidebar.markdown("---")
            st.sidebar.markdown("**Admin Actions**")
            confirm_delete = st.sidebar.checkbox("I want to delete ALL data", key="confirm_delete")
            if confirm_delete:
                if st.sidebar.button("Confirm: Clear All Data"):
                    session = Session()
                    session.query(SaleComp).delete()
                    session.query(LeaseComp).delete()
                    session.commit()
                    session.close()
                    st.rerun()

# =====================================================================
# PAGE 3: ANALYTICS
# =====================================================================
elif page == "Analytics":
    st.header("Analytics Dashboard")

    session = Session()
    sales_df = pd.read_sql(session.query(SaleComp).statement, session.bind)
    leases_df = pd.read_sql(session.query(LeaseComp).statement, session.bind)
    session.close()

    # --- SUMMARY METRICS ---
    st.subheader("Portfolio Summary")
    m1, m2, m3, m4 = st.columns(4)
    m1.metric("Total Sale Comps", len(sales_df))
    m2.metric("Total Lease Comps", len(leases_df))

    if not sales_df.empty and 'sale_price' in sales_df.columns:
        avg_price = sales_df['sale_price'].dropna().mean()
        avg_psf = sales_df['price_per_sf'].dropna().mean()
        m3.metric("Avg Sale Price", f"${avg_price:,.0f}" if pd.notna(avg_price) else "N/A")
        m4.metric("Avg $/SF (Sales)", f"${avg_psf:,.2f}" if pd.notna(avg_psf) else "N/A")
    else:
        m3.metric("Avg Sale Price", "N/A")
        m4.metric("Avg $/SF (Sales)", "N/A")

    if not leases_df.empty:
        m5, m6, m7, m8 = st.columns(4)
        avg_monthly = leases_df['rate_monthly'].dropna().mean()
        avg_annual = leases_df['rate_annually'].dropna().mean()
        avg_sf = leases_df['leased_sf'].dropna().mean()
        avg_ti = leases_df['ti_allowance'].dropna().mean()
        m5.metric("Avg Rate ($/SF/Mo)", f"${avg_monthly:.2f}" if pd.notna(avg_monthly) else "N/A")
        m6.metric("Avg Rate ($/SF/Yr)", f"${avg_annual:.2f}" if pd.notna(avg_annual) else "N/A")
        m7.metric("Avg Leased SF", f"{avg_sf:,.0f}" if pd.notna(avg_sf) else "N/A")
        m8.metric("Avg TI Allowance", f"${avg_ti:.2f}" if pd.notna(avg_ti) else "N/A")

    st.markdown("---")

    # --- CHARTS ---
    tab1, tab2 = st.tabs(["Sales Analysis", "Lease Analysis"])

    with tab1:
        if not sales_df.empty:
            col1, col2 = st.columns(2)
            with col1:
                price_data = sales_df['sale_price'].dropna()
                if not price_data.empty:
                    fig = px.histogram(price_data, nbins=20, title="Sale Price Distribution",
                                       labels={'value': 'Sale Price ($)', 'count': 'Count'})
                    fig.update_layout(showlegend=False)
                    st.plotly_chart(fig)

            with col2:
                psf_data = sales_df['price_per_sf'].dropna()
                if not psf_data.empty:
                    fig = px.histogram(psf_data, nbins=20, title="$/SF Distribution",
                                       labels={'value': '$/SF', 'count': 'Count'})
                    fig.update_layout(showlegend=False)
                    st.plotly_chart(fig)

            size_data = sales_df['building_size'].dropna()
            if not size_data.empty:
                fig = px.histogram(size_data, nbins=20, title="Building Size Distribution",
                                   labels={'value': 'Building Size (SF)', 'count': 'Count'})
                fig.update_layout(showlegend=False)
                st.plotly_chart(fig)
        else:
            st.info("No sales data to analyze.")

    with tab2:
        if not leases_df.empty:
            col1, col2 = st.columns(2)
            with col1:
                rate_data = leases_df['rate_monthly'].dropna()
                if not rate_data.empty:
                    fig = px.histogram(rate_data, nbins=20, title="Monthly Rate Distribution ($/SF/Mo)",
                                       labels={'value': '$/SF/Mo', 'count': 'Count'})
                    fig.update_layout(showlegend=False)
                    st.plotly_chart(fig)

            with col2:
                sf_data = leases_df['leased_sf'].dropna()
                if not sf_data.empty:
                    fig = px.histogram(sf_data, nbins=20, title="Leased SF Distribution",
                                       labels={'value': 'Leased SF', 'count': 'Count'})
                    fig.update_layout(showlegend=False)
                    st.plotly_chart(fig)

            ti_data = leases_df['ti_allowance'].dropna()
            if not ti_data.empty:
                fig = px.histogram(ti_data, nbins=15, title="TI Allowance Distribution",
                                   labels={'value': 'TI Allowance ($)', 'count': 'Count'})
                fig.update_layout(showlegend=False)
                st.plotly_chart(fig)
        else:
            st.info("No lease data to analyze.")

    # --- PROPERTY COMPARISON ---
    st.markdown("---")
    st.subheader("Property Comparison")
    compare_type = st.radio("Compare", ["Sales", "Leases"], horizontal=True, key="compare_type")

    if compare_type == "Sales" and not sales_df.empty:
        options = sales_df.apply(
            lambda r: f"{r['id']}: {r.get('address', 'N/A')} - ${r.get('sale_price', 0):,.0f}", axis=1
        ).tolist()
        selected = st.multiselect("Select properties to compare (2-4)", options, max_selections=4)
        if len(selected) >= 2:
            ids = [int(s.split(":")[0]) for s in selected]
            compare_df = sales_df[sales_df['id'].isin(ids)].set_index('address').T
            st.dataframe(compare_df, width="stretch")
    elif compare_type == "Leases" and not leases_df.empty:
        options = leases_df.apply(
            lambda r: f"{r['id']}: {r.get('address', 'N/A')} - {r.get('tenant_name', 'N/A')}", axis=1
        ).tolist()
        selected = st.multiselect("Select properties to compare (2-4)", options, max_selections=4)
        if len(selected) >= 2:
            ids = [int(s.split(":")[0]) for s in selected]
            compare_df = leases_df[leases_df['id'].isin(ids)].set_index('address').T
            st.dataframe(compare_df, width="stretch")
    else:
        st.info("Add data to use the comparison tool.")
