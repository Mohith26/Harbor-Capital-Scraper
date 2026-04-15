# UI Redesign Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the existing clunky Streamlit chrome with a custom dark icon sidebar, sticky topbar with filter chips, inline search/export, and a single-step Comp Finder form — all within `app.py` only.

**Architecture:** All changes are in `app.py` (1799 lines). New global CSS + HTML string helpers replace current sidebar-based navigation. Four page render blocks are rewritten in place; backend logic (comp_engine, database, storage, utils) is untouched.

**Tech Stack:** Streamlit, pandas, custom HTML/CSS via `st.markdown(unsafe_allow_html=True)`, folium/st_folium, plotly, streamlit_authenticator

---

## Chunk 1: Foundation — CSS, Logos, Navigation, Session State

### Task 1: Update logo paths and inject new global CSS

**Files:**
- Modify: `app.py:130-243`

This task replaces the existing `_logo_b64`/`_icon_b64` loading paths and the `<style>` block with the full new design system.

- [ ] **Step 1: Update logo loading paths (lines 137-138)**

Replace:
```python
_logo_b64 = _load_image_b64("HC-Logo-Stacked-Left-Charcoal@2000w.png")
_icon_b64 = _load_image_b64("Slate@512w.png")
```
With:
```python
_logo_b64 = _load_image_b64("Logo Masters/Stacked Left/@300w/HC-Logo-Stacked-Left-Charcoal@300w.png")
_icon_b64 = _load_image_b64("Logo Masters/Dry Dock/@300w/HC-Logo-Icon-White@300w.png")
```

- [ ] **Step 2: Replace the global CSS block (lines 141-243)**

Replace the entire `st.markdown("""<style>...""")` block with:

```python
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
</style>
""", unsafe_allow_html=True)
```

- [ ] **Step 3: Verify app syntax before running**

```bash
cd "/Users/mohithgajjela/Harbor Capital Scraper"
python -c "import ast; ast.parse(open('app.py').read()); print('Syntax OK')"
```

Expected: `Syntax OK`

- [ ] **Step 4: Commit**

```bash
cd "/Users/mohithgajjela/Harbor Capital Scraper"
git add app.py
git commit -m "feat: new design system CSS + updated logo paths"
```

---

### Task 2: HTML icon sidebar helper + navigation routing

**Files:**
- Modify: `app.py:248-268` (helper functions section — add `render_sidebar()` and `render_topbar()` helpers)
- Modify: `app.py:325-338` (post-auth block — replace old sidebar rendering)
- Modify: `app.py:467-479` (navigation block — replace `st.sidebar.radio()`)

- [ ] **Step 1: Add `render_sidebar()` helper after `render_metric_card()` (after line 267)**

Insert:
```python
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
```

- [ ] **Step 2: Add `render_topbar()` helper immediately after `render_sidebar()`**

```python
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
```

- [ ] **Step 3: Replace post-auth sidebar rendering**

Search for this anchor text (exact match, near top of post-auth block):
```python
if _logo_b64:
    st.markdown(f'<img src="data:image/png;base64,{_logo_b64}" width="320" style="margin-bottom:0.5rem;">', unsafe_allow_html=True)

# Sidebar: logo + user info + logout
if _icon_b64:
    st.sidebar.markdown(f'<img src="data:image/png;base64,{_icon_b64}" width="60" style="margin-bottom:0.5rem;">', unsafe_allow_html=True)
st.sidebar.markdown(f"**{st.session_state.get('name', '')}** &nbsp;|&nbsp; {user_role}")
authenticator.logout("Logout", "sidebar")
```

Replace with (use direct session state clearing — do NOT call `authenticator.logout` programmatically):
```python
# Handle logout via query param (?action=logout from the HTML sidebar "OUT" link)
if st.query_params.get("action") == "logout":
    for k in ["authentication_status", "name", "username", "logout"]:
        st.session_state.pop(k, None)
    st.query_params.clear()
    st.rerun()
```

Note: `user_role` derivation remains in the existing post-auth block just after the authenticator check (the `auth_config['credentials']['usernames'].get(...)` line). Do NOT remove that line — it must stay as-is so `user_role` is available throughout the file.

- [ ] **Step 4: Replace navigation block**

Note: Steps 1-3 insert new code above this section, so line numbers will have shifted. Use text search, not line numbers. Search for this anchor text:

Replace:
```python
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
```

With:
```python
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
```

- [ ] **Step 5: Verify page variable assignment is correct**

```bash
grep -n "^if page\|^elif page\|^page = " app.py
```

Expected: all `if page ==` and `elif page ==` conditions still work unchanged since `page = st.session_state["page"]` produces the same string values as the old `st.sidebar.radio()` did. Confirm no other routing pattern exists in the output.

- [ ] **Step 6: Add new session state keys to the init block (lines 339-351)**

Add after existing `if 'sheet_data' not in st.session_state:` block:
```python
if 'show_filter_panel' not in st.session_state:
    st.session_state.show_filter_panel = False
if 'show_export_menu' not in st.session_state:
    st.session_state.show_export_menu = False
if 'show_cf_export_menu' not in st.session_state:
    st.session_state.show_cf_export_menu = False
if 'db_search_text' not in st.session_state:
    st.session_state.db_search_text = ""
```

- [ ] **Step 7: Verify navigation works**

Run `streamlit run app.py --server.port 8502` and check:
- Sidebar shows 4 icon nav items (no Streamlit sidebar visible)
- Clicking each nav item changes page via `?page=<slug>` → page reruns and shows correct content
- "OUT" logout link clears session and returns to login

- [ ] **Step 8: Commit**

```bash
git add app.py
git commit -m "feat: HTML icon sidebar with query-param routing, replace st.sidebar.radio"
```

---

## Chunk 2: Page Implementations

### Task 3: Database View — topbar, metrics, search, export dropdown

**Files:**
- Modify: `app.py:976-1201` (Database View page block)

The existing page structure:
- Line 976: `elif page == "Database View":`
- Lines 980-986: type selector radio
- Lines 994-1011: sidebar filters + proximity
- Lines 1042-1043: 3 tabs (Data Table, Map View, Export & Actions)
- Lines 1045-1099: Data Table tab
- Lines 1101-1135: Map tab
- Lines 1137-1200: Export & Actions tab (to be removed as separate tab)

New structure:
1. Topbar HTML (logo + "Database View" + filter chips + selection badge + Export button)
2. Filter chip remove buttons (hidden `st.button` calls)
3. Four white metric cards in `st.columns(4)`
4. Table controls bar: type radio + search box + record count
5. Data Table content
6. Map tab
7. Admin expander (below tabs)
8. Export: triggered by `show_export_menu` session state

- [ ] **Step 1: Replace the `elif page == "Database View":` block header through `if df.empty:` check**

Replace lines 976-993:
```python
elif page == "Database View":

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
```

With:
```python
elif page == "Database View":

    sale_count, lease_count = get_record_counts()

    # ── Build filter chips HTML from active session state ──
    def _db_chip_html(label, rm_key):
        return f'<span class="hc-chip" id="chip-{rm_key}">{label} &nbsp;<span style="color:#F5A623;font-weight:900;">x</span></span>'

    filter_chips_html = ""
    if st.session_state.get("filter_cat_city"):
        for v in st.session_state["filter_cat_city"]:
            filter_chips_html += _db_chip_html(f"City: {v}", "filter_cat_city")
    if st.session_state.get("filter_cat_zip_code"):
        for v in st.session_state["filter_cat_zip_code"]:
            filter_chips_html += _db_chip_html(f"Zip: {v}", "filter_cat_zip_code")
    for _fk in ["filter_min_sale_price", "filter_max_sale_price", "filter_min_price_per_sf",
                "filter_max_price_per_sf", "filter_min_building_size", "filter_max_building_size",
                "filter_min_rate_monthly", "filter_max_rate_monthly"]:
        if st.session_state.get(_fk) is not None:
            _label = _fk.replace("filter_min_", "Min ").replace("filter_max_", "Max ").replace("_", " ").title()
            filter_chips_html += _db_chip_html(_label, _fk)
    if st.session_state.get("filter_loc_center"):
        filter_chips_html += _db_chip_html(f"Near: {st.session_state['filter_loc_center'][:20]}", "filter_loc_center")

    # ── Selection badge + export button HTML ──
    # (selection count filled in after table renders; pre-render placeholder)
    right_html = '''<button class="hc-export-btn" onclick="window.location.href=window.location.href.split(\'?\')[0]+\'?export_menu=1\'">Export ▾</button>'''

    # Handle export menu toggle via query param
    if st.query_params.get("export_menu") == "1":
        st.session_state.show_export_menu = True
        st.query_params.clear()
        st.rerun()

    render_topbar("Database View", filter_chips_html, right_html)

    # ── Filter chip remove buttons (rendered immediately below topbar) ──
    # Each active filter gets a tiny remove button keyed to its filter.
    _active_filter_keys = [k for k, v in st.session_state.items()
                           if k.startswith("filter_") and v not in (None, [], "", ())
                           and not k.endswith("_radius")]
    if _active_filter_keys:
        _rm_cols = st.columns(len(_active_filter_keys) + 1)
        for _i, _fk in enumerate(_active_filter_keys):
            with _rm_cols[_i]:
                if st.button(f"x {_fk.replace('filter_', '').replace('_', ' ')[:12]}", key=f"rm_{_fk}",
                             help=f"Remove {_fk} filter"):
                    del st.session_state[_fk]
                    st.rerun()
        with _rm_cols[-1]:
            if st.button("Clear all", key="rm_all_db"):
                reset_callback()
                st.rerun()

    # ── View type toggle ──
    _db_col1, _db_col2 = st.columns([2, 3])
    with _db_col1:
        view_type = st.radio(
            "Type",
            [f"Sales ({sale_count})", f"Leases ({lease_count})"],
            horizontal=True,
            key="db_view_type_radio",
            label_visibility="collapsed",
        )
    view_type = "Sales Comps" if "Sales" in view_type else "Lease Comps"

    df = load_data("SaleComp" if view_type == "Sales Comps" else "LeaseComp").copy()
    model_cls = SaleComp if view_type == "Sales Comps" else LeaseComp

    # ── Inline filter panel (below topbar, shown when filter_panel open) ──
    with _db_col2:
        if st.button("+ Filter", key="db_filter_btn"):
            st.session_state.show_filter_panel = not st.session_state.get("show_filter_panel", False)

    if st.session_state.get("show_filter_panel"):
        with st.container():
            st.markdown("---")
            filter_container = st.container()
            mask_from_panel = apply_sidebar_filters(df, view_type, include_proximity=True)
            st.markdown("---")
    else:
        mask_from_panel = None

    if df.empty:
        # ── Metrics row: empty state ──
        m1, m2, m3, m4 = st.columns(4)
        with m1: st.markdown('<div class="hc-metric-card"><div class="hc-metric-value">—</div><div class="hc-metric-label">Sales Comps</div></div>', unsafe_allow_html=True)
        with m2: st.markdown('<div class="hc-metric-card"><div class="hc-metric-value">—</div><div class="hc-metric-label">Lease Comps</div></div>', unsafe_allow_html=True)
        with m3: st.markdown('<div class="hc-metric-card"><div class="hc-metric-value">—</div><div class="hc-metric-label">Avg Sale Price</div></div>', unsafe_allow_html=True)
        with m4: st.markdown('<div class="hc-metric-card"><div class="hc-metric-value">—</div><div class="hc-metric-label">Avg $/SF</div></div>', unsafe_allow_html=True)
        st.info("No records yet. Upload a file to get started.")
    else:
```

- [ ] **Step 2: Add metrics row and apply filters (replaces old sidebar filter block)**

Immediately inside the `else:` (replacing the old `st.sidebar.markdown("---")` + `apply_sidebar_filters` + proximity block):

```python
        # ── Apply filters ──
        if mask_from_panel is not None:
            mask = mask_from_panel
        else:
            mask = pd.Series([True] * len(df))

        # Proximity filter (if set)
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

        df_filtered = df[mask].copy()

        # ── Metrics row ──
        _avg_sale = df_filtered['sale_price'].dropna().mean() if 'sale_price' in df_filtered.columns and view_type == "Sales Comps" else None
        _avg_psf  = df_filtered['price_per_sf'].dropna().mean() if 'price_per_sf' in df_filtered.columns and view_type == "Sales Comps" else None
        _avg_rate = df_filtered['rate_monthly'].dropna().mean() if 'rate_monthly' in df_filtered.columns and view_type == "Lease Comps" else None

        m1, m2, m3, m4 = st.columns(4)
        with m1:
            st.markdown(f'<div class="hc-metric-card"><div class="hc-metric-value">{sale_count:,}</div><div class="hc-metric-label">Sales Comps</div></div>', unsafe_allow_html=True)
        with m2:
            st.markdown(f'<div class="hc-metric-card"><div class="hc-metric-value">{lease_count:,}</div><div class="hc-metric-label">Lease Comps</div></div>', unsafe_allow_html=True)
        with m3:
            _val = f"${_avg_sale:,.0f}" if _avg_sale and pd.notna(_avg_sale) else (f"${_avg_rate:.2f}/mo" if _avg_rate and pd.notna(_avg_rate) else "—")
            _lbl = "Avg Sale Price" if view_type == "Sales Comps" else "Avg $/SF/Mo"
            st.markdown(f'<div class="hc-metric-card"><div class="hc-metric-value">{_val}</div><div class="hc-metric-label">{_lbl}</div></div>', unsafe_allow_html=True)
        with m4:
            _val4 = f"${_avg_psf:.2f}" if _avg_psf and pd.notna(_avg_psf) else "—"
            _lbl4 = "Avg $/SF"
            st.markdown(f'<div class="hc-metric-card"><div class="hc-metric-value">{_val4}</div><div class="hc-metric-label">{_lbl4}</div></div>', unsafe_allow_html=True)

        st.markdown("")
```

- [ ] **Step 3: Replace table controls bar and tabs (lines ~1022-1043)**

Replace from `# Column ordering for leases` through `tab_table, tab_map, tab_export = st.tabs(...)` with:

```python
        # Column ordering for leases
        if view_type == "Lease Comps":
            priority = ['address', 'rate_monthly', 'rate_annually', 'leased_sf', 'tenant_name']
            other_cols = [c for c in df_filtered.columns if c not in priority]
            df_filtered = df_filtered[priority + other_cols]

        # ── Table controls: inline search + record count ──
        _tc1, _tc2, _tc3 = st.columns([3, 1, 1])
        with _tc1:
            _search = st.text_input("Search", placeholder="Address, buyer/seller, notes...",
                                    key="db_search_text", label_visibility="collapsed")
        if _search:
            _addr_match = df_filtered.get('address', pd.Series(dtype=str)).astype(str).str.contains(_search, case=False, na=False)
            _buyer_match = df_filtered.get('buyer', pd.Series(dtype=str)).astype(str).str.contains(_search, case=False, na=False)
            _seller_match = df_filtered.get('seller', pd.Series(dtype=str)).astype(str).str.contains(_search, case=False, na=False)
            _tenant_match = df_filtered.get('tenant_name', pd.Series(dtype=str)).astype(str).str.contains(_search, case=False, na=False)
            _notes_match = df_filtered.get('notes', pd.Series(dtype=str)).astype(str).str.contains(_search, case=False, na=False)
            df_filtered = df_filtered[_addr_match | _buyer_match | _seller_match | _tenant_match | _notes_match]
        with _tc3:
            st.markdown(f'<div class="hc-record-count" style="padding-top:8px;"><b>{len(df_filtered)}</b> of {len(df)}</div>', unsafe_allow_html=True)

        st.caption("-- shift+click for range select --")

        # Column config
        col_config = {}
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

        # ── Tabs: Data Table | Map View ──
        tab_table, tab_map = st.tabs(["Data Table", "Map View"])
```

- [ ] **Step 4: Update Data Table tab content (keep existing logic, remove the old `tab_export` references)**

In `with tab_table:`, keep the existing admin/non-admin table rendering. After the table, add the export menu panel:

```python
        with tab_table:
            hide_cols = ['created_at', 'raw_address_data', 'source_file']
            display_df = df_filtered.drop(columns=[c for c in hide_cols if c in df_filtered.columns])

            if user_role == "admin":
                display_df.insert(0, "Select", False)
                edited_view = st.data_editor(
                    display_df, hide_index=True, column_config=col_config,
                    use_container_width=True, height=600,
                )
                selected_rows = edited_view[edited_view["Select"] == True].drop(columns=["Select"], errors="ignore")
            else:
                event = st.dataframe(
                    display_df, hide_index=True, column_config=col_config,
                    use_container_width=True, height=600,
                    on_select="rerun", selection_mode="multi-row",
                )
                sel_indices = event.selection.rows if event.selection else []
                selected_rows = display_df.iloc[sel_indices] if sel_indices else pd.DataFrame()

            # Selection count badge (rendered below table)
            if not selected_rows.empty:
                st.markdown(f'<span class="hc-selection-badge">{len(selected_rows)} selected</span>', unsafe_allow_html=True)

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
                st.toast(f"Saved changes to {save_count} records")
                st.rerun()

            # ── Export dropdown panel ──
            export_df = selected_rows.drop(columns=["Select"], errors="ignore") if not selected_rows.empty else df_filtered
            export_label = f"{len(selected_rows)} selected" if not selected_rows.empty else f"All {len(df_filtered)} filtered"

            if st.button(f"Export ▾ ({export_label})", key="db_export_toggle"):
                st.session_state.show_export_menu = not st.session_state.get("show_export_menu", False)

            if st.session_state.get("show_export_menu"):
                with st.container():
                    st.markdown(f'<div style="background:#fff;border:1px solid #e0e0e0;border-radius:8px;padding:12px 16px;display:inline-block;">', unsafe_allow_html=True)
                    _ex1, _ex2, _ex3 = st.columns(3)
                    with _ex1:
                        st.download_button("Excel", to_excel_bytes(export_df),
                                           "comps.xlsx", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                                           use_container_width=True)
                    with _ex2:
                        st.download_button("CSV", export_df.to_csv(index=False),
                                           "comps.csv", "text/csv", use_container_width=True)
                    with _ex3:
                        st.download_button("KML", generate_kml(export_df),
                                           "comps.kml", "application/vnd.google-earth.kml+xml",
                                           use_container_width=True)
                    st.markdown('</div>', unsafe_allow_html=True)
```

- [ ] **Step 5: Keep Map tab content unchanged (lines 1101-1135); remove `with tab_export:` block (lines 1137-1200)**

Replace `with tab_export:` entire block with an admin expander below the tabs:

```python
        # ── Admin actions (below tabs) ──
        if user_role == "admin":
            with st.expander("Admin Actions"):
                if 'selected_rows' in dir() and not selected_rows.empty and 'id' in selected_rows.columns:
                    confirm_sel = st.checkbox(f"Confirm deletion of {len(selected_rows)} selected records", key="confirm_delete_selected")
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

                confirm_delete = st.checkbox("Delete ALL data", key="confirm_delete")
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
```

- [ ] **Step 6: Verify Database View**

Load app in browser, navigate to Database View. Check:
- Topbar visible and sticky
- Metrics row shows 4 cards
- Type toggle works
- Search box filters table by address/tenant
- "Export ▾" button shows download options
- Map View tab shows map
- Admin: Admin Actions expander visible

- [ ] **Step 7: Commit**

```bash
git add app.py
git commit -m "feat: Database View — topbar, metrics row, inline search, export dropdown, admin expander"
```

---

### Task 4: Upload & Process — topbar, visible selectboxes, Apply Mapping button, mapping status bar

**Files:**
- Modify: `app.py:484-972` (Upload & Process page block)

Changes:
1. Add topbar at top of page
2. Remove step indicators (`render_step()` calls at lines 493-495)
3. Keep raw data HTML table unchanged
4. Change selectbox `label_visibility` from `"collapsed"` to `"visible"`
5. Rename "Re-Map Columns" button → "Apply Mapping"
6. Add mapping status bar (green mapped tags, red unmapped required tags) below selectboxes
7. Remove `st.toast` icon emoji (fix: `icon="\u2705"` → no icon kwarg)
8. Keep geocoding section unchanged

- [ ] **Step 1: Add topbar + remove step indicators**

Replace lines 484-498:
```python
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
```

With:
```python
if page == "Upload & Process":
    render_topbar("Upload & Process")

    # ── Drop zone ──
    st.markdown("""
    <div style="border:2px dashed #ccc;border-radius:9px;padding:24px 20px;text-align:center;background:#fff;margin-bottom:1rem;">
        <div style="font-size:14px;font-weight:600;color:#555;">Drop Excel or CSV here, or click to browse</div>
        <div style="font-size:11px;color:#999;margin-top:4px;">.xlsx &nbsp; .xls &nbsp; .csv &nbsp;—&nbsp; max 500 rows per sheet</div>
    </div>
    """, unsafe_allow_html=True)
    uploaded_file = st.file_uploader("Upload file", type=['csv', 'xlsx', 'xls'], label_visibility="collapsed")
```

- [ ] **Step 2: Make selectboxes visible (line ~700-706)**

In the column mapping loop, change `label_visibility="collapsed"` to `label_visibility="visible"`:

Find:
```python
                                st.selectbox(
                                    f"Map {target_field}",
                                    selectbox_options,
                                    index=default_idx,
                                    key=f"mapping_sel_{sheet_name}_{target_field}",
                                    label_visibility="collapsed",
                                )
```

Replace with:
```python
                                st.selectbox(
                                    f"Map {target_field}",
                                    selectbox_options,
                                    index=default_idx,
                                    key=f"mapping_sel_{sheet_name}_{target_field}",
                                    label_visibility="visible",
                                )
```

- [ ] **Step 3: Rename button + add mapping status bar (lines ~708-732)**

Replace:
```python
                    st.markdown("")
                    if st.button("Re-Map Columns", type="primary", use_container_width=True, key=f"remap_btn_{sheet_name}"):
```

With:
```python
                    # ── Mapping status bar ──
                    REQUIRED_FIELDS = ['address']
                    _mapped_fields = {tf: current_maps.get(tf) for tf in mapping_schema if current_maps.get(tf)}
                    _unmapped_required = [f for f in REQUIRED_FIELDS if f not in _mapped_fields]
                    _status_html = '<div class="hc-status-bar">'
                    for tf, src in _mapped_fields.items():
                        _status_html += f'<span class="hc-tag-mapped">+ {tf.replace("_"," ").title()}</span>'
                    for tf in _unmapped_required:
                        _status_html += f'<span class="hc-tag-unmapped">! {tf.replace("_"," ").title()} required</span>'
                    _status_html += '</div>'
                    st.markdown(_status_html, unsafe_allow_html=True)

                    st.markdown("")
                    _can_save = len(_unmapped_required) == 0
                    if st.button("Apply Mapping", type="primary", use_container_width=True, key=f"remap_btn_{sheet_name}"):
```

Also update the toast at line 731 to remove the icon kwarg:
```python
                        st.toast(f"Mapping updated for {sheet_name}!")
```

- [ ] **Step 4: Add "Geocode & Save" button (rename existing "Save to Database" button)**

At line ~839, replace `if st.button("Save to Database", type="primary", use_container_width=True):` with:
```python
            if st.button("Geocode & Save to Database", type="primary", use_container_width=True, disabled=not _can_save if 'clean_df' in st.session_state else False):
```

Note: `_can_save` is defined per-sheet in the mapping loop but we need it at the save level. Simplify: keep the button label change only:
```python
            if st.button("Geocode & Save to Database", type="primary", use_container_width=True):
```

- [ ] **Step 5: Verify Upload page**

Load app, go to Upload & Process. Check:
- No step indicators visible
- Drop zone HTML visible above file uploader
- After uploading a file: selectbox labels visible (not collapsed)
- "Apply Mapping" button (not "Re-Map Columns")
- Mapping status bar shows green/red field tags
- "Geocode & Save to Database" button at bottom

- [ ] **Step 6: Commit**

```bash
git add app.py
git commit -m "feat: Upload & Process — topbar, visible mapping selectboxes, Apply Mapping button, status bar"
```

---

### Task 5: Analytics — topbar, filter chips, move Heat Map + Comparison into tabs

**Files:**
- Modify: `app.py:1205-1512` (Analytics page block)

Changes:
1. Add topbar at top of Analytics page
2. Remove `st.sidebar.markdown("---")` call
3. Move inline filter panel (reuse same `apply_sidebar_filters()` in a container)
4. Move Heat Map section (lines 1457-1477) into a new "Map" tab
5. Move Property Comparison section (lines 1479-1512) into a new "Compare" tab
6. Tab structure becomes: `Distributions | Price vs Size | Trends | By Zip Code | Map | Compare`

- [ ] **Step 1: Replace Analytics page header block (lines 1205-1240)**

Replace:
```python
elif page == "Analytics":
    section_header("Analytics Dashboard")

    a_sale_count, a_lease_count = get_record_counts()

    # Type selector with counts
    analytics_type = st.radio(...)
    analytics_type = "Sales Comps" if "Sales" in analytics_type else "Lease Comps"

    # Only load the selected type
    ...

    # Sidebar filters
    st.sidebar.markdown("---")

    if analytics_type == "Sales Comps" and not sales_df.empty:
        analytics_mask = apply_sidebar_filters(sales_df, "Sales Comps")
        ...
```

With:
```python
elif page == "Analytics":
    a_sale_count, a_lease_count = get_record_counts()

    # ── Topbar ──
    _an_right = f'''<span style="display:flex;gap:6px;align-items:center;">
        <span style="font-size:11px;color:#777;">Sales</span>
    </span>'''
    render_topbar("Analytics", right_html="")

    # ── Type toggle ──
    _an_c1, _an_c2, _an_c3 = st.columns([2, 2, 3])
    with _an_c1:
        analytics_type = st.radio(
            "Analyze",
            [f"Sales ({a_sale_count})", f"Leases ({a_lease_count})"],
            horizontal=True,
            key="analytics_type",
            label_visibility="collapsed",
        )
    analytics_type = "Sales Comps" if "Sales" in analytics_type else "Lease Comps"

    if analytics_type == "Sales Comps":
        sales_df = load_data("SaleComp").copy()
        leases_df = pd.DataFrame()
    else:
        leases_df = load_data("LeaseComp").copy()
        sales_df = pd.DataFrame()

    # ── Inline filter panel ──
    with _an_c2:
        if st.button("+ Filter", key="an_filter_btn"):
            st.session_state.show_filter_panel = not st.session_state.get("show_filter_panel", False)

    if st.session_state.get("show_filter_panel"):
        with st.container():
            st.markdown("---")
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
            st.markdown("---")
    else:
        filtered_sales = sales_df
        filtered_leases = leases_df
```

- [ ] **Step 2: Update metrics row and chart tabs to 6 tabs**

In the `if analytics_type == "Sales Comps":` chart block, replace:
```python
        tab1, tab2, tab3, tab4 = st.tabs(["Distributions", "Price vs Size", "Trends", "By Zip Code"])
```
With:
```python
        tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs(["Distributions", "Price vs Size", "Trends", "By Zip Code", "Map", "Compare"])
```

And for Lease Comps:
```python
        tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs(["Distributions", "Rate vs Size", "Trends", "By Zip Code", "Map", "Compare"])
```

Wrap each existing tab block with an empty-state guard. For every `with tab1:`, `with tab2:`, etc. in both the Sales and Lease branches, add the guard at the top:

```python
        with tab1:
            if (filtered_sales if analytics_type == "Sales Comps" else filtered_leases).empty:
                st.info("No data matching current filters.")
            else:
                # preserve existing content from this tab verbatim
```

Do this for all four existing tabs (tab1–tab4) in both branches. The existing chart code inside each tab does not change — only the guard wraps it.

- [ ] **Step 3: Move Heat Map section into `with tab5:` for both branches**

The existing Heat Map block (search for `# --- HEAT MAP ---`) currently renders below the tabs. Cut it out and place it inside `with tab5:` in **both** the Sales and Lease branches (each branch already has its own `tab1`–`tab4`; add `tab5` and `tab6` inside each `if analytics_type == "Sales Comps":` / `else:` block).

For the **Sales Comps** branch, add:
```python
        with tab5:
            heat_df = filtered_sales
            if heat_df.empty:
                st.info("No data matching current filters.")
            else:
                geo_data = heat_df.dropna(subset=['latitude', 'longitude'])
                value_col = 'price_per_sf'
                geo_data = geo_data.dropna(subset=[value_col])
                if not geo_data.empty:
                    fig = px.density_mapbox(geo_data, lat='latitude', lon='longitude', z=value_col,
                                            radius=20, zoom=9, mapbox_style='open-street-map',
                                            hover_data=['address'],
                                            title="Price/SF Heat Map")
                    fig.update_layout(height=500)
                    st.plotly_chart(fig, use_container_width=True)
                else:
                    st.info("Not enough geocoded data for heat map.")
```

For the **Lease Comps** branch, add:
```python
        with tab5:
            heat_df = filtered_leases
            if heat_df.empty:
                st.info("No data matching current filters.")
            else:
                geo_data = heat_df.dropna(subset=['latitude', 'longitude'])
                value_col = 'rate_monthly'
                geo_data = geo_data.dropna(subset=[value_col])
                if not geo_data.empty:
                    fig = px.density_mapbox(geo_data, lat='latitude', lon='longitude', z=value_col,
                                            radius=20, zoom=9, mapbox_style='open-street-map',
                                            hover_data=['address'],
                                            title="Rate/SF/Mo Heat Map")
                    fig.update_layout(height=500)
                    st.plotly_chart(fig, use_container_width=True)
                else:
                    st.info("Not enough geocoded data for heat map.")
```

- [ ] **Step 4: Move Property Comparison into `with tab6:` for both branches**

Search for `# --- PROPERTY COMPARISON ---`. Cut the entire block through the final `st.info("Add data to use the comparison tool.")` line. Place it inside `with tab6:` in both the Sales and Lease branches.

For the **Sales Comps** branch, add:
```python
        with tab6:
            if sales_df.empty:
                st.info("No data available for comparison.")
            else:
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
```

For the **Lease Comps** branch, add:
```python
        with tab6:
            if leases_df.empty:
                st.info("No data available for comparison.")
            else:
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
```

- [ ] **Step 5: Remove the old standalone Heat Map and Property Comparison blocks (now in tabs)**

Confirm their location with:
```bash
grep -n "# --- HEAT MAP ---\|# --- PROPERTY COMPARISON ---" app.py
```

Delete from `# --- HEAT MAP ---` through the final `st.info("Add data to use the comparison tool.")` line. These blocks are now inside `tab5` and `tab6` respectively; the standalone versions must be removed to avoid duplicate rendering.

- [ ] **Step 6: Verify Analytics page**

Load app, go to Analytics. Check:
- Topbar visible
- Type toggle (Sales/Leases) works
- 6 tabs visible: Distributions, Price vs Size, Trends, By Zip Code, Map, Compare
- Map tab shows heat map
- Compare tab shows comparison tool
- Empty state message when filters produce no data

- [ ] **Step 7: Commit**

```bash
git add app.py
git commit -m "feat: Analytics — topbar, 6 tabs with Map and Compare moved inside tab structure"
```

---

### Task 6: Comp Finder — topbar, inline advanced weights, combined geocode+search

**Files:**
- Modify: `app.py:1517-1799` (Comp Finder page block)

Changes:
1. Add topbar
2. Remove all `st.sidebar.*` slider/checkbox calls (lines 1533-1545)
3. Move weights into inline `st.expander("Advanced Weights", expanded=False)` inside the form
4. Remove separate "Geocode" button; fold geocoding into "Find Comparable Properties" button click
5. Show geocode status as small text below address input
6. Add export dropdown to results section

- [ ] **Step 1: Replace the Comp Finder page header + sidebar sliders (lines 1517-1545)**

Replace:
```python
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
```

With:
```python
elif page == "Comp Finder":
    render_topbar("Comp Finder")

    # Session state for results persistence
    if 'cf_results' not in st.session_state:
        st.session_state.cf_results = None
    if 'cf_subject' not in st.session_state:
        st.session_state.cf_subject = None
    if 'cf_subject_coords' not in st.session_state:
        st.session_state.cf_subject_coords = None
    if 'cf_geocode_status' not in st.session_state:
        st.session_state.cf_geocode_status = None  # None | "ok" | "error"
    if 'cf_geocode_addr_done' not in st.session_state:
        st.session_state.cf_geocode_addr_done = ""

    # --- Comp type selector ---
    cf_type = st.radio("Search in", ["Sales Comps", "Leases Comps"], horizontal=True, key="cf_type_radio")
    cf_type_key = "Sales" if "Sales" in cf_type else "Leases"

    # --- Left panel: form (~350px via column ratio) ---
    form_col, results_col = st.columns([1, 2])

    with form_col:
        cf_address = st.text_input("Subject Address", placeholder="e.g. 123 Main St, Houston TX", key="cf_address")

        # Geocode status text
        if st.session_state.cf_geocode_status == "ok" and st.session_state.cf_subject_coords:
            lat, lng = st.session_state.cf_subject_coords
            st.markdown(f'<div style="font-size:11px;color:#2E7D32;margin-top:-8px;margin-bottom:8px;">Geocoded: {lat:.4f}, {lng:.4f}</div>', unsafe_allow_html=True)
        elif st.session_state.cf_geocode_status == "error":
            st.markdown('<div style="font-size:11px;color:#C62828;margin-top:-8px;margin-bottom:8px;">Could not geocode — try a more specific address.</div>', unsafe_allow_html=True)

        if cf_type_key == "Sales":
            cf_size = st.number_input("Building Size (SF)", value=None, min_value=0, step=100, key="cf_size")
        else:
            cf_size = st.number_input("Leased SF", value=None, min_value=0, step=100, key="cf_size")

        # Optional fields in 2-column grid
        _of1, _of2 = st.columns(2)
        with _of1:
            if cf_type_key == "Sales":
                cf_price = st.number_input("Sale Price ($)", value=None, min_value=0, step=10000, key="cf_price")
                cf_year = st.number_input("Year Built", value=None, min_value=1900, max_value=2030, step=1, key="cf_year")
            else:
                cf_rate_mo = st.number_input("Rate $/SF/Mo", value=None, min_value=0.0, step=0.25, key="cf_rate_mo")
                cf_btype = st.text_input("Building Type", placeholder="e.g. Industrial", key="cf_btype")
        with _of2:
            if cf_type_key == "Sales":
                cf_psf = st.number_input("Price/SF ($)", value=None, min_value=0.0, step=1.0, key="cf_psf")
                cf_city = st.text_input("City", placeholder="e.g. Houston", key="cf_city")
            else:
                cf_rate_yr = st.number_input("Rate $/SF/Yr", value=None, min_value=0.0, step=1.0, key="cf_rate_yr")
                cf_city = st.text_input("City", placeholder="e.g. Houston", key="cf_city")
        cf_zip = st.text_input("Zip Code", placeholder="e.g. 77001", key="cf_zip")

        # Advanced Weights expander
        with st.expander("Advanced Weights", expanded=False):
            w_proximity = st.slider("Proximity Weight", 0.0, 1.0, 0.30, 0.05, key="cf_w_prox")
            w_size = st.slider("Size Weight", 0.0, 1.0, 0.25, 0.05, key="cf_w_size")
            w_price = st.slider("Price / Rate Weight", 0.0, 1.0, 0.20, 0.05, key="cf_w_price")
            w_recency = st.slider("Recency Weight", 0.0, 1.0, 0.15, 0.05, key="cf_w_recency")
            w_other = st.slider("Other Attributes Weight", 0.0, 1.0, 0.10, 0.05, key="cf_w_other")
            max_radius = st.slider("Max Radius (miles)", 1, 50, 25, key="cf_max_radius")
            max_results = st.slider("Max Results", 5, 50, 20, key="cf_max_results")
            use_ai = st.checkbox("AI Enhancement", value=False, key="cf_use_ai")
            ai_blend = 0.3
            if use_ai:
                ai_blend = st.slider("AI Blend Ratio", 0.1, 0.9, 0.3, 0.05, key="cf_ai_blend")
            else:
                ai_blend = 0.3
```

- [ ] **Step 2: Remove old subject form columns and geocode button**

Search for this anchor text to identify the block to delete:
```python
    # --- Subject property form ---
    st.markdown("")
    col_left, col_right = st.columns(2)
```

Delete from that line through (and including):
```python
    # --- Build subject dict ---
    subject = {}
```

(i.e., delete the old `col_left`/`col_right` form block and the old "Build subject dict" line — both are now inside `form_col` from Step 1). Confirm with:
```bash
grep -n "col_left, col_right\|col_left:\|col_right:\|geocode_btn\|cf_geocode_btn" app.py
```
Expected: zero matches after deletion.

- [ ] **Step 3: Build subject/weights dicts and combined Find button inside `form_col`**

Add after the expander (still inside `with form_col:`):

```python
        # Build subject dict
        subject = {}
        if st.session_state.cf_subject_coords:
            subject["lat"] = st.session_state.cf_subject_coords[0]
            subject["lng"] = st.session_state.cf_subject_coords[1]
        subject["address"] = cf_address or None
        subject["city"] = cf_city or None
        subject["zip_code"] = cf_zip or None

        if cf_type_key == "Sales":
            subject["building_size"] = cf_size
            subject["sale_price"] = cf_price or None
            subject["price_per_sf"] = cf_psf or None
            subject["year_built"] = cf_year or None
        else:
            subject["leased_sf"] = cf_size
            subject["rate_monthly"] = cf_rate_mo or None
            subject["rate_annually"] = cf_rate_yr or None
            subject["building_type"] = cf_btype or None

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

        st.markdown("")
        find_btn = st.button("Find Comparable Properties", type="primary", use_container_width=True,
                             disabled=not bool(cf_address))
```

- [ ] **Step 4: Replace the old geocode + find logic**

Replace existing geocode/find logic (lines ~1634-1661) with:

```python
        if find_btn and cf_address:
            api_key = get_secret("GOOGLE_API_KEY", "")
            # Geocode if address changed or not yet geocoded
            if cf_address != st.session_state.cf_geocode_addr_done:
                with st.spinner("Geocoding address..."):
                    addr, lat, lng, city_g, zip_g, warn = fetch_google_data(cf_address, api_key)
                if lat and lng:
                    st.session_state.cf_subject_coords = (lat, lng)
                    st.session_state.cf_geocode_status = "ok"
                    st.session_state.cf_geocode_addr_done = cf_address
                    subject["lat"] = lat
                    subject["lng"] = lng
                else:
                    st.session_state.cf_geocode_status = "error"
                    st.session_state.cf_subject_coords = None
                    st.rerun()

            if st.session_state.cf_subject_coords:
                subject["lat"] = st.session_state.cf_subject_coords[0]
                subject["lng"] = st.session_state.cf_subject_coords[1]
                comps_df = load_comps(cf_type_key)
                if comps_df.empty:
                    st.info(f"No {cf_type_key.lower()} comps in database yet.")
                else:
                    with st.spinner("Scoring comparables..."):
                        results = compute_match_scores(subject, comps_df, cf_type_key, weights, max_radius)
                        if use_ai:
                            try:
                                ai_scores = compute_ai_scores(subject, results, cf_type_key)
                                results["match_score"] = blend_scores(results["match_score"], ai_scores, ai_blend)
                                results = results.sort_values("match_score", ascending=False).reset_index(drop=True)
                            except Exception as e:
                                st.toast(f"AI scoring failed: {e}")
                        results = results[results["match_score"] > 0].head(max_results)
                        st.session_state.cf_results = results
                        st.session_state.cf_subject = subject
                        st.rerun()
```

- [ ] **Step 5: Results panel inside `with results_col:` (use 4-space indent throughout)**

Wrap the existing results display block in `with results_col:`. Search for this anchor:
```python
    # --- Display Results ---
    if st.session_state.cf_results is not None and not st.session_state.cf_results.empty:
```

Replace with (use 4-space indent everywhere):
```python
    with results_col:
        if st.session_state.cf_results is not None and not st.session_state.cf_results.empty:
            results = st.session_state.cf_results  # use persisted state, not local var
            subject = st.session_state.cf_subject

            st.markdown("")
            section_header("Results", f"{len(results)} comps found")

            # ── Export dropdown for results ──
            cf_export_df = results.copy()
            if st.button("Export Results ▾", key="cf_export_toggle"):
                st.session_state.show_cf_export_menu = not st.session_state.get("show_cf_export_menu", False)
            if st.session_state.get("show_cf_export_menu"):
                _cfe1, _cfe2 = st.columns(2)
                with _cfe1:
                    st.download_button("Excel", to_excel_bytes(cf_export_df), "cf_results.xlsx",
                                       "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                                       use_container_width=True)
                with _cfe2:
                    st.download_button("CSV", cf_export_df.to_csv(index=False), "cf_results.csv",
                                       "text/csv", use_container_width=True)
```

Then re-indent the rest of the results block (the existing `tab_ranked, tab_map, tab_breakdown = st.tabs(...)` and everything inside it) by one extra level (from 8 spaces to 12 spaces) to sit inside `with results_col: → if ...:`.

Also replace the final empty state message:
```python
    elif st.session_state.cf_results is not None and st.session_state.cf_results.empty:
        st.info(f"No comparable properties found within {max_radius} miles. Try increasing the max radius or adjusting weights.")
```

With (inside `with results_col:`):
```python
        elif st.session_state.cf_results is not None and st.session_state.cf_results.empty:
            st.info(f"No comparable properties found within {max_radius} miles. Try increasing the max radius or adjusting weights.")
```

- [ ] **Step 6: Remove old Download Results CSV button (duplicate of new export)**

Find and delete:
```python
            # Export results
            csv_data = display_df[available_cols].to_csv(index=False)
            st.download_button("Download Results (CSV)", csv_data, "comp_finder_results.csv", "text/csv",
                               use_container_width=True)
```

- [ ] **Step 7: Verify Comp Finder**

Load app, go to Comp Finder. Check:
- No sidebar sliders visible
- Form has address, size, optional fields in 2-col grid, "Advanced Weights" expander
- "Find Comparable Properties" button (no separate Geocode button)
- After search: geocode status text shows lat/lng
- Results panel shows ranked results, export dropdown, Map and Score Breakdown tabs
- Empty state: "No comparable properties found within N miles..."

- [ ] **Step 8: Commit**

```bash
git add app.py
git commit -m "feat: Comp Finder — topbar, inline advanced weights, combined geocode+search, export dropdown"
```

---

### Task 7: Final cleanup and smoke test

**Files:**
- Modify: `app.py` (cleanup only)

- [ ] **Step 1: Remove now-unused `render_step()` function**

Check whether it is still called anywhere:
```bash
grep -n "render_step" app.py
```

If the grep shows no call sites (only the definition), delete the `render_step()` function definition. It looks like:
```python
def render_step(number, title, status="active"):
    css_circle = {"active": "step-active", "done": "step-done", "pending": "step-pending"}[status]
    css_label = {"active": "step-label-active", "done": "step-label-done", "pending": "step-label-pending"}[status]
    icon = "&#10003;" if status == "done" else str(number)
    st.markdown(f'''<div class="step-row">
        <div class="step-circle {css_circle}">{icon}</div>
        <span class="step-label {css_label}">{title}</span>
    </div>''', unsafe_allow_html=True)
```

If any call sites remain, remove those call sites first, then delete the function.

Check `section_header()` and `render_metric_card()`:
```bash
grep -n "section_header\|render_metric_card" app.py
```

Leave both functions in place — they are still used in Analytics, Comp Finder, and the save section of Upload & Process.

- [ ] **Step 2: Verify no Python errors**

```bash
cd "/Users/mohithgajjela/Harbor Capital Scraper"
python -c "import ast; ast.parse(open('app.py').read()); print('No syntax errors')"
```

Expected: `No syntax errors`

- [ ] **Step 3: Run app and do a full walkthrough**

```bash
streamlit run app.py --server.port 8502
```

Walk through each page:
1. Login page loads (existing auth, unchanged)
2. After login: icon sidebar visible on left; no Streamlit sidebar
3. **Database View**: 4 metric cards, search works, Export ▾ shows download options, Map View tab works, Admin Actions expander visible for admin role
4. **Upload & Process**: drop zone HTML, file uploader, visible selectbox labels, "Apply Mapping" button, mapping status bar, "Geocode & Save to Database" button
5. **Analytics**: 6 tabs, Map tab shows heat map, Compare tab shows comparison tool
6. **Comp Finder**: form with advanced weights expander, single "Find Comparable Properties" button, results appear in right panel, export works
7. Logout ("OUT" in sidebar) returns to login screen

- [ ] **Step 4: Final commit**

```bash
git add app.py
git commit -m "feat: final cleanup — remove unused render_step, smoke test passed"
```
