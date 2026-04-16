# Harbor Capital Comp Database -- Platform Migration Spec
**Date:** 2026-04-16
**Status:** Approved for implementation

---

## Overview

Migrate the Harbor Capital Comp Database from Streamlit to FastAPI + Jinja2 + HTMX + Alpine.js. The current Streamlit implementation suffers from fundamental platform limitations: full-script reruns on every interaction, HTML sanitization that strips event handlers, no real URL routing, and fragile session state. The new stack gives full HTML/CSS/JS control while keeping the entire backend in Python.

**Scope:** Replace `app.py` (Streamlit UI layer) with a FastAPI web application. All business logic modules are unchanged: `engine/`, `learning/`, `comp_finder.py`, `database.py`, `storage.py`, `utils.py`.

**Deployment:** Railway (same). Dockerfile updated to run `uvicorn` instead of `streamlit run`.

---

## Design Decisions

### Stack

| Layer | Choice | Rationale |
|-------|--------|-----------|
| Web framework | FastAPI | Async Python, Starlette routing, Jinja2 built-in, same language as existing code |
| Templates | Jinja2 | Full HTML control, no sanitization, server-side rendering |
| Interactivity | HTMX 2.x | Server-driven partial page updates via HTML-over-the-wire. No JS build step |
| Client state | Alpine.js 3.x | Lightweight reactive JS for dropdowns, modals, toggles. Inline in HTML attributes |
| Styling | Tailwind CSS 3.x (CDN) | Utility-first CSS. No build step. Matches brand design system |
| Data table | AG Grid Community 32.x | Multi-row select, column sort/filter, shift-click range, CSV/Excel export |
| Charts | Plotly.js 2.x | Same charts as current Streamlit app, rendered client-side |
| Maps | Leaflet.js 1.9 | Lighter than folium, direct marker/popup/heatmap control |
| Auth | Session cookies + bcrypt | Replace streamlit_authenticator. Same user YAML config, httponly cookies |
| File upload | Native HTML5 + FastAPI `UploadFile` | Drag-and-drop, progress bar, no Streamlit widget limitations |

All JS libraries loaded via CDN. No npm, no node_modules, no build step.

### Visual Style (unchanged from UI redesign spec)

- **Layout:** Dark icon sidebar (58px, `#333333`) + white topbar per page + light content area (`#f4f5f7`)
- **Brand colors:** `#333333` charcoal, `#F5A623` amber, `#FFF3DC` amber-pale
- **Typography:** `-apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif`
- **Border radius:** 8-9px on cards/panels, 6-7px on inputs/buttons, 12px on chips
- **No emoji anywhere**

### Logo Assets

- **Sidebar icon:** `Logo Masters/Dry Dock/@300w/HC-Logo-Icon-White@300w.png` -- base64-encoded at startup
- **Topbar logo:** `Logo Masters/Stacked Left/@300w/HC-Logo-Stacked-Left-Charcoal@300w.png` -- base64-encoded at startup

---

## Architecture

### File Structure

```
Harbor Capital Scraper/
├── web/                        # NEW: FastAPI web application
│   ├── main.py                 # FastAPI app factory, lifespan, middleware
│   ├── auth.py                 # Login/logout, session management, role checks
│   ├── config.py               # Settings from env vars (replaces st.secrets)
│   ├── dependencies.py         # FastAPI dependency injection (db session, current user, learning store)
│   ├── routes/
│   │   ├── __init__.py
│   │   ├── database.py         # /database — Database View page + HTMX endpoints
│   │   ├── upload.py           # /upload — Upload & Process page + HTMX endpoints
│   │   ├── analytics.py        # /analytics — Analytics page + HTMX endpoints
│   │   └── comp_finder.py      # /finder — Comp Finder page + HTMX endpoints
│   ├── templates/
│   │   ├── base.html           # Shared layout: sidebar + topbar + content slot
│   │   ├── login.html          # Login page
│   │   ├── database.html       # Database View full page
│   │   ├── upload.html         # Upload & Process full page
│   │   ├── analytics.html      # Analytics full page
│   │   ├── comp_finder.html    # Comp Finder full page
│   │   └── partials/           # HTMX fragment templates
│   │       ├── metrics_row.html
│   │       ├── data_table.html
│   │       ├── filter_chips.html
│   │       ├── filter_panel.html
│   │       ├── export_menu.html
│   │       ├── upload_preview.html
│   │       ├── mapping_grid.html
│   │       ├── mapping_status.html
│   │       ├── processing_status.html
│   │       ├── chart_tab.html
│   │       ├── comp_results.html
│   │       └── comp_map.html
│   └── static/
│       ├── css/
│       │   └── app.css         # Brand colors, sidebar, topbar, cards, chips, custom overrides
│       └── js/
│           └── app.js          # AG Grid init, Plotly chart helpers, Leaflet map helpers, Alpine components
├── engine/                     # UNCHANGED
├── learning/                   # UNCHANGED
├── comp_finder.py              # UNCHANGED
├── database.py                 # UNCHANGED (add get_session helper for FastAPI DI)
├── storage.py                  # UNCHANGED
├── utils.py                    # UNCHANGED
├── auth_config.yaml            # UNCHANGED (user credentials)
├── Dockerfile                  # UPDATED: uvicorn instead of streamlit
├── start.sh                    # UPDATED: uvicorn command
├── requirements.txt            # UPDATED: remove streamlit deps, add fastapi/uvicorn/etc.
└── app.py                      # DEPRECATED: kept for reference, not run
```

### Request Flow

```
Browser GET /database
  → FastAPI router (web/routes/database.py)
  → Auth middleware checks session cookie
  → Route handler queries DB via SQLAlchemy session
  → Renders templates/database.html via Jinja2
  → Returns full HTML page (sidebar + topbar + content)

Browser HTMX GET /database/table?type=sales&search=houston
  → FastAPI router
  → Auth middleware
  → Handler queries DB, filters, returns templates/partials/data_table.html
  → HTMX swaps just the table div (no full page reload)
```

---

## Authentication

### Current (streamlit_authenticator)

- `auth_config.yaml` stores usernames, bcrypt-hashed passwords, roles
- Cookie-based auto-login (30-day expiry)
- Two roles: `admin` (can edit/delete) and `analyst` (read-only + upload)

### New (FastAPI session auth)

- Same `auth_config.yaml` format and credential storage
- Login: POST `/login` with username/password → server verifies bcrypt hash → sets signed session cookie (httponly, samesite=lax, 30-day maxage)
- Session data stored server-side in a dict (adequate for <10 concurrent users). Key: random token. Value: `{username, role, login_time}`.
- Middleware: every request except `/login` and `/static/*` checks session cookie. Invalid/expired → redirect to `/login`.
- Logout: GET `/logout` → clear session + cookie → redirect to `/login`.
- Role check: `require_admin` dependency raises 403 for non-admin users on delete/edit endpoints.

---

## Page-by-Page Design

### Shared Layout (base.html)

Every page extends `base.html` which renders:

**Fixed sidebar (58px wide, `#333333`, full viewport height):**
- HC icon logo at top
- Nav items: DB, UPLOAD, STATS, FINDER — each is a real `<a href="/database">` link with SVG icon + label
- Active item: amber text + left border (determined by `current_page` template variable)
- Bottom: user initials circle + role label + logout link

**Fixed topbar (top: 0, left: 58px, right: 0, z-index: 100, white background):**
- Left: HC charcoal logo → divider → page title (passed as block variable)
- Center: filter chips slot (optional, filled by child template)
- Right: action buttons slot (optional, filled by child template)

**Content area:** Below topbar, left of sidebar. `padding-top: 57px; padding-left: 74px;` to clear fixed elements.

CDN includes in `<head>`: Tailwind CSS, HTMX, Alpine.js, AG Grid, Plotly.js, Leaflet.js + CSS.

---

### 1. Database View (`/database`)

**Route:** `GET /database` — full page render
**HTMX endpoints:**
- `GET /database/table` — returns table partial (params: type, search, page, sort, filters)
- `GET /database/metrics` — returns metrics row partial (params: type, filters)
- `POST /database/filters` — add filter, returns updated chips + table
- `DELETE /database/filters/{key}` — remove filter, returns updated chips + table
- `GET /database/filter-panel` — returns filter panel partial (show/hide)
- `GET /database/export` — returns file download (params: format=xlsx|csv|kml, selected_ids)
- `DELETE /database/records` — admin: delete selected records
- `GET /database/map` — returns map partial

**Layout:**

Type toggle (Sales/Leases) — radio buttons with HTMX: `hx-get="/database/table?type=sales"` + `hx-get="/database/metrics?type=sales"`, triggers on change, targets `#table-container` and `#metrics-container`.

Metrics row: four cards (Sales Comps count, Lease Comps count, Avg Sale Price / Avg Rate, Avg $/SF). White cards with amber left-border. Empty state: "--" values.

Search input: `hx-get="/database/table"` with `hx-trigger="keyup changed delay:300ms"` and `hx-include="[name='type'],[name='search']"`. Server-side `pandas.str.contains(search, case=False)` on address, buyer/seller/tenant, notes.

Filter button: `+ Filter` opens filter panel below topbar via HTMX (`hx-get="/database/filter-panel"` → `hx-swap="innerHTML"` on `#filter-panel`). Panel contains the same filter widgets as current app (categorical multiselect for city/zip, numeric range for price/size/rate, location radius). Each filter submission → `POST /database/filters` → server stores in URL query params or form data → returns updated chips + table.

Filter chips in topbar: rendered server-side from active filter dict. Each chip has `x` button: `hx-delete="/database/filters/filter_cat_city"` → returns updated chips + table.

Data table: AG Grid Community initialized in `app.js`. Data passed as JSON from server in a `<script>` tag embedded in the table partial. Columns match current schema. Features: multi-row select (shift+click), column sort, column resize. Selection state tracked in JS, passed to export/delete endpoints.

Record count: "N of M" label above table, updated with each HTMX table swap.

Tabs: "Data Table" | "Map View" — Alpine.js `x-show` toggle (no server round-trip for tab switch). Map tab renders Leaflet map with markers from current filtered data (passed as GeoJSON in template).

Export dropdown: Alpine.js `x-show` on click. Three `<a>` links: Excel, CSV, KML. Each is `href="/database/export?format=xlsx"` with selected IDs appended as query params via JS. If no selection, exports all filtered rows.

Admin actions: `x-show="userRole === 'admin'"` expander below table. Delete Selected button: `hx-delete="/database/records"` with confirmation modal (Alpine.js).

---

### 2. Upload & Process (`/upload`)

**Route:** `GET /upload` — full page render
**HTMX endpoints:**
- `POST /upload/file` — receive uploaded file, return preview partial
- `POST /upload/mapping` — apply mapping changes, return updated preview
- `POST /upload/geocode` — run geocoding, return progress updates via SSE
- `POST /upload/save` — save to database, return success/error message

**Layout:**

Drop zone: HTML5 drag-and-drop area (`<div>` with dragover/drop event handlers in Alpine.js). Text: "Drop Excel, CSV, or PDF here, or click to browse" / ".xlsx  .xls  .csv  .pdf -- max 500 rows per sheet". On file select/drop → `hx-post="/upload/file"` with `hx-encoding="multipart/form-data"`.

Server processing: receives file → calls `robust_load_file_segmented()` for Excel/CSV or `run_vision_pdf_stage()` for PDF → returns `upload_preview.html` partial containing:

**Raw Input Table:** Real HTML `<table>` (first 5 rows). Column headers show amber badges for mapped columns, grey "unmapped" for others. Each header cell includes the mapped target field name.

**Column Mapping Grid:** 2-column grid of `<select>` elements below the raw table. Each select labeled with target field name + confidence indicator (green dot >=0.7, amber 0.4-0.7, red <0.4). Options: all source columns + "-- Skip --". Pre-selected based on engine's `generate_standardized_df` output. On change → `hx-post="/upload/mapping"` with all select values → returns updated preview + raw table badges.

**Mapping Status Bar:** Green tags for mapped required fields, red tags for unmapped required fields. Required: `address`. Shows "Ready to geocode" or "Missing required: address".

**Multi-sheet support:** Tabs for each sheet/segment — Alpine.js `x-show` tabs, all data loaded at once (sheets are small, <500 rows each).

**Geocode & Save button:** `hx-post="/upload/geocode"` with `hx-ext="sse"` for streaming progress. Server geocodes each row, streams progress events (`data: {"done": 5, "total": 50, "current": "123 Main St"}`). On completion, auto-triggers save or shows save button.

**Save to Database:** `hx-post="/upload/save"` → inserts rows via SQLAlchemy → returns success message with count. Also calls `persist_with_learning()` to record mapping/geocoding corrections in the learning store.

---

### 3. Analytics (`/analytics`)

**Route:** `GET /analytics` — full page render
**HTMX endpoints:**
- `GET /analytics/charts` — returns chart data partial (params: type, tab, filters)
- `GET /analytics/metrics` — returns metrics partial

**Layout:**

Type toggle (Sales/Leases) in topbar right — same HTMX pattern as Database View.

Filter chips in topbar — same system as Database View (shared filter logic in `dependencies.py`).

Metrics row: four cards — Total Comps, Avg Sale Price / Avg Rate Monthly, Avg $/SF / Avg Rate Annually, Avg Size.

Chart tabs (Alpine.js client-side tab switching):
1. **Distributions** — Plotly histograms (price, size, $/SF)
2. **Price vs Size** — Plotly scatter plot
3. **Trends** — Plotly time series (price over closing_date)
4. **By Zip Code** — Plotly bar chart grouped by zip
5. **Map** — Leaflet heat map layer
6. **Compare** — Side-by-side property comparison (select 2-5 properties from multiselect)

Chart data: passed as JSON in `<script>` tags within the chart partial. Plotly.js renders client-side. Tab switching is Alpine.js `x-show` — all chart divs present in DOM, Plotly initializes on first show.

Empty state: "No data matching current filters" info box per tab.

---

### 4. Comp Finder (`/finder`)

**Route:** `GET /finder` — full page render
**HTMX endpoints:**
- `POST /finder/search` — run comp search, return results partial
- `GET /finder/export` — export results (params: format, result_ids)

**Layout:**

Two-column layout:

**Left panel (~350px, form):**
- Sales / Leases radio toggle
- Subject Address text input
- Optional fields in 2-column grid: Size, Sale Price / Rate, Price/SF, Year Built, City, Zip Code
- "Advanced Weights" collapsible section (Alpine.js `x-show`): proximity, size, price, recency sliders + max radius + max results + AI toggle
- "Find Comparable Properties" button: `hx-post="/finder/search"` with all form fields → targets `#results-container`

Server processing: geocodes address via `resolve_geocode()`, calls `load_comps()`, `compute_match_scores()`, optionally `compute_ai_scores()` + `blend_scores()`. Returns `comp_results.html` partial.

**Right panel (results):**
- Header: "N comps found" + Export dropdown (same Alpine.js pattern)
- Ranked cards: each card shows rank number (amber for top 3, grey for rest), address, meta line (size, price, date), match % with CSS progress bar
- Tabs below results (Alpine.js): "Map" (Leaflet with subject marker + comp markers) | "Score Breakdown" (Plotly radar chart or table showing dimension scores)

**Empty state:** "No comparable properties found within N miles. Try increasing the max radius or adjusting weights."

---

## Configuration

### Environment Variables

Same 5 env vars as current, read via `web/config.py` using `pydantic-settings` or `os.environ`:

| Variable | Purpose |
|----------|---------|
| `GOOGLE_API_KEY` | Google Maps Geocoding API |
| `SUPABASE_DB_URL` | PostgreSQL connection string |
| `SUPABASE_URL` | Supabase project URL (for storage) |
| `SUPABASE_KEY` | Supabase anon key (for storage) |
| `OPENAI_API_KEY` | OpenAI API for embeddings + vision |

No more `start.sh` writing to `.streamlit/secrets.toml`. FastAPI reads env vars directly.

### Dependencies Changes

**Remove:** streamlit, streamlit-folium, streamlit-authenticator

**Add:** fastapi, uvicorn[standard], python-multipart, jinja2, itsdangerous (cookie signing), aiofiles (static file serving)

**Keep:** pandas, sqlalchemy, openai, scipy, python-dateutil, requests, numpy, psycopg2-binary, openpyxl, plotly, statsmodels, xlsxwriter, pyyaml, bcrypt, supabase, pdf2image, pillow, rapidfuzz

---

## Deployment Changes

### Dockerfile

```dockerfile
FROM python:3.11-slim
RUN apt-get update && apt-get install -y --no-install-recommends poppler-utils && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY . .
EXPOSE 8501
CMD ["uvicorn", "web.main:app", "--host", "0.0.0.0", "--port", "8501"]
```

### start.sh (simplified)

```bash
#!/bin/bash
exec uvicorn web.main:app --host 0.0.0.0 --port ${PORT:-8501}
```

No secrets.toml generation needed. Env vars read directly by `web/config.py`.

---

## Migration Path

1. All new code goes in `web/` directory
2. `app.py` is kept but not executed (reference only)
3. Existing engine/learning/database/storage/utils modules are imported by `web/routes/*` with zero changes
4. `database.py` gets one addition: a `get_session()` generator for FastAPI dependency injection
5. Once `web/` is complete and verified, `Dockerfile` CMD changes from streamlit to uvicorn
6. `app.py` can be deleted in a follow-up cleanup

---

## What This Fixes

| Problem | Root Cause | Solution |
|---------|-----------|----------|
| Tab clicks open new page | Streamlit reruns entire script on query-param change | Real URL routing (`/database`, `/upload`, etc.) |
| Filters don't persist | Session state resets on full reruns | Server-side filter state in URL params, HTMX partial swaps |
| Export button shows as raw HTML | Streamlit strips onclick handlers | Real HTML buttons with Alpine.js click handlers |
| Sidebar nav colors wrong | CSS specificity battles with Streamlit's injected styles | Full CSS control, no Streamlit chrome to fight |
| Mapping UI cramped and confusing | Limited to st.selectbox widgets | Real HTML `<select>` elements in a 2-column grid with visual feedback |
| No live mapping preview | Streamlit requires full rerun to update | HTMX partial swap: change select → POST → get updated preview |
| Two-step geocode then search | Streamlit button limitations | Single form POST, server handles geocode + search |
| 2300-line app.py monolith | Streamlit forces single-file architecture | 5 focused route files + 12 templates |
| Topbar gap / fixed position hacks | Streamlit injects hidden elements with height | No Streamlit elements at all |
