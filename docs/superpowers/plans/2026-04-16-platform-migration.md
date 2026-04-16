# Platform Migration Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace Streamlit UI with FastAPI + Jinja2 + HTMX + Alpine.js while keeping all business logic modules unchanged.

**Architecture:** FastAPI serves Jinja2 templates with HTMX for partial page updates and Alpine.js for client-side state. AG Grid for tables, Plotly.js for charts, Leaflet.js for maps. All JS via CDN, no build step. Session auth replaces streamlit_authenticator.

**Tech Stack:** FastAPI, Jinja2, HTMX 2.x, Alpine.js 3.x, Tailwind CSS 3.x (CDN), AG Grid Community 32.x, Plotly.js 2.x, Leaflet.js 1.9, uvicorn

**Spec:** `docs/superpowers/specs/2026-04-16-platform-migration-design.md`

---

## File Structure

```
web/                          # NEW directory — all FastAPI code
  main.py                     # App factory, lifespan, middleware, static/template config
  config.py                   # Settings from env vars (replaces st.secrets)
  auth.py                     # Login/logout, session dict, cookie signing, role checks
  dependencies.py             # FastAPI Depends: get_db_session, get_current_user, get_learning_store
  routes/
    __init__.py               # Router aggregation
    database.py               # /database page + HTMX endpoints
    upload.py                 # /upload page + HTMX endpoints
    analytics.py              # /analytics page + HTMX endpoints
    comp_finder.py            # /finder page + HTMX endpoints
  templates/
    base.html                 # Shared layout: sidebar + topbar + content blocks
    login.html                # Login page
    database.html             # Database View full page
    upload.html               # Upload & Process full page
    analytics.html            # Analytics full page
    comp_finder.html          # Comp Finder full page
    partials/
      metrics_row.html        # Metric cards partial
      data_table.html         # AG Grid data + init script partial
      filter_chips.html       # Active filter chips
      filter_panel.html       # Filter form widgets
      export_menu.html        # Export dropdown options
      upload_preview.html     # Raw table + mapping grid + status bar
      mapping_status.html     # Mapped/unmapped field tags
      processing_status.html  # Geocoding progress
      chart_distributions.html
      chart_price_size.html
      chart_trends.html
      chart_zip.html
      chart_map.html
      chart_compare.html
      comp_results.html       # Ranked comp cards
      comp_map.html           # Leaflet map with comp markers
  static/
    css/app.css               # Brand colors, sidebar, topbar, cards, chips, form styles
    js/app.js                 # AG Grid init, Plotly helpers, Leaflet helpers, Alpine components, HTMX afterSwap handler
```

**Modified existing files:**
- `database.py` — add `get_session()` generator for FastAPI DI (3 lines)
- `requirements.txt` — swap streamlit deps for fastapi deps
- `Dockerfile` — change CMD to uvicorn
- `start.sh` — simplify to uvicorn command

**Unchanged:** `engine/*`, `learning/*`, `comp_finder.py`, `storage.py`, `utils.py`, `comp_engine.py`, `auth_config.yaml`

---

## Chunk 1: Foundation

### Task 1: FastAPI App Skeleton + Config + Auth

**Files:**
- Create: `web/__init__.py`
- Create: `web/config.py`
- Create: `web/auth.py`
- Create: `web/dependencies.py`
- Create: `web/main.py`
- Create: `web/routes/__init__.py`
- Modify: `database.py` (add `get_session` generator)
- Modify: `requirements.txt`

- [ ] **Step 1: Create `web/config.py`**

```python
"""Application settings from environment variables."""
import os

class Settings:
    GOOGLE_API_KEY: str = os.environ.get("GOOGLE_API_KEY", "")
    SUPABASE_DB_URL: str = os.environ.get("SUPABASE_DB_URL", "sqlite:///comps.db")
    SUPABASE_URL: str = os.environ.get("SUPABASE_URL", "")
    SUPABASE_KEY: str = os.environ.get("SUPABASE_KEY", "")
    OPENAI_API_KEY: str = os.environ.get("OPENAI_API_KEY", "")
    SECRET_KEY: str = os.environ.get("SECRET_KEY", "harbor-capital-dev-secret-change-me")
    SESSION_MAX_AGE: int = 30 * 24 * 3600  # 30 days in seconds

settings = Settings()
```

- [ ] **Step 2: Create `web/auth.py`**

Session-based auth reading `auth_config.yaml`. Stores sessions in a server-side dict. Signs session tokens with `itsdangerous`.

```python
"""Session-based authentication for FastAPI."""
import os
import secrets
import time
import yaml
import bcrypt
from typing import Optional
from itsdangerous import URLSafeTimedSerializer
from fastapi import Request, Response, HTTPException
from fastapi.responses import RedirectResponse
from web.config import settings

_serializer = URLSafeTimedSerializer(settings.SECRET_KEY)
_sessions: dict[str, dict] = {}  # token -> {username, name, role, login_time}

COOKIE_NAME = "harbor_session"
COOKIE_MAX_AGE = settings.SESSION_MAX_AGE

def _load_credentials() -> dict:
    """Load user credentials from auth_config.yaml."""
    config_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "auth_config.yaml")
    with open(config_path, "r") as f:
        config = yaml.safe_load(f)
    return config.get("credentials", {}).get("usernames", {})

def verify_password(username: str, password: str) -> Optional[dict]:
    """Verify username/password against auth_config.yaml. Returns user dict or None."""
    users = _load_credentials()
    user = users.get(username)
    if not user:
        return None
    stored_hash = user["password"].encode("utf-8")
    if bcrypt.checkpw(password.encode("utf-8"), stored_hash):
        return {"username": username, "name": user.get("name", username), "role": user.get("role", "analyst")}
    return None

def create_session(user_info: dict, response: Response) -> str:
    """Create a new session and set the cookie."""
    token = secrets.token_urlsafe(32)
    _sessions[token] = {**user_info, "login_time": time.time()}
    signed = _serializer.dumps(token)
    response.set_cookie(
        COOKIE_NAME, signed,
        max_age=COOKIE_MAX_AGE, httponly=True, samesite="lax",
    )
    return token

def get_session(request: Request) -> Optional[dict]:
    """Get current session from cookie. Returns user dict or None."""
    cookie = request.cookies.get(COOKIE_NAME)
    if not cookie:
        return None
    try:
        token = _serializer.loads(cookie, max_age=COOKIE_MAX_AGE)
    except Exception:
        return None
    session = _sessions.get(token)
    if not session:
        return None
    return session

def destroy_session(request: Request, response: Response):
    """Clear session and cookie."""
    cookie = request.cookies.get(COOKIE_NAME)
    if cookie:
        try:
            token = _serializer.loads(cookie, max_age=COOKIE_MAX_AGE)
            _sessions.pop(token, None)
        except Exception:
            pass
    response.delete_cookie(COOKIE_NAME)
```

- [ ] **Step 3: Create `web/dependencies.py`**

FastAPI dependency injection for DB sessions, current user, and learning store.

```python
"""FastAPI dependency injection."""
from typing import Generator
from fastapi import Request, HTTPException
from fastapi.responses import RedirectResponse
from database import Session as DBSession
from learning.store import SqliteLearningStore
from web.auth import get_session

def get_db() -> Generator:
    """Yield a SQLAlchemy session, auto-close on completion."""
    session = DBSession()
    try:
        yield session
    finally:
        session.close()

def get_current_user(request: Request) -> dict:
    """Get current authenticated user. Raises 401 if not logged in."""
    user = get_session(request)
    if not user:
        raise HTTPException(status_code=401, detail="Not authenticated")
    return user

def require_admin(request: Request) -> dict:
    """Require admin role. Raises 403 if not admin."""
    user = get_current_user(request)
    if user.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Admin access required")
    return user

_learning_store = None

def get_learning_store() -> SqliteLearningStore:
    """Singleton learning store instance."""
    global _learning_store
    if _learning_store is None:
        _learning_store = SqliteLearningStore()
    return _learning_store
```

- [ ] **Step 4: Create `web/routes/__init__.py`**

```python
"""Route aggregation."""
from fastapi import APIRouter
from web.routes.database import router as database_router
from web.routes.upload import router as upload_router
from web.routes.analytics import router as analytics_router
from web.routes.comp_finder import router as comp_finder_router

api_router = APIRouter()
api_router.include_router(database_router)
api_router.include_router(upload_router)
api_router.include_router(analytics_router)
api_router.include_router(comp_finder_router)
```

Create placeholder route files so imports work:

```python
# web/routes/database.py
from fastapi import APIRouter
router = APIRouter(prefix="/database", tags=["database"])

@router.get("")
async def database_page():
    return {"page": "database", "status": "placeholder"}
```

```python
# web/routes/upload.py
from fastapi import APIRouter
router = APIRouter(prefix="/upload", tags=["upload"])

@router.get("")
async def upload_page():
    return {"page": "upload", "status": "placeholder"}
```

```python
# web/routes/analytics.py
from fastapi import APIRouter
router = APIRouter(prefix="/analytics", tags=["analytics"])

@router.get("")
async def analytics_page():
    return {"page": "analytics", "status": "placeholder"}
```

```python
# web/routes/comp_finder.py
from fastapi import APIRouter
router = APIRouter(prefix="/finder", tags=["finder"])

@router.get("")
async def finder_page():
    return {"page": "finder", "status": "placeholder"}
```

- [ ] **Step 5: Create `web/main.py`**

```python
"""FastAPI application factory."""
import os
import base64
from contextlib import asynccontextmanager
from fastapi import FastAPI, Request
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from fastapi.responses import RedirectResponse
from starlette.middleware.base import BaseHTTPMiddleware
from web.config import settings
from web.auth import get_session as get_auth_session
from web.routes import api_router

# --- Logo loading (same base64 approach as Streamlit version) ---
def _load_image_b64(relative_path: str) -> str:
    """Load image file and return base64 string."""
    base = os.path.dirname(os.path.dirname(__file__))
    full = os.path.join(base, relative_path)
    try:
        with open(full, "rb") as f:
            return base64.b64encode(f.read()).decode()
    except Exception:
        return ""

# --- Auth middleware ---
class AuthMiddleware(BaseHTTPMiddleware):
    EXEMPT_PATHS = {"/login", "/static"}

    async def dispatch(self, request: Request, call_next):
        path = request.url.path
        if any(path.startswith(p) for p in self.EXEMPT_PATHS):
            return await call_next(request)
        user = get_auth_session(request)
        if not user:
            return RedirectResponse("/login", status_code=303)
        request.state.user = user
        return await call_next(request)

# --- App lifespan ---
@asynccontextmanager
async def lifespan(app: FastAPI):
    # Load logos at startup
    app.state.logo_b64 = _load_image_b64("Logo Masters/Stacked Left/@300w/HC-Logo-Stacked-Left-Charcoal@300w.png")
    app.state.icon_b64 = _load_image_b64("Logo Masters/Dry Dock/@300w/HC-Logo-Icon-White@300w.png")
    from database import ensure_tables
    ensure_tables()
    yield

# --- App factory ---
app = FastAPI(title="Harbor Capital Comp Database", lifespan=lifespan)
app.add_middleware(AuthMiddleware)

# Static files
static_dir = os.path.join(os.path.dirname(__file__), "static")
os.makedirs(static_dir, exist_ok=True)
app.mount("/static", StaticFiles(directory=static_dir), name="static")

# Templates
templates_dir = os.path.join(os.path.dirname(__file__), "templates")
os.makedirs(templates_dir, exist_ok=True)
templates = Jinja2Templates(directory=templates_dir)

# Make templates available to routes
app.state.templates = templates

# Include routes
app.include_router(api_router)

# Login routes (not in api_router because they're special)
from fastapi import Form
from fastapi.responses import HTMLResponse

@app.get("/login")
async def login_page(request: Request):
    return templates.TemplateResponse("login.html", {"request": request, "error": None})

@app.post("/login")
async def login_submit(request: Request, username: str = Form(...), password: str = Form(...)):
    from web.auth import verify_password, create_session
    user = verify_password(username, password)
    if not user:
        return templates.TemplateResponse("login.html", {"request": request, "error": "Invalid username or password"})
    response = RedirectResponse("/database", status_code=303)
    create_session(user, response)
    return response

@app.get("/logout")
async def logout(request: Request):
    from web.auth import destroy_session
    response = RedirectResponse("/login", status_code=303)
    destroy_session(request, response)
    return response

@app.get("/")
async def root():
    return RedirectResponse("/database", status_code=303)
```

- [ ] **Step 6: Create `web/__init__.py`**

```python
# Harbor Capital Comp Database — FastAPI web application
```

- [ ] **Step 7: Update `requirements.txt`**

Remove streamlit-specific deps, add FastAPI deps:

```
# --- Web framework ---
fastapi>=0.115.0
uvicorn[standard]>=0.34.0
python-multipart>=0.0.18
jinja2>=3.1.0
itsdangerous>=2.2.0
aiofiles>=24.0.0

# --- Data / DB ---
pandas>=2.1.0
sqlalchemy>=2.0.0
numpy>=1.26.0
psycopg2-binary>=2.9.9
openpyxl>=3.1.0
xlsxwriter>=3.1.0

# --- APIs ---
openai>=1.0.0
requests>=2.31.0
supabase>=2.0.0

# --- ML / Math ---
scipy>=1.12.0
statsmodels>=0.14.0
rapidfuzz>=3.0.0

# --- Auth ---
pyyaml>=6.0
bcrypt>=4.1.0

# --- Utilities ---
python-dateutil>=2.9.0
python-dotenv>=1.0.0

# --- PDF ---
pdf2image>=1.17.0
pillow>=10.0.0

# --- Testing ---
pytest>=8.0.0
pytest-mock>=3.12.0
responses>=0.25.0
httpx>=0.28.0
```

Note: `httpx` added for FastAPI test client. `streamlit`, `streamlit-folium`, `streamlit-authenticator`, `folium`, `plotly` removed (charts/maps now client-side JS).

- [ ] **Step 8: Verify the app starts**

```bash
cd "/Users/mohithgajjela/Harbor Capital Scraper"
pip install fastapi uvicorn[standard] python-multipart jinja2 itsdangerous aiofiles httpx
uvicorn web.main:app --port 8502 --reload &
sleep 2
curl -s -o /dev/null -w "%{http_code}" http://localhost:8502/login
# Expected: 200 (or 500 if templates missing — that's OK, we create them in Task 2)
kill %1
```

- [ ] **Step 9: Commit**

```bash
git add web/ requirements.txt
git commit -m "feat: FastAPI app skeleton with config, auth, dependencies, and route placeholders"
```

---

### Task 2: Base Template + Login Page + Static Assets

**Files:**
- Create: `web/templates/base.html`
- Create: `web/templates/login.html`
- Create: `web/static/css/app.css`
- Create: `web/static/js/app.js`

- [ ] **Step 1: Create `web/static/css/app.css`**

All brand styles: sidebar, topbar, cards, chips, forms, buttons, tables. See spec for exact colors (`#333333`, `#F5A623`, `#FFF3DC`, `#f4f5f7`).

Key CSS classes:
- `.hc-sidebar` — fixed left 58px, `#333333`, full height, z-index 200
- `.hc-nav-item` — sidebar nav link with SVG icon, hover/active states
- `.hc-topbar` — fixed top, left 58px, white, z-index 100, flexbox
- `.hc-topbar-logo` — 28px height
- `.hc-topbar-divider` — 1px vertical separator
- `.metric-card` — white card with amber left border
- `.filter-chip` — 12px border-radius, amber background, x button
- `.drop-zone` — dashed border upload area
- `.comp-card` — ranked comp result card
- `.btn-primary` — amber background button
- `.btn-outline` — bordered button

Font: `-apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif`. Border-radius: 8px cards, 6px inputs, 12px chips. No emoji.

- [ ] **Step 2: Create `web/static/js/app.js`**

Core JS module with:
- AG Grid initialization helper (`initGrid(containerId, columnDefs, rowData, onSelectionChanged)`)
- Plotly chart helpers (`renderHistogram(divId, data, layout)`, etc.)
- Leaflet map helper (`initMap(divId, center, zoom)`, `addMarkers(map, geojson)`)
- HTMX afterSwap handler for Alpine.js re-init: `document.addEventListener('htmx:afterSwap', e => { if (window.Alpine) Alpine.initTree(e.detail.target); })`
- Export helper: `async function exportData(url, format, ids)` — POSTs JSON body, triggers file download
- Type toggle helper: `function switchType(type, tableTarget, metricsTarget)` — fires two HTMX ajax calls

- [ ] **Step 3: Create `web/templates/base.html`**

Shared layout with CDN includes, sidebar, topbar, content area.

```html
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Harbor Capital Comp Database</title>
    <!-- Tailwind CSS -->
    <script src="https://cdn.tailwindcss.com"></script>
    <!-- HTMX -->
    <script src="https://unpkg.com/htmx.org@2.0.4"></script>
    <script src="https://unpkg.com/htmx-ext-sse@2.2.2/sse.js"></script>
    <!-- Alpine.js -->
    <script defer src="https://unpkg.com/alpinejs@3.14.8/dist/cdn.min.js"></script>
    <!-- AG Grid -->
    <script src="https://cdn.jsdelivr.net/npm/ag-grid-community@32.3.3/dist/ag-grid-community.min.js"></script>
    <!-- Plotly -->
    <script src="https://cdn.plot.ly/plotly-2.35.2.min.js"></script>
    <!-- Leaflet -->
    <link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css" />
    <script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
    <!-- App CSS & JS -->
    <link rel="stylesheet" href="/static/css/app.css">
    <script src="/static/js/app.js"></script>
</head>
<body class="bg-[#f4f5f7] font-sans">
    <!-- Sidebar -->
    <nav class="hc-sidebar">
        <img class="hc-sidebar-logo" src="data:image/png;base64,{{ icon_b64 }}" alt="HC">
        <a class="hc-nav-item {% if current_page == 'database' %}active{% endif %}" href="/database">
            <svg viewBox="0 0 24 24"><path d="M4 6h16v2H4zm0 5h16v2H4zm0 5h16v2H4z"/></svg>
            DB
        </a>
        <a class="hc-nav-item {% if current_page == 'upload' %}active{% endif %}" href="/upload">
            <svg viewBox="0 0 24 24"><path d="M9 16h6v-6h4l-7-7-7 7h4zm-4 2h14v2H5z"/></svg>
            UPLOAD
        </a>
        <a class="hc-nav-item {% if current_page == 'analytics' %}active{% endif %}" href="/analytics">
            <svg viewBox="0 0 24 24"><path d="M19 3H5c-1.1 0-2 .9-2 2v14c0 1.1.9 2 2 2h14c1.1 0 2-.9 2-2V5c0-1.1-.9-2-2-2zM9 17H7v-7h2v7zm4 0h-2V7h2v10zm4 0h-2v-4h2v4z"/></svg>
            STATS
        </a>
        <a class="hc-nav-item {% if current_page == 'finder' %}active{% endif %}" href="/finder">
            <svg viewBox="0 0 24 24"><path d="M12 2C8.13 2 5 5.13 5 9c0 5.25 7 13 7 13s7-7.75 7-13c0-3.87-3.13-7-7-7zm0 9.5c-1.38 0-2.5-1.12-2.5-2.5s1.12-2.5 2.5-2.5 2.5 1.12 2.5 2.5-1.12 2.5-2.5 2.5z"/></svg>
            FINDER
        </a>
        <div class="hc-nav-spacer"></div>
        <div class="hc-nav-user">{{ user.name[0]|upper }}<br>{{ user.role[:5]|upper }}</div>
        <a class="hc-nav-logout" href="/logout">OUT</a>
    </nav>

    <!-- Topbar -->
    <header class="hc-topbar">
        <img class="hc-topbar-logo" src="data:image/png;base64,{{ logo_b64 }}" alt="Harbor Capital">
        <div class="hc-topbar-divider"></div>
        <div class="hc-topbar-title">{% block page_title %}{% endblock %}</div>
        {% block topbar_center %}{% endblock %}
        <div class="ml-auto flex items-center gap-2">
            {% block topbar_right %}{% endblock %}
        </div>
    </header>

    <!-- Content -->
    <main class="hc-content">
        {% block content %}{% endblock %}
    </main>
</body>
</html>
```

- [ ] **Step 4: Create `web/templates/login.html`**

Standalone login page (does NOT extend base.html — no sidebar/topbar for unauthenticated users).

```html
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <title>Login - Harbor Capital</title>
    <script src="https://cdn.tailwindcss.com"></script>
    <link rel="stylesheet" href="/static/css/app.css">
</head>
<body class="bg-[#f4f5f7] min-h-screen flex items-center justify-center">
    <div class="bg-white rounded-lg shadow-md p-8 w-full max-w-sm">
        <div class="text-center mb-6">
            <img src="data:image/png;base64,{{ logo_b64 }}" alt="Harbor Capital" class="h-10 mx-auto mb-4">
            <h1 class="text-lg font-bold text-[#333333]">Comp Database</h1>
        </div>
        {% if error %}
        <div class="bg-red-50 text-red-700 px-4 py-2 rounded mb-4 text-sm">{{ error }}</div>
        {% endif %}
        <form method="post" action="/login">
            <label class="block text-sm font-medium text-gray-700 mb-1">Username</label>
            <input type="text" name="username" required
                   class="w-full px-3 py-2 border rounded-md mb-3 focus:outline-none focus:ring-2 focus:ring-[#F5A623]">
            <label class="block text-sm font-medium text-gray-700 mb-1">Password</label>
            <input type="password" name="password" required
                   class="w-full px-3 py-2 border rounded-md mb-4 focus:outline-none focus:ring-2 focus:ring-[#F5A623]">
            <button type="submit" class="btn-primary w-full">Sign In</button>
        </form>
    </div>
</body>
</html>
```

- [ ] **Step 5: Verify login flow works**

```bash
uvicorn web.main:app --port 8502 --reload &
sleep 2
# Should show login page
curl -s http://localhost:8502/login | grep "Sign In"
# Should redirect to login
curl -s -o /dev/null -w "%{http_code}" http://localhost:8502/database
# Expected: 303 redirect to /login
kill %1
```

- [ ] **Step 6: Commit**

```bash
git add web/templates/ web/static/
git commit -m "feat: base template with sidebar/topbar, login page, static assets (CSS + JS)"
```

---

## Chunk 2: Database View Page

### Task 3: Database View — Full Page + Table + Metrics

**Files:**
- Create: `web/templates/database.html`
- Create: `web/templates/partials/metrics_row.html`
- Create: `web/templates/partials/data_table.html`
- Modify: `web/routes/database.py`

- [ ] **Step 1: Implement `web/routes/database.py`**

Full Database View route with HTMX endpoints:

```python
"""Database View page and HTMX endpoints."""
import io
import pandas as pd
from fastapi import APIRouter, Request, Depends, Query
from fastapi.responses import HTMLResponse, StreamingResponse
from database import Session, SaleComp, LeaseComp
from web.dependencies import get_db, get_current_user

router = APIRouter(prefix="/database", tags=["database"])

def _load_data(session, comp_type: str) -> pd.DataFrame:
    """Load comps from DB into a DataFrame."""
    Model = SaleComp if comp_type == "sales" else LeaseComp
    records = session.query(Model).all()
    if not records:
        return pd.DataFrame()
    rows = [{c.name: getattr(r, c.name) for c in Model.__table__.columns} for r in records]
    return pd.DataFrame(rows)

def _apply_filters(df: pd.DataFrame, filters: dict, comp_type: str) -> pd.DataFrame:
    """Apply active filters to DataFrame."""
    if not filters or df.empty:
        return df
    # Search filter
    search = filters.get("search", "").strip()
    if search:
        search_cols = ["address"]
        if comp_type == "sales":
            search_cols += ["buyer", "seller", "notes"]
        else:
            search_cols += ["tenant_name", "notes"]
        mask = pd.Series(False, index=df.index)
        for col in search_cols:
            if col in df.columns:
                mask |= df[col].astype(str).str.contains(search, case=False, na=False, regex=False)
        df = df[mask]
    # Categorical filters (city, zip_code)
    for key in ["city", "zip_code"]:
        vals = filters.get(key)
        if vals:
            df = df[df[key].isin(vals)]
    # Numeric range filters
    numeric_filters = {
        "min_sale_price": ("sale_price", ">="),
        "max_sale_price": ("sale_price", "<="),
        "min_price_per_sf": ("price_per_sf", ">="),
        "max_price_per_sf": ("price_per_sf", "<="),
        "min_building_size": ("building_size", ">="),
        "max_building_size": ("building_size", "<="),
        "min_rate_monthly": ("rate_monthly", ">="),
        "max_rate_monthly": ("rate_monthly", "<="),
    }
    for fkey, (col, op) in numeric_filters.items():
        val = filters.get(fkey)
        if val is not None and col in df.columns:
            if op == ">=":
                df = df[pd.to_numeric(df[col], errors="coerce") >= float(val)]
            else:
                df = df[pd.to_numeric(df[col], errors="coerce") <= float(val)]
    # Date range filters
    for date_col in ["closing_date", "commencement_date"]:
        if date_col in df.columns:
            min_date = filters.get(f"min_{date_col}")
            max_date = filters.get(f"max_{date_col}")
            if min_date or max_date:
                dates = pd.to_datetime(df[date_col], errors="coerce")
                if min_date:
                    df = df[dates >= pd.to_datetime(min_date)]
                if max_date:
                    df = df[dates <= pd.to_datetime(max_date)]
    return df

def _compute_metrics(df: pd.DataFrame, comp_type: str) -> dict:
    """Compute summary metrics for the metrics row."""
    if df.empty:
        return {"count": 0, "avg_price": None, "avg_psf": None}
    if comp_type == "sales":
        return {
            "count": len(df),
            "avg_price": pd.to_numeric(df.get("sale_price"), errors="coerce").mean(),
            "avg_psf": pd.to_numeric(df.get("price_per_sf"), errors="coerce").mean(),
        }
    return {
        "count": len(df),
        "avg_rate_monthly": pd.to_numeric(df.get("rate_monthly"), errors="coerce").mean(),
        "avg_rate_annually": pd.to_numeric(df.get("rate_annually"), errors="coerce").mean(),
    }

def _parse_filters(request: Request) -> dict:
    """Parse filter state from query params."""
    params = dict(request.query_params)
    filters = {}
    if "search" in params:
        filters["search"] = params["search"]
    for key in ["city", "zip_code"]:
        val = params.get(key)
        if val:
            filters[key] = val.split(",")
    for key in ["min_sale_price", "max_sale_price", "min_price_per_sf", "max_price_per_sf",
                 "min_building_size", "max_building_size", "min_rate_monthly", "max_rate_monthly",
                 "min_closing_date", "max_closing_date", "min_commencement_date", "max_commencement_date"]:
        val = params.get(key)
        if val:
            filters[key] = val
    return filters

@router.get("", response_class=HTMLResponse)
async def database_page(request: Request):
    """Full Database View page."""
    templates = request.app.state.templates
    user = request.state.user
    session = Session()
    try:
        comp_type = request.query_params.get("type", "sales")
        filters = _parse_filters(request)
        df = _load_data(session, comp_type)
        total = len(df)
        df_filtered = _apply_filters(df, filters, comp_type)

        # Count both types for display
        sale_count = session.query(SaleComp).count()
        lease_count = session.query(LeaseComp).count()

        metrics = _compute_metrics(df_filtered, comp_type)
        table_data = df_filtered.to_dict(orient="records") if not df_filtered.empty else []

        return templates.TemplateResponse("database.html", {
            "request": request,
            "user": user,
            "current_page": "database",
            "logo_b64": request.app.state.logo_b64,
            "icon_b64": request.app.state.icon_b64,
            "comp_type": comp_type,
            "sale_count": sale_count,
            "lease_count": lease_count,
            "metrics": metrics,
            "table_data": table_data,
            "total": total,
            "filtered": len(df_filtered),
            "filters": filters,
            "columns": list(df_filtered.columns) if not df_filtered.empty else [],
        })
    finally:
        session.close()

@router.get("/table", response_class=HTMLResponse)
async def database_table(request: Request):
    """HTMX: return table partial."""
    templates = request.app.state.templates
    session = Session()
    try:
        comp_type = request.query_params.get("type", "sales")
        filters = _parse_filters(request)
        df = _load_data(session, comp_type)
        total = len(df)
        df_filtered = _apply_filters(df, filters, comp_type)
        table_data = df_filtered.to_dict(orient="records") if not df_filtered.empty else []
        return templates.TemplateResponse("partials/data_table.html", {
            "request": request,
            "table_data": table_data,
            "total": total,
            "filtered": len(df_filtered),
            "comp_type": comp_type,
            "columns": list(df_filtered.columns) if not df_filtered.empty else [],
        })
    finally:
        session.close()

@router.get("/metrics", response_class=HTMLResponse)
async def database_metrics(request: Request):
    """HTMX: return metrics row partial."""
    templates = request.app.state.templates
    session = Session()
    try:
        comp_type = request.query_params.get("type", "sales")
        filters = _parse_filters(request)
        df = _load_data(session, comp_type)
        df_filtered = _apply_filters(df, filters, comp_type)
        sale_count = session.query(SaleComp).count()
        lease_count = session.query(LeaseComp).count()
        metrics = _compute_metrics(df_filtered, comp_type)
        return templates.TemplateResponse("partials/metrics_row.html", {
            "request": request,
            "sale_count": sale_count,
            "lease_count": lease_count,
            "metrics": metrics,
            "comp_type": comp_type,
        })
    finally:
        session.close()

@router.post("/export")
async def database_export(request: Request):
    """Export data as Excel, CSV, or KML."""
    body = await request.json()
    fmt = body.get("format", "csv")
    ids = body.get("ids", [])
    comp_type = body.get("type", "sales")

    session = Session()
    try:
        df = _load_data(session, comp_type)
        if ids:
            df = df[df["id"].isin(ids)]
        if df.empty:
            return StreamingResponse(io.BytesIO(b"No data"), media_type="text/plain")

        if fmt == "csv":
            buffer = io.StringIO()
            df.to_csv(buffer, index=False)
            return StreamingResponse(
                io.BytesIO(buffer.getvalue().encode()),
                media_type="text/csv",
                headers={"Content-Disposition": f"attachment; filename={comp_type}_comps.csv"},
            )
        elif fmt == "xlsx":
            buffer = io.BytesIO()
            df.to_excel(buffer, index=False, engine="xlsxwriter")
            buffer.seek(0)
            return StreamingResponse(
                buffer,
                media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                headers={"Content-Disposition": f"attachment; filename={comp_type}_comps.xlsx"},
            )
        elif fmt == "kml":
            from comp_engine import generate_kml
            kml_str = generate_kml(df)
            return StreamingResponse(
                io.BytesIO(kml_str.encode()),
                media_type="application/vnd.google-earth.kml+xml",
                headers={"Content-Disposition": f"attachment; filename={comp_type}_comps.kml"},
            )
    finally:
        session.close()

@router.delete("/records")
async def delete_records(request: Request):
    """Admin: delete selected records."""
    user = request.state.user
    if user.get("role") != "admin":
        return HTMLResponse("Forbidden", status_code=403)
    body = await request.json()
    ids = body.get("ids", [])
    comp_type = body.get("type", "sales")
    Model = SaleComp if comp_type == "sales" else LeaseComp
    session = Session()
    try:
        session.query(Model).filter(Model.id.in_(ids)).delete(synchronize_session=False)
        session.commit()
        return HTMLResponse(f"<div class='text-green-700'>Deleted {len(ids)} records.</div>")
    except Exception as e:
        session.rollback()
        return HTMLResponse(f"<div class='text-red-700'>Error: {e}</div>")
    finally:
        session.close()
```

- [ ] **Step 2: Create `web/templates/database.html`**

Extends `base.html`. Contains: type toggle, search input, metrics container, AG Grid container, map tab, export dropdown, admin actions. All dynamic regions have `id` attributes for HTMX targeting.

Key structure:
```html
{% extends "base.html" %}
{% block page_title %}Database View{% endblock %}
{% block topbar_right %}
  <!-- Export dropdown (Alpine.js, lives in topbar — never HTMX-swapped) -->
  <div x-data="{open: false}" class="relative">
    <button @click="open = !open" class="btn-primary text-sm">Export &#9662;</button>
    <div x-show="open" @click.away="open = false" class="export-dropdown">
      <a href="#" @click.prevent="exportData('/database/export','xlsx','{{ comp_type }}')">Excel</a>
      <a href="#" @click.prevent="exportData('/database/export','csv','{{ comp_type }}')">CSV</a>
      <a href="#" @click.prevent="exportData('/database/export','kml','{{ comp_type }}')">KML</a>
    </div>
  </div>
{% endblock %}
{% block content %}
  <!-- Type toggle + search (Alpine.js fires dual HTMX requests) -->
  <!-- Metrics row: #metrics-container -->
  <!-- AG Grid: #ag-grid-container (stable, never swapped) -->
  <!-- Data script: #table-data (swapped by HTMX) -->
  <!-- Map tab: Leaflet div -->
  <!-- Filter panel: #filter-panel -->
  <!-- Admin actions (x-show for admin role) -->
{% endblock %}
```

- [ ] **Step 3: Create `web/templates/partials/metrics_row.html`**

Four metric cards. Use Jinja2 formatting for currency/numbers. Handle None/empty with "--".

- [ ] **Step 4: Create `web/templates/partials/data_table.html`**

Contains a `<script>` tag that sets `window.__tableData` and `window.__tableColumns` then calls `updateGrid()` from `app.js`. Also contains the record count label.

- [ ] **Step 5: Verify Database View loads with data**

```bash
uvicorn web.main:app --port 8502 --reload &
# Log in, navigate to /database, verify table renders with AG Grid
```

- [ ] **Step 6: Commit**

```bash
git add web/
git commit -m "feat: Database View page with AG Grid table, metrics, filters, export, admin delete"
```

---

### Task 4: Database View — Filters + Map

**Files:**
- Create: `web/templates/partials/filter_panel.html`
- Create: `web/templates/partials/filter_chips.html`
- Modify: `web/routes/database.py` (add filter endpoints)

- [ ] **Step 1: Add filter HTMX endpoints to `web/routes/database.py`**

```python
@router.get("/filter-panel", response_class=HTMLResponse)
async def filter_panel(request: Request):
    """HTMX: return filter panel with available options."""
    # Query distinct cities, zip codes for categorical filters
    # Return filter_panel.html with options

@router.post("/filters", response_class=HTMLResponse)
async def add_filter(request: Request):
    """HTMX: add filter, return updated chips + table (hx-swap-oob)."""
    # Parse form data, build filter query string
    # Return filter_chips.html (OOB swap) + data_table.html

@router.delete("/filters/{key}", response_class=HTMLResponse)
async def remove_filter(request: Request, key: str):
    """HTMX: remove filter, return updated chips + table."""
    # Remove key from current filters, return updated partials

@router.get("/map", response_class=HTMLResponse)
async def database_map(request: Request):
    """HTMX: return map partial with GeoJSON markers."""
    # Load filtered data, build GeoJSON, return Leaflet init script
```

- [ ] **Step 2: Create `web/templates/partials/filter_panel.html`**

Filter form with: city multiselect, zip multiselect, numeric range inputs (price, size, rate), date range inputs. Each input has `name` attribute matching filter key. Submit button fires `hx-post="/database/filters"`.

- [ ] **Step 3: Create `web/templates/partials/filter_chips.html`**

Iterate active filters, render each as a chip with `hx-delete="/database/filters/{{ key }}"`. Include `hx-swap-oob="true"` attribute so chips update independently from table swaps.

- [ ] **Step 4: Verify filters work end-to-end**

Test: add a city filter → chips appear → table updates → remove chip → table resets.

- [ ] **Step 5: Commit**

```bash
git add web/
git commit -m "feat: Database View filters (chips, panel, map tab)"
```

---

## Chunk 3: Upload & Process Page

### Task 5: Upload & Process — File Upload + Mapping + Save

**Files:**
- Create: `web/templates/upload.html`
- Create: `web/templates/partials/upload_preview.html`
- Create: `web/templates/partials/mapping_status.html`
- Create: `web/templates/partials/processing_status.html`
- Modify: `web/routes/upload.py`

- [ ] **Step 1: Implement `web/routes/upload.py`**

Key endpoints:
```python
@router.get("")  # Full page
@router.post("/file")  # Receive file → process → return preview partial
@router.post("/mapping")  # Re-apply mapping → return updated preview
@router.post("/geocode")  # Start geocoding job → return job_id
@router.get("/geocode-stream")  # SSE stream for geocoding progress
@router.post("/save")  # Save to DB + persist_with_learning
```

The `/file` endpoint:
1. Save uploaded file to temp dir
2. Detect PDF vs Excel/CSV
3. For PDF: call `run_vision_pdf_stage(pdf_path, filename)`
4. For Excel/CSV: call `get_sheet_names()`, then `robust_load_file_segmented()` per sheet, then `run_mapping_stage()` per segment
5. Store results in server-side dict keyed by a job_id (UUID)
6. Return `upload_preview.html` partial

The `/mapping` endpoint:
1. Receive all select values as form data
2. Re-map columns using the user's selections (override engine mappings)
3. Return updated preview

The `/geocode` endpoint:
1. Start a background thread that geocodes each row via `resolve_geocode()`
2. Store progress in server-side dict
3. Return job_id

The `/geocode-stream` endpoint:
1. SSE generator that yields progress events until complete
2. Uses `EventSourceResponse` from `sse-starlette` (or manual SSE via `StreamingResponse`)

The `/save` endpoint:
1. Insert rows to DB via SQLAlchemy
2. Call `persist_with_learning(segments, final_mappings, edited_dfs, confirmed_broker, {}, store, db_saver, user)`
3. Return success message

- [ ] **Step 2: Create `web/templates/upload.html`**

Extends `base.html`. Contains:
- Drop zone with Alpine.js drag/drop handlers
- Hidden `<input type="file">` triggered by drop zone click
- `#upload-preview` div for HTMX swap target
- On file select: `hx-post="/upload/file"` with `hx-encoding="multipart/form-data"` targeting `#upload-preview`

- [ ] **Step 3: Create `web/templates/partials/upload_preview.html`**

Contains per-segment (multi-sheet) tabs:
- Raw input table (HTML `<table>`, first 5 rows, column headers with amber/grey badges)
- Column mapping grid (2-column grid of `<select>` elements)
- Confirmed Broker text input (pre-populated from `detect_broker_stage`)
- Mapping status bar (green/red tags)
- Geocode & Save button

Each `<select>` change fires `hx-post="/upload/mapping"` targeting `#upload-preview`.

- [ ] **Step 4: Create mapping status and processing status partials**

`mapping_status.html` — green tags for mapped fields, red for unmapped required.
`processing_status.html` — progress bar + current address during geocoding.

- [ ] **Step 5: Verify upload flow end-to-end**

Upload a test CSV → see raw table with badges → adjust a mapping → see preview update → geocode → save → verify records appear in Database View.

- [ ] **Step 6: Commit**

```bash
git add web/
git commit -m "feat: Upload & Process page with file upload, mapping, geocoding SSE, and save"
```

---

## Chunk 4: Analytics + Comp Finder Pages

### Task 6: Analytics Page

**Files:**
- Create: `web/templates/analytics.html`
- Create: `web/templates/partials/chart_distributions.html`
- Create: `web/templates/partials/chart_price_size.html`
- Create: `web/templates/partials/chart_trends.html`
- Create: `web/templates/partials/chart_zip.html`
- Create: `web/templates/partials/chart_map.html`
- Create: `web/templates/partials/chart_compare.html`
- Modify: `web/routes/analytics.py`

- [ ] **Step 1: Implement `web/routes/analytics.py`**

```python
@router.get("")  # Full page with all chart data
@router.get("/charts")  # HTMX: return chart data for a specific tab
@router.get("/metrics")  # HTMX: return metrics partial
```

The full page loads all data needed for all 6 tabs. Chart data is pre-aggregated in Python (pandas groupby, value_counts, etc.) and passed as JSON to templates. Plotly.js builds the figures client-side.

Metrics: Total Comps, Avg Sale Price / Avg Rate Monthly, Avg $/SF / Avg Rate Annually, Avg Size.

- [ ] **Step 2: Create `web/templates/analytics.html`**

Extends `base.html`. Contains: type toggle, filter chips, metrics row, 6-tab container (Alpine.js `x-show`). Each tab div contains a Plotly chart div + `<script>` with chart initialization.

- [ ] **Step 3: Create chart partials**

Each partial contains:
- A `<div id="chart-{name}">` for Plotly to render into
- A `<script>` that calls `Plotly.newPlot()` with the pre-aggregated data
- Handle empty state: "No data matching current filters"

Distribution tab: histograms for price, size, $/SF
Price vs Size: scatter plot
Trends: time series
By Zip Code: bar chart grouped by zip
Map: Leaflet heat map
Compare: multiselect to pick 2-5 properties, side-by-side table

- [ ] **Step 4: Verify all 6 chart tabs render**

- [ ] **Step 5: Commit**

```bash
git add web/
git commit -m "feat: Analytics page with 6 chart tabs (Plotly.js + Leaflet)"
```

---

### Task 7: Comp Finder Page

**Files:**
- Create: `web/templates/comp_finder.html`
- Create: `web/templates/partials/comp_results.html`
- Create: `web/templates/partials/comp_map.html`
- Modify: `web/routes/comp_finder.py`

- [ ] **Step 1: Implement `web/routes/comp_finder.py`**

```python
@router.get("")  # Full page with search form
@router.post("/search")  # HTMX: run comp search, return results partial
@router.post("/export")  # Export comp results
```

The `/search` endpoint:
1. Parse form: address, comp_type, optional fields (size, price, year_built, city, zip)
2. Geocode address via `resolve_geocode()`
3. Build subject dict with lat/lng + optional fields
4. Call `load_comps(comp_type)`
5. Call `compute_match_scores(subject, comps_df, comp_type, weights, max_radius)`
6. If AI enabled: call `compute_ai_scores()` + `blend_scores()`
7. Return `comp_results.html` partial

- [ ] **Step 2: Create `web/templates/comp_finder.html`**

Extends `base.html`. Two-column layout:
- Left: form with all inputs, Advanced Weights expander (Alpine.js `x-show`)
- Right: `#results-container` for HTMX swap

The "Find Comparable Properties" button: `hx-post="/finder/search"` with `hx-target="#results-container"` and `hx-include="form"`.

- [ ] **Step 3: Create `web/templates/partials/comp_results.html`**

- "N comps found" header + export dropdown
- Ranked cards (loop over results): rank badge (amber top 3, grey rest), address, meta line, match % with CSS progress bar
- Tabs below: Map | Score Breakdown

- [ ] **Step 4: Create `web/templates/partials/comp_map.html`**

Leaflet map with:
- Subject property marker (different color/icon)
- Comp markers with popups (address, score, key fields)

- [ ] **Step 5: Verify comp search flow end-to-end**

Enter address → click Find → see results appear → check map → export.

- [ ] **Step 6: Commit**

```bash
git add web/
git commit -m "feat: Comp Finder page with search, ranked results, map, export"
```

---

## Chunk 5: Deployment + Cleanup

### Task 8: Deployment Config + Final Cleanup

**Files:**
- Modify: `Dockerfile`
- Modify: `start.sh`
- Modify: `database.py` (remove streamlit dependency from `_get_db_url`)

- [ ] **Step 1: Update `database.py` to remove Streamlit dependency**

Replace the `_get_db_url()` function:

```python
def _get_db_url():
    """Get database URL from environment variable. No Streamlit dependency."""
    return os.environ.get("SUPABASE_DB_URL", "sqlite:///comps.db")
```

Remove the `try: import streamlit` block.

- [ ] **Step 2: Update `Dockerfile`**

```dockerfile
FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends poppler-utils && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

EXPOSE 8501

CMD ["uvicorn", "web.main:app", "--host", "0.0.0.0", "--port", "8501"]
```

- [ ] **Step 3: Update `start.sh`**

```bash
#!/bin/bash
exec uvicorn web.main:app --host 0.0.0.0 --port ${PORT:-8501}
```

No more secrets.toml generation. Env vars read directly by `web/config.py`.

- [ ] **Step 4: Update `comp_engine.py` imports if needed**

Check if `comp_engine.py` has any remaining `import streamlit` calls. If so, remove or guard them. The `generate_kml()` function should still work — it's pure Python.

- [ ] **Step 5: Run the full app locally**

```bash
# Set env vars
export GOOGLE_API_KEY="..."
export SUPABASE_DB_URL="..."
export OPENAI_API_KEY="..."

uvicorn web.main:app --port 8502 --reload

# Test all 4 pages:
# 1. Login with admin/harbor2024
# 2. Database View — verify table, filters, export
# 3. Upload — upload a test file, verify mapping, geocode, save
# 4. Analytics — verify all 6 chart tabs
# 5. Comp Finder — search, verify results + map
```

- [ ] **Step 6: Docker build and test**

```bash
docker build -t harbor-capital .
docker run -p 8501:8501 \
  -e GOOGLE_API_KEY="..." \
  -e SUPABASE_DB_URL="..." \
  -e OPENAI_API_KEY="..." \
  harbor-capital
```

- [ ] **Step 7: Commit**

```bash
git add Dockerfile start.sh database.py comp_engine.py requirements.txt
git commit -m "feat: deployment config for FastAPI (Dockerfile, start.sh, remove Streamlit deps)"
```

- [ ] **Step 8: Push to GitHub**

```bash
git push origin main
```

Railway will auto-deploy from the updated Dockerfile.
