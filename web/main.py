"""FastAPI application factory."""
import os
import sys
import base64
import traceback
from contextlib import asynccontextmanager
from fastapi import FastAPI, Request, Form
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from fastapi.responses import RedirectResponse, HTMLResponse, PlainTextResponse
from starlette.middleware.base import BaseHTTPMiddleware
from web.config import settings
from web.auth import get_session as get_auth_session
from web.routes import api_router

def _load_image_b64(relative_path: str) -> str:
    """Load image file and return base64 string."""
    base = os.path.dirname(os.path.dirname(__file__))
    full = os.path.join(base, relative_path)
    try:
        with open(full, "rb") as f:
            return base64.b64encode(f.read()).decode()
    except Exception as e:
        print(f"[logo] failed to load {full}: {e}", file=sys.stderr)
        return ""

class AuthMiddleware(BaseHTTPMiddleware):
    EXEMPT_PATHS = {"/login", "/static", "/health", "/version", "/api"}

    async def dispatch(self, request: Request, call_next):
        path = request.url.path
        if any(path.startswith(p) for p in self.EXEMPT_PATHS):
            return await call_next(request)
        user = get_auth_session(request)
        if not user:
            return RedirectResponse("/login", status_code=303)
        request.state.user = user
        return await call_next(request)

@asynccontextmanager
async def lifespan(app: FastAPI):
    try:
        app.state.logo_b64 = _load_image_b64("Logo Masters/Stacked Left/@300w/HC-Logo-Stacked-Left-Charcoal@300w.png")
        app.state.icon_b64 = _load_image_b64("Logo Masters/Dry Dock/@300w/HC-Logo-Icon-White@300w.png")
        print(f"[startup] logo_b64 len={len(app.state.logo_b64)} icon_b64 len={len(app.state.icon_b64)}", file=sys.stderr)
    except Exception as e:
        print(f"[startup] logo load failed: {e}", file=sys.stderr)
        app.state.logo_b64 = ""
        app.state.icon_b64 = ""
    try:
        from database import ensure_tables
        ensure_tables()
        print("[startup] ensure_tables ok", file=sys.stderr)
    except Exception as e:
        print(f"[startup] ensure_tables failed: {e}", file=sys.stderr)
    yield

app = FastAPI(title="Harbor Capital Comp Database", lifespan=lifespan)


# TEMPORARY diagnostic token — gates traceback exposure for a prod-only 500 we
# cannot reproduce locally. Remove once /database + /analytics are fixed.
_DIAG_TOKEN = "diag-3b9f7a1c-temp"


@app.exception_handler(Exception)
async def unhandled_exc_handler(request: Request, exc: Exception):
    tb = traceback.format_exc()
    print(f"[500] {request.method} {request.url.path}\n{tb}", file=sys.stderr)
    # In dev, show trace; in prod keep generic to avoid leaking internals.
    # Secret-gated escape hatch for diagnosing prod-only failures.
    if os.environ.get("DEBUG") == "1" or request.query_params.get("__diag") == _DIAG_TOKEN:
        return PlainTextResponse(tb, status_code=500)
    return PlainTextResponse("Internal Server Error", status_code=500)


app.add_middleware(AuthMiddleware)

static_dir = os.path.join(os.path.dirname(__file__), "static")
os.makedirs(static_dir, exist_ok=True)
app.mount("/static", StaticFiles(directory=static_dir), name="static")

templates_dir = os.path.join(os.path.dirname(__file__), "templates")
os.makedirs(templates_dir, exist_ok=True)
templates = Jinja2Templates(directory=templates_dir)
templates.env.globals["GOOGLE_API_KEY"] = settings.GOOGLE_API_KEY
app.state.templates = templates

app.include_router(api_router)

@app.get("/login")
async def login_page(request: Request):
    return templates.TemplateResponse(request, "login.html", {
        "request": request, "error": None,
        "logo_b64": request.app.state.logo_b64,
    })

@app.post("/login")
async def login_submit(request: Request, username: str = Form(...), password: str = Form(...)):
    from web.auth import verify_password, create_session
    user = verify_password(username, password)
    if not user:
        return templates.TemplateResponse(request, "login.html", {
            "request": request, "error": "Invalid username or password",
            "logo_b64": request.app.state.logo_b64,
        })
    response = RedirectResponse("/database", status_code=303)
    create_session(user, response)
    return response

@app.get("/logout")
async def logout(request: Request):
    from web.auth import destroy_session
    response = RedirectResponse("/login", status_code=303)
    destroy_session(request, response)
    return response

@app.get("/health")
async def health():
    return {"status": "ok"}

@app.get("/version")
async def version():
    """Report the running build's git commit so deploys are verifiable via curl.

    Railway injects RAILWAY_GIT_COMMIT_SHA into every deployment; falls back to
    a build-baked GIT_COMMIT_SHA env var, else "unknown".
    """
    return {
        "commit": os.environ.get("RAILWAY_GIT_COMMIT_SHA")
        or os.environ.get("GIT_COMMIT_SHA", "unknown"),
        "branch": os.environ.get("RAILWAY_GIT_BRANCH", "unknown"),
        "deploy_id": os.environ.get("RAILWAY_DEPLOYMENT_ID", "unknown"),
    }

@app.get("/")
async def root():
    return RedirectResponse("/database", status_code=303)


@app.get("/stats")
async def stats_alias():
    return RedirectResponse("/analytics", status_code=303)


@app.get("/comp-finder")
async def comp_finder_alias():
    return RedirectResponse("/finder", status_code=303)
