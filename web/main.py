"""FastAPI application factory."""
import os
import base64
from contextlib import asynccontextmanager
from fastapi import FastAPI, Request, Form
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from fastapi.responses import RedirectResponse, HTMLResponse
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
    except Exception:
        return ""

class AuthMiddleware(BaseHTTPMiddleware):
    EXEMPT_PATHS = {"/login", "/static", "/health"}

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
    app.state.logo_b64 = _load_image_b64("Logo Masters/Stacked Left/@300w/HC-Logo-Stacked-Left-Charcoal@300w.png")
    app.state.icon_b64 = _load_image_b64("Logo Masters/Dry Dock/@300w/HC-Logo-Icon-White@300w.png")
    from database import ensure_tables
    ensure_tables()
    yield

app = FastAPI(title="Harbor Capital Comp Database", lifespan=lifespan)
app.add_middleware(AuthMiddleware)

static_dir = os.path.join(os.path.dirname(__file__), "static")
os.makedirs(static_dir, exist_ok=True)
app.mount("/static", StaticFiles(directory=static_dir), name="static")

templates_dir = os.path.join(os.path.dirname(__file__), "templates")
os.makedirs(templates_dir, exist_ok=True)
templates = Jinja2Templates(directory=templates_dir)
app.state.templates = templates

app.include_router(api_router)

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

@app.get("/health")
async def health():
    return {"status": "ok"}

@app.get("/")
async def root():
    return RedirectResponse("/database", status_code=303)
