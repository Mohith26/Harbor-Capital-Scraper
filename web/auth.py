"""Session-based authentication for FastAPI."""
import os
import secrets
import time
import yaml
import bcrypt
from typing import Optional
from itsdangerous import URLSafeTimedSerializer
from fastapi import Request, Response
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
