"""Authentication for the Comp Database.

Two paths, checked in order:

1. **Cloudflare Access** - when the request carries a
   ``Cf-Access-Jwt-Assertion`` header and ``CF_ACCESS_TEAM_DOMAIN`` /
   ``CF_ACCESS_AUD`` are configured, the identity comes from Access and no
   password is involved.
2. **Local password login** - the original ``auth_config.yaml`` + bcrypt flow,
   kept as a fallback so the app still works if Access is not enabled.

Sessions are persisted in the database rather than in a module-level dict.
The old in-memory store meant every restart logged everyone out, which is
fatal on Cloudflare Containers because they sleep when idle.
"""

import json
import os
import secrets
import time
import urllib.request
from typing import Optional

import bcrypt
import yaml
from fastapi import Request, Response
from itsdangerous import URLSafeTimedSerializer

from database import Session, UserSession
from web.config import settings

_serializer = URLSafeTimedSerializer(settings.SECRET_KEY)

COOKIE_NAME = "harbor_session"
COOKIE_MAX_AGE = settings.SESSION_MAX_AGE

# --- Cloudflare Access -------------------------------------------------------

_ACCESS_TEAM_DOMAIN = os.environ.get("CF_ACCESS_TEAM_DOMAIN", "").strip()
_ACCESS_AUD = os.environ.get("CF_ACCESS_AUD", "").strip()
_ACCESS_HEADER = "cf-access-jwt-assertion"

_jwks_cache: dict = {"fetched_at": 0.0, "keys": None}


def access_enabled() -> bool:
    """True when Cloudflare Access is configured for this deployment."""
    return bool(_ACCESS_TEAM_DOMAIN and _ACCESS_AUD)


def _load_jwks():
    """Fetch (and cache for an hour) Cloudflare Access's signing keys."""
    now = time.time()
    if _jwks_cache["keys"] is not None and now - _jwks_cache["fetched_at"] < 3600:
        return _jwks_cache["keys"]
    url = f"https://{_ACCESS_TEAM_DOMAIN}/cdn-cgi/access/certs"
    with urllib.request.urlopen(url, timeout=10) as response:
        payload = json.loads(response.read().decode("utf-8"))
    _jwks_cache["keys"] = payload.get("keys") or []
    _jwks_cache["fetched_at"] = now
    return _jwks_cache["keys"]


def _identity_from_access(request: Request) -> Optional[dict]:
    """Validate the Access JWT and return the user it identifies."""
    if not access_enabled():
        return None
    token = request.headers.get(_ACCESS_HEADER)
    if not token:
        return None
    try:
        import jwt  # PyJWT

        header = jwt.get_unverified_header(token)
        for key in _load_jwks():
            if key.get("kid") != header.get("kid"):
                continue
            public_key = jwt.algorithms.RSAAlgorithm.from_jwk(json.dumps(key))
            claims = jwt.decode(
                token,
                public_key,
                algorithms=["RS256"],
                audience=_ACCESS_AUD,
                issuer=f"https://{_ACCESS_TEAM_DOMAIN}",
            )
            email = claims.get("email") or claims.get("common_name") or "access-user"
            return {
                "username": email,
                "name": email.split("@")[0].replace(".", " ").title(),
                "role": "analyst",
                "via": "cloudflare-access",
            }
    except Exception as exc:  # pragma: no cover - depends on live Access config
        print(f"Cloudflare Access token rejected: {exc}")
    return None


# --- Local password login ----------------------------------------------------

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
        return {
            "username": username,
            "name": user.get("name", username),
            "role": user.get("role", "analyst"),
        }
    return None


# --- Session storage ---------------------------------------------------------

def _purge_expired(db) -> None:
    try:
        db.query(UserSession).filter(UserSession.expires_at < time.time()).delete(
            synchronize_session=False
        )
        db.commit()
    except Exception:
        db.rollback()


def create_session(user_info: dict, response: Response) -> str:
    """Persist a new session and set the signed cookie."""
    token = secrets.token_urlsafe(32)
    now = time.time()
    db = Session()
    try:
        db.add(
            UserSession(
                token=token,
                username=user_info["username"],
                name=user_info.get("name"),
                role=user_info.get("role", "analyst"),
                login_time=now,
                expires_at=now + COOKIE_MAX_AGE,
            )
        )
        db.commit()
        _purge_expired(db)
    finally:
        db.close()
    response.set_cookie(
        COOKIE_NAME,
        _serializer.dumps(token),
        max_age=COOKIE_MAX_AGE,
        httponly=True,
        samesite="lax",
        secure=settings.COOKIE_SECURE,
    )
    return token


def get_session(request: Request) -> Optional[dict]:
    """Get the current user, preferring a Cloudflare Access identity."""
    identity = _identity_from_access(request)
    if identity:
        return identity

    cookie = request.cookies.get(COOKIE_NAME)
    if not cookie:
        return None
    try:
        token = _serializer.loads(cookie, max_age=COOKIE_MAX_AGE)
    except Exception:
        return None

    db = Session()
    try:
        record = db.query(UserSession).filter(UserSession.token == token).first()
        if record is None:
            return None
        if record.expires_at < time.time():
            db.delete(record)
            db.commit()
            return None
        return {
            "username": record.username,
            "name": record.name,
            "role": record.role,
            "login_time": record.login_time,
        }
    finally:
        db.close()


def destroy_session(request: Request, response: Response) -> None:
    """Clear the stored session and the cookie."""
    cookie = request.cookies.get(COOKIE_NAME)
    if cookie:
        try:
            token = _serializer.loads(cookie, max_age=COOKIE_MAX_AGE)
            db = Session()
            try:
                db.query(UserSession).filter(UserSession.token == token).delete(
                    synchronize_session=False
                )
                db.commit()
            finally:
                db.close()
        except Exception:
            pass
    response.delete_cookie(COOKIE_NAME)
