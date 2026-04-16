"""FastAPI dependency injection."""
from typing import Generator
from fastapi import Request, HTTPException
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
