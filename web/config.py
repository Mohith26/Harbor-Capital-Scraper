"""Application settings from environment variables."""
import os
from dotenv import load_dotenv

load_dotenv()

def _flag(name: str, default: str = "") -> bool:
    return os.environ.get(name, default).strip().lower() in {"1", "true", "yes", "on"}


class Settings:
    GOOGLE_API_KEY: str = os.environ.get("GOOGLE_API_KEY", "")
    SUPABASE_DB_URL: str = os.environ.get("SUPABASE_DB_URL", "sqlite:///comps.db")
    SUPABASE_URL: str = os.environ.get("SUPABASE_URL", "")
    SUPABASE_KEY: str = os.environ.get("SUPABASE_KEY", "")
    OPENAI_API_KEY: str = os.environ.get("OPENAI_API_KEY", "")
    SECRET_KEY: str = os.environ.get("SECRET_KEY", "harbor-capital-dev-secret-change-me")

    # --- Cloudflare -------------------------------------------------------
    CLOUDFLARE_ACCOUNT_ID: str = os.environ.get("CLOUDFLARE_ACCOUNT_ID", "")
    D1_DATABASE_ID: str = os.environ.get("D1_DATABASE_ID", "")
    CF_ACCESS_TEAM_DOMAIN: str = os.environ.get("CF_ACCESS_TEAM_DOMAIN", "")
    CF_ACCESS_AUD: str = os.environ.get("CF_ACCESS_AUD", "")

    # Served over HTTPS everywhere except local development.
    COOKIE_SECURE: bool = _flag("COOKIE_SECURE", "true")
    SESSION_MAX_AGE: int = 30 * 24 * 3600  # 30 days in seconds
    PDF_MAX_PAGES: int = int(os.environ.get("PDF_MAX_PAGES", "20"))
    PDF_TOTAL_TIMEOUT_SECONDS: int = int(os.environ.get("PDF_TOTAL_TIMEOUT_SECONDS", "120"))
    PDF_RASTER_TIMEOUT_SECONDS: int = int(os.environ.get("PDF_RASTER_TIMEOUT_SECONDS", "20"))
    PDF_OPENAI_TIMEOUT_SECONDS: int = int(os.environ.get("PDF_OPENAI_TIMEOUT_SECONDS", "45"))
    PDF_DPI: int = int(os.environ.get("PDF_DPI", "150"))

settings = Settings()
