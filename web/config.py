"""Application settings from environment variables."""
import os
from dotenv import load_dotenv

load_dotenv()

class Settings:
    GOOGLE_API_KEY: str = os.environ.get("GOOGLE_API_KEY", "")
    SUPABASE_DB_URL: str = os.environ.get("SUPABASE_DB_URL", "sqlite:///comps.db")
    SUPABASE_URL: str = os.environ.get("SUPABASE_URL", "")
    SUPABASE_KEY: str = os.environ.get("SUPABASE_KEY", "")
    OPENAI_API_KEY: str = os.environ.get("OPENAI_API_KEY", "")
    SECRET_KEY: str = os.environ.get("SECRET_KEY", "harbor-capital-dev-secret-change-me")
    SESSION_MAX_AGE: int = 30 * 24 * 3600  # 30 days in seconds

settings = Settings()
