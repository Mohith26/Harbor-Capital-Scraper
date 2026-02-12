import os
from uuid import uuid4
from supabase import create_client

BUCKET_NAME = "comp-files"

def _get_secret(key, default=""):
    try:
        import streamlit as st
        return st.secrets[key]
    except Exception:
        return os.environ.get(key, default)

def _get_supabase_client():
    url = _get_secret("SUPABASE_URL")
    key = _get_secret("SUPABASE_KEY")
    if not url or not key:
        raise ValueError("SUPABASE_URL and SUPABASE_KEY must be configured in secrets or environment variables.")
    return create_client(url, key)


def upload_file(file_bytes: bytes, filename: str) -> str:
    """Upload file to Supabase Storage and return the public URL."""
    client = _get_supabase_client()
    safe_name = filename.replace(" ", "_")
    path = f"{uuid4().hex[:8]}_{safe_name}"
    client.storage.from_(BUCKET_NAME).upload(
        path, file_bytes, {"content-type": "application/octet-stream"}
    )
    return client.storage.from_(BUCKET_NAME).get_public_url(path)


def get_download_url(path: str) -> str:
    """Get public download URL for a file in Supabase Storage."""
    client = _get_supabase_client()
    return client.storage.from_(BUCKET_NAME).get_public_url(path)
