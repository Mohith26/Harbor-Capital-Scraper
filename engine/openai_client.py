"""Shared OpenAI client for engine modules."""
from __future__ import annotations
import os

_openai_client = None


def _get_secret(key, default=""):
    try:
        import streamlit as st
        return st.secrets[key]
    except Exception:
        return os.environ.get(key, default)


def _client():
    global _openai_client
    if _openai_client is None:
        from openai import OpenAI
        api_key = _get_secret("OPENAI_API_KEY")
        _openai_client = OpenAI(api_key=api_key)
    return _openai_client


def normalize(raw_text: str) -> str:
    """Ask GPT to clean up a messy address into a Google-Maps-friendly form.

    Texas is guaranteed, so we instruct the model to append ', TX' if missing.
    """
    prompt = (
        "Clean this commercial real estate property reference into a Google-Maps-"
        "friendly street address. The property is in Texas. Return ONLY the cleaned "
        "address with no commentary.\n\n"
        f"Raw: {raw_text}"
    )
    resp = _client().chat.completions.create(
        model="gpt-4o-mini",
        messages=[{"role": "user", "content": prompt}],
        temperature=0,
        max_tokens=80,
    )
    return resp.choices[0].message.content.strip()
