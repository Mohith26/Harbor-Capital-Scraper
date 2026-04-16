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


def extract_broker(sample_text: str, filename: str) -> dict:
    """One-shot LLM call: return {"broker": str|None, "confidence": float}.

    Texts provided: filename + first 2000 chars of sample_text.
    Low confidence (<0.5) returns None as the broker name.
    """
    import json
    prompt = (
        "You are given a commercial real estate comp file. Identify which brokerage "
        "firm produced it (e.g., JLL, CBRE, Colliers, Newmark, Cushman & Wakefield). "
        'Return a JSON object: {"broker": "<name>" or null, "confidence": 0.0-1.0}. '
        "If you're not reasonably sure, return null.\n\n"
        f"Filename: {filename}\n\n"
        f"Sample:\n{sample_text[:2000]}"
    )
    try:
        resp = _client().chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role": "user", "content": prompt}],
            response_format={"type": "json_object"},
            temperature=0,
            max_tokens=100,
        )
        parsed = json.loads(resp.choices[0].message.content)
        if parsed.get("confidence", 0) < 0.5:
            return {"broker": None, "confidence": parsed.get("confidence", 0)}
        return parsed
    except Exception:
        return {"broker": None, "confidence": 0.0}


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
