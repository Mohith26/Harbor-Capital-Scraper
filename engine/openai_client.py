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
