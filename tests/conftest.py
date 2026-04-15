"""Shared pytest fixtures for the self-learning engine tests."""
import os
import pickle
import hashlib
import pytest
from pathlib import Path


FIXTURES_DIR = Path(__file__).parent / "fixtures"
CACHE_PATH = FIXTURES_DIR / "embeddings_cache.pkl"


@pytest.fixture
def fixtures_dir():
    return FIXTURES_DIR


@pytest.fixture
def sample_comp_files_dir():
    """Path to the 16 real sample files used for Phase 8 seeding + regression."""
    return Path(__file__).parent.parent / "sample comp files"


def _cache_key(texts):
    return hashlib.sha256("\x00".join(texts).encode("utf-8")).hexdigest()


@pytest.fixture(autouse=True)
def deterministic_embeddings(monkeypatch, request):
    """Replace get_embeddings with cache-backed version for all tests.
    Opt out with @pytest.mark.live_embeddings."""
    if request.node.get_closest_marker("live_embeddings"):
        return
    if not CACHE_PATH.exists():
        return  # let tests that need embeddings skip/fail naturally

    with CACHE_PATH.open("rb") as fh:
        cache: dict = pickle.load(fh)

    def _fake(texts):
        key = _cache_key(list(texts))
        if key not in cache:
            raise KeyError(
                f"embedding cache miss ({len(texts)} texts) — "
                "rerun tools/rebuild_embedding_fixture.py"
            )
        return cache[key]

    try:
        import comp_engine
        monkeypatch.setattr(comp_engine, "get_embeddings", _fake)
    except Exception:
        pass
    try:
        import engine.mapping
        monkeypatch.setattr(engine.mapping, "get_embeddings", _fake)
    except Exception:
        pass
