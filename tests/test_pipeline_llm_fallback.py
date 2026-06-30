import os
import pandas as pd
import pytest
import engine.pipeline as pipeline
from engine.pipeline import run_mapping_stage
from learning.fakes import FakeLearningStore


@pytest.fixture(autouse=True)
def _clear_llm_cache():
    """The raw_hash cache is module-level; isolate it between tests."""
    pipeline._LLM_MAPPING_CACHE.clear()
    yield
    pipeline._LLM_MAPPING_CACHE.clear()


def _df():
    return pd.DataFrame({
        "Property": ["1326 W Carrier Pkwy"],
        "Tenant": ["Spire Building Supplies"],
        "Asking Rate": ["$8.15"],
        "Area Leased": ["20,007"],
    })


def test_llm_path_used_when_enabled(monkeypatch):
    monkeypatch.setenv("COMP_LLM_MAPPER", "1")

    def fake_llm(headers, sample_rows, schema, file_type, examples=None):
        return {
            "mappings": {"Asking Rate": "rate_psf", "Area Leased": "leased_sf",
                         "Tenant": "tenant_name", "Property": "address"},
            "confidence": {"Asking Rate": 0.95, "Area Leased": 0.92,
                           "Tenant": 0.9, "Property": 0.85},
            "unmapped": [],
            "reasoning": "ok",
        }

    monkeypatch.setattr(pipeline, "llm_map_columns", fake_llm)
    monkeypatch.setattr(pipeline, "verify_mapping",
                        lambda m, rows, schema: {"adjusted_confidence": {}, "flags": []})

    result = run_mapping_stage(_df(), "lease_comps.csv", "Lease", FakeLearningStore())

    assert result.source.startswith("llm")
    assert result.mappings.get("Asking Rate") == "rate_psf"
    assert result.mappings.get("Area Leased") == "leased_sf"


def test_llm_error_falls_through_to_embeddings(monkeypatch):
    monkeypatch.setenv("COMP_LLM_MAPPER", "1")

    def boom(*a, **k):
        raise RuntimeError("no api key")
    monkeypatch.setattr(pipeline, "llm_map_columns", boom)

    # Must not raise; falls through to generate_standardized_df_with_hints (heuristic offline)
    result = run_mapping_stage(_df(), "lease_comps.csv", "Lease", FakeLearningStore())
    assert result.source in ("embedding", "embedding+corrections", "heuristic")


def test_llm_disabled_by_kill_switch(monkeypatch):
    monkeypatch.setenv("COMP_LLM_MAPPER", "0")
    called = {"llm": False}

    def fake_llm(*a, **k):
        called["llm"] = True
        return {"mappings": {}, "confidence": {}, "unmapped": [], "reasoning": ""}
    monkeypatch.setattr(pipeline, "llm_map_columns", fake_llm)

    run_mapping_stage(_df(), "lease_comps.csv", "Lease", FakeLearningStore())
    assert called["llm"] is False
