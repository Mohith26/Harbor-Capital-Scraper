"""Smoke test for run_geocoding_stage in engine/pipeline.py."""
import pandas as pd
from engine.pipeline import run_geocoding_stage
from learning.fakes import FakeLearningStore


def test_stage_uses_override_for_every_row():
    store = FakeLearningStore()
    store.record_geocode_override(
        raw_text="123 A St, TX",  # Texas-normalized key
        override_address="123 A St, Houston, TX",
        lat=29.7, lng=-95.3,
        confirmed_by="u",
    )
    df = pd.DataFrame({"addr": ["123 A St", "123 A St"]})
    out = run_geocoding_stage(df, "addr", api_key="k", store=store)
    assert list(out["latitude"]) == [29.7, 29.7]
    assert list(out["geocode_source"]) == ["override", "override"]


def test_stage_returns_failed_source_when_no_key():
    store = FakeLearningStore()
    df = pd.DataFrame({"addr": ["999 Unknown Blvd"]})
    out = run_geocoding_stage(df, "addr", api_key="", store=store)
    # Empty api_key → fetch_google_data returns None → source = "failed"
    assert out["geocode_source"].iloc[0] == "failed"
    assert out["latitude"].iloc[0] is None


def test_stage_preserves_existing_columns():
    store = FakeLearningStore()
    df = pd.DataFrame({"addr": ["123 A St"], "tenant_name": ["Corp"]})
    out = run_geocoding_stage(df, "addr", api_key="", store=store)
    assert "tenant_name" in out.columns
