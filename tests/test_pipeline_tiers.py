"""Tests for engine/pipeline.py — tiered mapping stage."""
import pandas as pd
import pytest
from engine.pipeline import run_mapping_stage
from engine.fingerprint import compute_fingerprint
from learning.fakes import FakeLearningStore


def test_exact_tier_skips_embeddings():
    """Tier 1 exact match uses stored mappings without calling embeddings."""
    store = FakeLearningStore()
    headers = ["Property Name", "Tenant Name", "Rent PSF", "Date", "SF"]
    df = pd.DataFrame({h: ["x"] for h in headers})

    # classify_file_type returns uppercase so must use "LEASE" here
    fp = compute_fingerprint(headers, "f.xlsx", None, "LEASE")
    store.record_accepted_mapping(
        fp,
        {"Property Name": "address", "Tenant Name": "tenant_name",
         "Rent PSF": "rate_psf", "Date": "commencement_date", "SF": "leased_sf"},
        confirmed_by="user@test",
    )

    result = run_mapping_stage(df, filename="f.xlsx", sheet_name=None, store=store)
    assert result.source == "exact"
    assert result.mappings.get("Rent PSF") == "rate_psf"


def test_no_match_falls_back_to_embeddings():
    """Empty store causes fallback to embedding pipeline."""
    store = FakeLearningStore()
    df = pd.DataFrame({
        "Property": ["p"], "Tenant": ["t"], "Rent PSF": [1],
        "Lease Date": ["2024-01-01"], "SF": [100],
    })
    result = run_mapping_stage(df, filename="f.xlsx", sheet_name=None, store=store)
    assert result.source in {"embedding", "embedding+corrections"}
    assert "Rent PSF" in result.mappings


def test_result_is_mapping_result_type():
    """run_mapping_stage returns a MappingResult with all required fields."""
    from engine.types import MappingResult
    store = FakeLearningStore()
    df = pd.DataFrame({"Tenant Name": ["Corp"], "Rent PSF": [20.0]})
    result = run_mapping_stage(df, filename="x.xlsx", sheet_name="Sheet1", store=store)
    assert isinstance(result, MappingResult)
    assert result.fingerprint is not None
    assert isinstance(result.mappings, dict)
    assert isinstance(result.confidence, dict)
    assert result.source in {"exact", "fuzzy", "broker", "embedding", "embedding+corrections"}
