"""Tests for generate_standardized_df_with_hints in engine/mapping.py."""
import pandas as pd
import pytest
from engine.mapping import (
    _apply_hard_overrides,
    generate_standardized_df_with_hints,
    LEASE_SCHEMA,
)
from learning.fakes import FakeLearningStore


def test_hint_biases_toward_corrected_column():
    """Correction for 'asking rate' -> 'rate_psf' should survive in output mappings."""
    store = FakeLearningStore()
    store.upsert_correction(
        file_type="LEASE",
        raw_header="asking rate",
        target_column="rate_psf",
        confirmed_by="user@test",
    )

    df = pd.DataFrame({
        "Property": ["P1"],
        "Tenant": ["T1"],
        "Asking Rate": [18.5],
        "Lease Date": ["2024-01-15"],
        "SF": [10000],
    })

    out_df, mappings, confidence = generate_standardized_df_with_hints(
        df, LEASE_SCHEMA, file_type="LEASE", store=store
    )
    assert mappings.get("Asking Rate") == "rate_psf"


def test_no_store_behaves_like_standard_mapping():
    """With store=None, results should be valid (no crash, schema columns in output)."""
    df = pd.DataFrame({
        "Property Name": ["Building A"],
        "Tenant": ["Corp Inc"],
        "Rent PSF": [24.0],
        "SF": [5000],
    })

    out_df, mappings, confidence = generate_standardized_df_with_hints(
        df, LEASE_SCHEMA, file_type="LEASE", store=None
    )
    assert isinstance(out_df, pd.DataFrame)
    assert isinstance(mappings, dict)
    assert isinstance(confidence, dict)
    # Some schema fields should be in output
    assert len(out_df.columns) > 0


def test_empty_store_behaves_like_standard_mapping():
    """Empty store (no corrections) should produce same structure as no store."""
    store = FakeLearningStore()
    df = pd.DataFrame({
        "Tenant": ["T1"],
        "Rent PSF": [18.0],
    })
    out_df, mappings, confidence = generate_standardized_df_with_hints(
        df, LEASE_SCHEMA, file_type="LEASE", store=store
    )
    assert isinstance(out_df, pd.DataFrame)
    assert isinstance(mappings, dict)


def test_hint_returns_raw_header_keyed_mappings():
    """mappings keys must be raw header strings (not schema column names)."""
    store = FakeLearningStore()
    df = pd.DataFrame({
        "Tenant Name": ["Corp"],
        "Rent PSF": [20.0],
        "Lease SF": [3000],
    })
    out_df, mappings, confidence = generate_standardized_df_with_hints(
        df, LEASE_SCHEMA, file_type="LEASE", store=store
    )
    for k in mappings:
        assert k in df.columns, f"mapping key '{k}' not a raw header"


def test_hard_overrides_keep_best_source_per_target():
    raw_headers = ["Rate", "Rate PSF"]
    normalized = ["rate", "rate psf"]
    mappings = {"Rate": "tenant_name"}
    confidence = {"Rate": 0.5}

    _apply_hard_overrides(
        raw_headers,
        normalized,
        "LEASE",
        list(LEASE_SCHEMA.keys()),
        mappings,
        confidence,
    )

    assert mappings["Rate PSF"] == "rate_psf"
    assert list(mappings.values()).count("rate_psf") == 1


def test_hard_overrides_choose_exact_target_before_schema_order():
    raw_headers = ["Rate Type"]
    normalized = ["rate type"]
    mappings = {}
    confidence = {}

    _apply_hard_overrides(
        raw_headers,
        normalized,
        "LEASE",
        list(LEASE_SCHEMA.keys()),
        mappings,
        confidence,
    )

    assert mappings["Rate Type"] == "lease_type"
