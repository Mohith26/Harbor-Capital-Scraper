"""Tests for engine/mapping.py."""
import pytest
import pandas as pd
from engine.mapping import (
    classify_file_type,
    generate_standardized_df,
    LEASE_SCHEMA,
    SALE_SCHEMA,
)


def test_classify_detects_lease():
    headers = ["Property", "Tenant", "Rent PSF", "Lease Date", "SF"]
    result = classify_file_type(headers, filename="lease_comps.xlsx")
    assert result == "LEASE"


def test_classify_detects_sale():
    headers = ["Property", "Sale Price", "Sale Date", "SF", "PSF"]
    result = classify_file_type(headers, filename="sale_comps.xlsx")
    assert result == "SALE"


def test_generate_standardized_df_returns_mapping():
    df = pd.DataFrame({
        "Property Name": ["Foo"],
        "Tenant Name": ["Bar"],
        "Rent PSF": [18.5],
        "Lease Date": ["2024-01-15"],
        "SF": [10000],
    })
    out_df, confidence, mappings = generate_standardized_df(df, LEASE_SCHEMA, "LEASE")
    assert isinstance(mappings, dict)
    assert len(mappings) > 0


def test_generate_standardized_df_uses_heuristic_when_embeddings_missing(monkeypatch):
    import engine.mapping

    monkeypatch.setattr(
        engine.mapping,
        "get_embeddings",
        lambda _texts: (_ for _ in ()).throw(RuntimeError("missing credentials")),
    )
    df = pd.DataFrame({
        "Property Name": ["Foo"],
        "Tenant Name": ["Bar"],
        "Rent PSF": [18.5],
        "Lease Date": ["2024-01-15"],
        "SF": [10000],
    })

    out_df, confidence, mappings = generate_standardized_df(df, LEASE_SCHEMA, "LEASE")

    assert out_df.attrs["mapping_source"] == "heuristic"
    assert mappings["address"] == "Property Name"
    assert mappings["tenant_name"] == "Tenant Name"
    assert mappings["rate_psf"] == "Rent PSF"
    assert mappings["leased_sf"] == "SF"
