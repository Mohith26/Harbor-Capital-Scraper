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
