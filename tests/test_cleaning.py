"""Tests for engine/cleaning.py."""
import pandas as pd
from engine.cleaning import clean_header, get_column_profile, apply_rate_logic, _to_float


def test_clean_header_lowercases_and_strips():
    assert clean_header("  Rent PSF  ") == "rent psf"
    assert clean_header(None) == "none"  # str(None).lower() = 'none'


def test_column_profile_detects_numeric():
    s = pd.Series([1.0, 2.5, 3.7, None])
    profile = get_column_profile(s)
    assert profile in ("numeric_clean", "numeric_money")


def test_to_float_parses_currency():
    assert _to_float("$1,234.56") == 1234.56
    assert _to_float("—") is None
    assert _to_float(None) is None


def test_apply_rate_logic_splits_monthly_annual():
    df = pd.DataFrame({"rate_psf": [1.50, 18.00, 1.25, 24.00]})
    result = apply_rate_logic(df, rate_header=None)
    assert "rate_monthly" in result.columns
    assert "rate_annually" in result.columns
    assert "rate_basis" in result.columns
