import pathlib
import pandas as pd
import pytest
from engine.loaders import (
    get_sheet_names,
    robust_load_file,
    robust_load_file_segmented,
    _trim_leading_empty_columns,
    _merge_split_headers,
)

# Resolve the sample files directory whether running from repo root or a worktree
_HERE = pathlib.Path(__file__).parent
SAMPLE_DIR = None
for _candidate in [
    _HERE.parent / "sample comp files",                               # repo root checkout
    _HERE.parent.parent.parent.parent / "sample comp files",          # worktree inside .claude/worktrees
]:
    if _candidate.exists():
        SAMPLE_DIR = _candidate
        break

SAMPLE = str(SAMPLE_DIR / "Arlington Class B Comps.xlsx") if SAMPLE_DIR else None
_needs_sample = pytest.mark.skipif(SAMPLE is None, reason="'sample comp files' directory not present in this checkout")


@_needs_sample
def test_get_sheet_names_returns_list():
    names = get_sheet_names(SAMPLE)
    assert isinstance(names, list) and len(names) >= 1

@_needs_sample
def test_robust_load_file_returns_dataframe():
    df = robust_load_file(SAMPLE)
    assert isinstance(df, pd.DataFrame)
    assert len(df.columns) > 0
    assert len(df) > 0

@_needs_sample
def test_robust_load_file_segmented_returns_list_of_segments():
    segments = robust_load_file_segmented(SAMPLE)
    assert isinstance(segments, list)
    assert len(segments) >= 1
    assert all(isinstance(s['df'], pd.DataFrame) for s in segments)


def test_trim_leading_empty_columns_tolerates_duplicate_labels():
    """A merged-header sheet can land here with duplicate column names
    (e.g. several trailing 'Unknown' columns). df[col] then returns a
    DataFrame, not a Series — earlier code did `if col_data.isna().all()`
    which raised "The truth value of a Series is ambiguous". Regression
    guard for that crash. Also asserts the leading empty col is dropped.
    """
    df = pd.DataFrame({
        "A": [None, None, None],
        "B": [1, 2, 3],
        "C": [4, 5, 6],
        "D": [7, 8, 9],
    })
    # Force a duplicate label by reassigning columns (simulating what
    # _merge_split_headers produces when several header cells are blank).
    df.columns = ["LEAD_EMPTY", "Unknown", "Unknown", "Unknown"]
    out = _trim_leading_empty_columns(df)
    assert list(out.columns) == ["Unknown", "Unknown", "Unknown"]
    assert len(out) == 3


def test_merge_split_headers_uses_positional_placeholder_for_blank_cells():
    """When a sub-header cell is empty AND the parent column is Unnamed,
    the merged label used to collapse to literally 'Unknown' — yielding
    duplicates that broke downstream label-based lookups. Each blank cell
    now gets a positional placeholder like 'Unknown_3'."""
    # Sub-header row is fully blank → merge collapses to per-column placeholder
    df = pd.DataFrame({
        0: ["", "data1", "data2"],
        1: ["", "data1", "data2"],
        2: ["", "data1", "data2"],
    })
    df.columns = ["Unnamed_0", "Unnamed_1", "Unnamed_2"]
    out = _merge_split_headers(df)
    # The function may decide the all-blank sub-row is not a sub-header and
    # return df unchanged; but if it DID merge, no two columns may share a
    # literal "Unknown" label.
    unknowns = [c for c in out.columns if c == "Unknown"]
    assert len(unknowns) <= 1, \
        f"multiple bare 'Unknown' columns emitted: {list(out.columns)}"
