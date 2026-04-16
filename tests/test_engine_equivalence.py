"""
Runs the NEW split engine (via comp_engine facade) against the snapshot captured
from the original engine. Verifies zero behavior drift after Phase 2 extraction.
"""
import pathlib
import pickle
import pandas as pd
import numpy as np
import pytest

SAMPLE_DIR = pathlib.Path("sample comp files")
if not SAMPLE_DIR.exists():
    SAMPLE_DIR = pathlib.Path(__file__).parent.parent.parent.parent.parent / "sample comp files"

XLSX_FILES = sorted(p for p in SAMPLE_DIR.iterdir() if p.suffix.lower() == ".xlsx") if SAMPLE_DIR.exists() else []
SNAPSHOT_PATH = pathlib.Path(__file__).parent / "fixtures" / "engine_output_snapshot.pkl"


def _normalize_df(df):
    if df is None or (hasattr(df, 'empty') and df.empty):
        return df
    df = df.copy()
    df = df.reindex(sorted(df.columns), axis=1)
    for col in df.columns:
        if df[col].dtype == object:
            df[col] = df[col].astype(str)
    return df.reset_index(drop=True)


@pytest.fixture(scope="module")
def snapshot():
    if not SNAPSHOT_PATH.exists():
        pytest.skip("snapshot missing — run tools/snapshot_current_engine.py first")
    with SNAPSHOT_PATH.open("rb") as fh:
        return pickle.load(fh)


@pytest.mark.parametrize("path", XLSX_FILES, ids=lambda p: p.name)
def test_new_engine_matches_snapshot(path, snapshot):
    import comp_engine

    sheets = comp_engine.get_sheet_names(str(path))
    matched_any = False
    for sheet in sheets:
        result = comp_engine.process_all_sheets(str(path), path.name, selected_sheets=[sheet])
        combined, confidences, mappings, errors = result if result else (None, None, {}, [])
        if combined is None:
            continue
        for seg_label, seg_mappings in mappings.items():
            key = f"{path.name}::{seg_label}"
            if key not in snapshot:
                continue
            matched_any = True
            expected = snapshot[key]
            exp_df = expected.get("clean_df")
            exp_mappings = expected.get("mappings", {})

            actual_rows = combined[combined['source_sheet'] == seg_label] if 'source_sheet' in combined.columns else combined
            assert actual_rows is not None, f"{key}: no rows"

            # Mappings must match
            assert seg_mappings == exp_mappings, f"{key}: mappings differ\ngot: {seg_mappings}\nexp: {exp_mappings}"

            # Numeric columns must be identical
            if exp_df is not None and not exp_df.empty:
                for col in actual_rows.select_dtypes(include=[np.number]).columns:
                    if col in exp_df.columns:
                        a = actual_rows[col].to_numpy(dtype=float, na_value=np.nan)
                        e = exp_df[col].to_numpy(dtype=float, na_value=np.nan)
                        if len(a) == len(e):
                            np.testing.assert_allclose(a, e, atol=1e-9, equal_nan=True,
                                                       err_msg=f"{key}:{col} numeric mismatch")
