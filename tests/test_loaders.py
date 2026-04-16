import pathlib
import pandas as pd
from engine.loaders import get_sheet_names, robust_load_file, robust_load_file_segmented

# Resolve the sample files directory whether running from repo root or a worktree
_HERE = pathlib.Path(__file__).parent
for _candidate in [
    _HERE.parent / "sample comp files",                               # repo root checkout
    _HERE.parent.parent.parent.parent / "sample comp files",          # worktree inside .claude/worktrees
]:
    if _candidate.exists():
        SAMPLE_DIR = _candidate
        break
else:
    raise RuntimeError("Cannot find 'sample comp files' directory")

SAMPLE = str(SAMPLE_DIR / "Arlington Class B Comps.xlsx")

def test_get_sheet_names_returns_list():
    names = get_sheet_names(SAMPLE)
    assert isinstance(names, list) and len(names) >= 1

def test_robust_load_file_returns_dataframe():
    df = robust_load_file(SAMPLE)
    assert isinstance(df, pd.DataFrame)
    assert len(df.columns) > 0
    assert len(df) > 0

def test_robust_load_file_segmented_returns_list_of_segments():
    segments = robust_load_file_segmented(SAMPLE)
    assert isinstance(segments, list)
    assert len(segments) >= 1
    assert all(isinstance(s['df'], pd.DataFrame) for s in segments)
