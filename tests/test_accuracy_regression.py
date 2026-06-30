"""Accuracy regression test against labeled ground-truth sample files.

Skips automatically when no ground-truth files exist yet. When labels are
present, asserts that the pipeline achieves ≥ 0.85 average F1 across all
labeled segments.

Run:
    pytest tests/test_accuracy_regression.py -v
"""
from __future__ import annotations

import json
import pathlib
import sys

import pytest

sys.path.insert(0, ".")

SAMPLE_DIR = pathlib.Path("sample comp files")
GROUND_TRUTH_DIR = pathlib.Path("learning_data/ground_truth")

# Collect (path, gt_path) pairs that exist on disk
LABELED_FILES = [
    (p, GROUND_TRUTH_DIR / f"{p.stem}.json")
    for p in sorted(SAMPLE_DIR.iterdir())
    if p.suffix.lower() in {".xlsx", ".xls"}
    and (GROUND_TRUTH_DIR / f"{p.stem}.json").exists()
] if SAMPLE_DIR.exists() and GROUND_TRUTH_DIR.exists() else []


def _f1(predicted: dict[str, str], expected: dict[str, str]) -> float:
    """Per-segment F1: treat each (raw_header → target_col) pair as a label."""
    pred_pairs = set(predicted.items())
    true_pairs = set(expected.items())
    if not true_pairs:
        return 1.0
    tp = len(pred_pairs & true_pairs)
    fp = len(pred_pairs - true_pairs)
    fn = len(true_pairs - pred_pairs)
    precision = tp / (tp + fp) if (tp + fp) else 0.0
    recall = tp / (tp + fn) if (tp + fn) else 0.0
    if precision + recall == 0:
        return 0.0
    return 2 * precision * recall / (precision + recall)


@pytest.mark.skipif(not LABELED_FILES, reason="no labeled sample files yet")
def test_pipeline_accuracy_regression():
    from engine.loaders import robust_load_file_segmented, get_sheet_names
    from engine.pipeline import run_mapping_stage
    from learning.store import EmptyLearningStore

    store = EmptyLearningStore()
    scores: list[float] = []

    for path, gt_path in LABELED_FILES:
        with gt_path.open() as fh:
            ground_truth = json.load(fh)

        for sheet in get_sheet_names(str(path)):
            segments = robust_load_file_segmented(str(path), sheet_name=sheet)
            for seg_idx, seg in enumerate(segments):
                segment_key = f"{sheet}::{seg_idx}"
                if segment_key not in ground_truth:
                    continue
                expected_mappings: dict[str, str] = ground_truth[segment_key]["mappings"]
                df = seg.get("df") if isinstance(seg, dict) else seg
                if not hasattr(df, "columns"):
                    continue

                result = run_mapping_stage(
                    df=df,
                    filename=path.name,
                    sheet_name=sheet,
                    store=store,
                )
                score = _f1(result.mappings, expected_mappings)
                scores.append(score)
                print(f"{path.name}::{segment_key}  F1={score:.3f}")

    if not scores:
        pytest.skip("no scoreable segments found")

    avg_f1 = sum(scores) / len(scores)
    print(f"\nAverage F1: {avg_f1:.3f} over {len(scores)} segments")
    assert avg_f1 >= 0.85, (
        f"Average F1 {avg_f1:.3f} < 0.85 — pipeline accuracy regressed"
    )


# --- LLM mapper regression on the real messy lease comp CSV (Task 6) ---
import os
import pandas as pd
import engine.pipeline as pipeline
from engine.pipeline import run_mapping_stage
from learning.fakes import FakeLearningStore

_LEASE_CSV = os.path.join(
    os.path.dirname(os.path.dirname(__file__)),
    "Additional Comps - HC.xlsx - Lease (1).csv",
)


@pytest.mark.skipif(not os.path.exists(_LEASE_CSV), reason="sample CSV not present")
def test_llm_maps_messy_lease_headers(monkeypatch):
    monkeypatch.setenv("COMP_LLM_MAPPER", "1")
    pipeline._LLM_MAPPING_CACHE.clear()
    # Header row is the 2nd line; skip the title row
    df = pd.read_csv(_LEASE_CSV, skiprows=1).dropna(axis=1, how="all")

    def fake_llm(headers, sample_rows, schema, file_type, examples=None):
        m = {}
        for h in headers:
            hl = str(h).lower()
            if "base rent" in hl:
                m[h] = "rate_psf"
            elif "area leased" in hl:
                m[h] = "leased_sf"
            elif hl == "tenant":
                m[h] = "tenant_name"
            elif "address" in hl:
                m[h] = "address"
        return {"mappings": m, "confidence": {h: 0.95 for h in m},
                "unmapped": [h for h in headers if h not in m], "reasoning": "test"}

    monkeypatch.setattr(pipeline, "llm_map_columns", fake_llm)
    monkeypatch.setattr(pipeline, "verify_mapping",
                        lambda m, rows, schema: {"adjusted_confidence": {}, "flags": []})

    result = run_mapping_stage(df, "Additional Comps Lease.csv", "Lease", FakeLearningStore())
    targets = set(result.mappings.values())
    assert "rate_psf" in targets
    assert "leased_sf" in targets
    assert "tenant_name" in targets
