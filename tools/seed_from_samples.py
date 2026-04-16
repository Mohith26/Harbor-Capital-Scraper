"""Walk every file in `sample comp files/` and, for those we have ground-truth
mappings in `learning_data/ground_truth/<filename>.json`, record them into the
learning store.

Idempotent — safe to re-run. Uses SqliteLearningStore at `learning_local.db`
by default; override with LEARNING_DB_URL.
"""
from __future__ import annotations

import json
import os
import pathlib
import sys

sys.path.insert(0, ".")

from engine.loaders import robust_load_file_segmented, get_sheet_names
from engine.mapping import classify_file_type
from engine.fingerprint import compute_fingerprint
from learning.store import SqliteLearningStore


SAMPLE_DIR = pathlib.Path("sample comp files")
GROUND_TRUTH_DIR = pathlib.Path("learning_data/ground_truth")


def main():
    db_url = os.environ.get("LEARNING_DB_URL", "sqlite:///learning_local.db")
    store = SqliteLearningStore(engine_url=db_url)

    count = 0
    for path in sorted(SAMPLE_DIR.iterdir()):
        if path.suffix.lower() not in {".xlsx", ".xls"}:
            continue

        gt_path = GROUND_TRUTH_DIR / f"{path.stem}.json"
        if not gt_path.exists():
            print(f"skip (no ground truth): {path.name}")
            continue

        with gt_path.open() as fh:
            ground_truth = json.load(fh)

        for sheet in get_sheet_names(str(path)):
            segments = robust_load_file_segmented(str(path), sheet_name=sheet)
            for seg_idx, seg in enumerate(segments):
                segment_key = f"{sheet}::{seg_idx}"
                if segment_key not in ground_truth:
                    continue
                mappings = ground_truth[segment_key]["mappings"]
                df = seg.get("df") if isinstance(seg, dict) else seg
                if hasattr(df, "columns"):
                    headers = [str(c) for c in df.columns]
                else:
                    continue
                file_type = classify_file_type(headers, filename=path.name, sheet_name=sheet)
                fp = compute_fingerprint(headers, path.name, sheet, file_type)
                store.record_accepted_mapping(
                    fingerprint=fp, mappings=mappings, confirmed_by="seed"
                )
                count += 1
                print(f"seeded {path.name}::{segment_key}")

    print(f"\nTotal fingerprints seeded: {count}")


if __name__ == "__main__":
    main()
