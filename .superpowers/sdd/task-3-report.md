# Task 3 Report: Wire the mapping audit into `upload_file`

## What was implemented

`web/routes/upload.py`:
1. Added `from concurrent.futures import ThreadPoolExecutor` after `import threading` (top imports).
2. Added `from engine.mapping_audit import audit_segment` after the `from engine.mapping import SALE_SCHEMA, LEASE_SCHEMA, dedupe_mappings_by_target` line.
3. Added a new `_AUDIT_MAX_WORKERS = 8` constant and the `_attach_audit_flags(job: dict, segments_data: list[dict]) -> None` helper, inserted immediately after `_rate_header_for_mapping` and immediately before `@router.get("", response_class=HTMLResponse)`. The helper:
   - Builds a `segment_key -> SegmentResult` lookup and reads `job["raw_dfs"]`.
   - For each `segments_data` entry, resolves the matching segment + raw df; if either is missing, returns `[]` for that entry (handles the "no raw df" / "no matching segment" degradation case).
   - Computes `mapped_headers` (mapping keys that are actually present as raw_df columns); if none, returns `[]`.
   - Samples up to 5 rows of the mapped columns (`raw_df[mapped_headers].head(5).to_dict("records")`) and calls `audit_segment(mappings, sample_rows, seg.fingerprint.file_type)` inside a `try/except Exception: return []` (per-segment degradation on any error, including monkeypatched `RuntimeError`).
   - Runs all entries concurrently via `ThreadPoolExecutor(max_workers=min(_AUDIT_MAX_WORKERS, len(segments_data)))` and writes `entry["audit_flags"] = flags` back onto each `segments_data` dict in place, preserving order via `zip(segments_data, results)`.
   - No-ops immediately if `segments_data` is empty.
4. Wired the call into `upload_file`: inserted `_attach_audit_flags(_jobs[job_id], segments_data)` (with the audit-layer comment from the brief) directly after the `except Exception as e: return HTMLResponse(...)` block that closes the try, and immediately before `# Determine schema fields for mapping dropdowns` / `first_type = ...`. This covers both the Excel/CSV and PDF branches uniformly since both populate `_jobs[job_id]` and `segments_data` before this point.

`tests/test_upload_route.py`: appended the three tests from the brief verbatim, using the existing `_segment` helper already defined in the file:
- `test_attach_audit_flags_populates_segments_data`
- `test_attach_audit_flags_degrades_to_empty_on_error`
- `test_attach_audit_flags_handles_missing_raw_df`

No other files were modified. `apply_mappings`, `save_to_db`, and the geocode path (`start_geocode` / `_geocode_thread`) were left untouched.

## TDD evidence

### RED

Command:
```
.venv/bin/python -m pytest tests/test_upload_route.py -k audit_flags -v
```

Output (tail):
```
tests/test_upload_route.py::test_attach_audit_flags_populates_segments_data FAILED
tests/test_upload_route.py::test_attach_audit_flags_degrades_to_empty_on_error FAILED
tests/test_upload_route.py::test_attach_audit_flags_handles_missing_raw_df FAILED

E       AttributeError: <module 'web.routes.upload' from '.../web/routes/upload.py'> has no attribute 'audit_segment'
...
3 failed, 8 deselected, 1 warning in 1.25s
```

This confirms the tests were exercising real (missing) behavior: `web.routes.upload` had neither `audit_segment` imported nor `_attach_audit_flags` defined yet, so `monkeypatch.setattr(up, "audit_segment", ...)` failed immediately (for tests 2 and 3) and `up._attach_audit_flags(...)` would have raised `AttributeError` (for test 1, masked here since `audit_segment` isn't even a module attribute yet to monkeypatch — same root cause: nothing implemented).

### GREEN

Implemented imports, `_attach_audit_flags`, and the call site as described above.

Command:
```
.venv/bin/python -m pytest tests/test_upload_route.py -k audit_flags -v
```

Output:
```
tests/test_upload_route.py::test_attach_audit_flags_populates_segments_data PASSED [ 33%]
tests/test_upload_route.py::test_attach_audit_flags_degrades_to_empty_on_error PASSED [ 66%]
tests/test_upload_route.py::test_attach_audit_flags_handles_missing_raw_df PASSED [100%]

3 passed, 8 deselected, 1 warning in 1.03s
```

## Full test results (no-regression check)

Command:
```
.venv/bin/python -m pytest tests/test_upload_route.py tests/test_mapping_audit.py tests/test_verify_mapping.py -q
```

Output:
```
......................                                                   [100%]
22 passed, 1 warning in 0.75s
```

Full repo suite (broader sanity check, per Global Constraints "introduce no NEW failures"):
```
.venv/bin/python -m pytest -q
```

Output (tail):
```
FAILED tests/test_finder.py::test_match_scores_are_spread_not_clustered - sqlalchemy.exc.OperationalError: (sqlite3.OperationalError) no such column: sale_comps.costar_property_id
1 failed, 144 passed, 5 skipped, 1 warning in 1.89s
```

This is the pre-existing, unrelated failure called out in the task brief (a stale SQLite test-fixture schema missing a `costar_property_id` column — unrelated to mapping/audit). No new failures were introduced; the failure count matches "1 pre-existing failure to ignore."

## Concerns

None. The implementation:
- Never mutates anything outside the `segments_data` entries (reads `job["segments"]` / `job["raw_dfs"]` read-only).
- Degrades to `[]` per-segment on: missing segment, missing raw df, no mapped headers present in the raw df, or any exception from `audit_segment` (including the monkeypatched `RuntimeError`).
- Bounds concurrency via `ThreadPoolExecutor(max_workers=min(8, len(segments_data)))`, so it never over-spawns threads for small uploads and never blocks longer than the slowest single-segment audit call.
- Preserves ordering (`zip(segments_data, results)`), so the `audit_flags` attached to each entry line up with the correct segment even though execution is concurrent.
- Does not touch `apply_mappings`, `save_to_db`, or the geocode path — confirmed by reviewing `web/routes/upload.py` diff scope (only imports, one new helper, and one two-line call site were added).
- A no-flag preview remains byte-identical: `_attach_audit_flags` only adds a new `audit_flags` key to each `segments_data` dict; it does not alter any existing keys (`mappings`, `preview_rows`, `schema_fields`, etc.), and `audit_segment` itself returns `[]` when `COMP_MAPPING_AUDIT=0` or on any internal failure (per Task 2's `engine/mapping_audit.py`).

## Commit

```
git add web/routes/upload.py tests/test_upload_route.py
git commit -m "feat: run mapping audit on every segment at upload, attach audit_flags"
```

## Fix (post-review)

A reviewer flagged two Important robustness gaps and two Minor items. Theme: the mapping
audit is advisory and must NEVER crash, block, or slow the upload preview — it must degrade
to an empty flag list on ANY error, and must not silently swallow errors without a log.

### Fix 1 — widen the try/except in `_attach_audit_flags` (`web/routes/upload.py`)

`_flags_for` previously computed `mappings`, `mapped_headers`, and `sample_rows` OUTSIDE
the `try` block. A malformed DataFrame (e.g. duplicate columns) or a missing
`.mapping_result.mappings` would raise there, propagate out of the `ThreadPoolExecutor`,
and crash the whole upload with an unhandled 500 (this call site sits after `upload_file`'s
own try/except, so nothing upstream was catching it). Moved the entire computation inside
the `try`, keeping only the simple `seg is None or raw_df is None` None-checks outside (they
cannot raise). On any exception, logs via the module's existing `log = logging.getLogger(__name__)`
at `log.debug(..., exc_info=True)` before returning `[]`.

### Fix 2 — bound the LLM call with a timeout (`engine/llm_mapping.py`)

`_chat_json` called the OpenAI client with no timeout, so a hung request could block the
synchronous upload preview indefinitely. Added `_LLM_TIMEOUT_SECONDS = 30` as a module-level
constant next to `_MODEL`, and passed `timeout=_LLM_TIMEOUT_SECONDS` into the
`_client().chat.completions.create(...)` call. A timeout now raises inside 30s, which the
existing degrade chain (`verify_mapping`'s try/except → `audit_segment` → `_flags_for`) turns
into `[]`. No other behavior in `_chat_json` changed.

### Fix 3 — add logging to the audit seam's degrade path (`engine/mapping_audit.py`)

The `except Exception: return []` wrapping `verify_mapping` inside `audit_segment` swallowed
errors with zero signal, violating the "never silently swallow errors" rule. Added
`import logging` + `log = logging.getLogger(__name__)` at module top, and added
`log.debug("mapping audit verify_mapping failed (file_type=%s)", file_type, exc_info=True)`
immediately before the `return []` in that except block. Kill-switch/normalization logic
elsewhere in `audit_segment` was left untouched.

### Fix 4 — multi-segment / no-mapped-headers test (`tests/test_upload_route.py`)

Appended `test_attach_audit_flags_multi_segment_and_no_mapped_headers`, which exercises real
concurrency across 3 segments processed through the `ThreadPoolExecutor` in `_attach_audit_flags`,
and covers the "mapped header absent from raw_df" degrade branch (segment C maps `GHOST` -> `buyer`,
but `GHOST` is not a column in `raw_c`, so `mapped_headers` is empty and the segment gets `[]`
without ever calling `audit_segment`). Segments A and B assert `audit_flags` are correctly
attached and ordered per-segment despite concurrent execution.

### Commands run and output

```
.venv/bin/python -m pytest tests/test_upload_route.py tests/test_mapping_audit.py tests/test_verify_mapping.py -v
```
Result: `23 passed, 1 warning in 1.08s` (all pass, including the new multi-segment test).

```
.venv/bin/python -m pytest -q
```
Result: `1 failed, 145 passed, 5 skipped, 1 warning in 1.46s` — the single failure is the
pre-existing, unrelated `tests/test_finder.py::test_match_scores_are_spread_not_clustered`
(`sqlite3.OperationalError: no such column: sale_comps.costar_property_id`, a stale
SQLite test-fixture schema issue, unrelated to the mapping audit). No new failures were
introduced (145 passed here vs. 144 passed previously, reflecting the one new test added).

### Commit

```
git add web/routes/upload.py engine/llm_mapping.py engine/mapping_audit.py tests/test_upload_route.py .superpowers/sdd/task-3-report.md
git commit -m "fix: harden mapping audit — widen degrade scope, bound LLM timeout, log on degrade"
```
