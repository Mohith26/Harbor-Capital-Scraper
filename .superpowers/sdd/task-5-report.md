# Task 5 Report: Mapping-audit final-review polish

## Summary

Applied all 4 minor, low-risk final-review fixes for the mapping-audit feature. Theme: the
audit stays advisory and degrades to `[]` on ANY error, but genuinely unexpected failures
must be visible in logs (never silently swallowed), while the EXPECTED degradation path
(LLM call failing / no API key) stays low-noise at `debug`.

## Final-review fixes

### Fix A — log levels on the degrade paths

1. `web/routes/upload.py` — `_attach_audit_flags._flags_for`'s `except Exception:` block
   (catches genuinely-unexpected errors, e.g. a malformed DataFrame) changed from
   `log.debug(...)` to `log.warning(...)`.
2. `engine/mapping_audit.py` — `audit_segment`'s `except Exception:` around
   `verify_mapping` (defense-in-depth for unexpected errors) changed from `log.debug(...)`
   to `log.warning(...)`.
3. `engine/verify_mapping.py` — this file had no logger. Added `import logging` and
   `log = logging.getLogger(__name__)`, and added a `log.debug(...)` trace line in the
   `except Exception:` around the `_chat_json` call (kept at `debug` deliberately — this is
   the EXPECTED LLM-degradation path, e.g. no API key, and must stay quiet by default).

### Fix B — belt-and-suspenders try/except at the audit call site

`web/routes/upload.py`'s `upload_file` route: the `_attach_audit_flags(...)` call sits after
the function's own try/except, so an unforeseen error there would previously 500 the whole
upload. Wrapped the call in its own `try/except Exception: log.warning(..., exc_info=True)`
so the "audit never crashes the upload" guarantee is now structural, not incidental.

### Fix C — regression test for the widened-try (pre-audit) failure mode

Added `test_attach_audit_flags_degrades_when_segment_malformed` to
`tests/test_upload_route.py`. Constructs a segment with `mapping_result = None` (so
`.mapping_result.mappings` raises inside `_flags_for`'s try block, before `audit_segment` is
ever called), monkeypatches `audit_segment` to raise `AssertionError` if reached (fails loudly
if the guard regresses), and asserts `audit_flags == []`.

### Fix D — ops documentation

Appended a "Mapping audit layer (COMP_MAPPING_AUDIT)" section to `APP_OVERVIEW.md`
documenting the env var (default ON, `0` disables), and the cost/latency/timeout/degrade
behavior.

## Commands run and output

```
.venv/bin/python -m pytest tests/test_upload_route.py tests/test_mapping_audit.py tests/test_verify_mapping.py -v
```
Result: `24 passed, 1 warning in 1.35s` (all green, including the new
`test_attach_audit_flags_degrades_when_segment_malformed`).

```
.venv/bin/python -m pytest -q
```
Result: `1 failed, 146 passed, 5 skipped, 1 warning in 1.80s`. The 1 failure is the
pre-existing, unrelated `tests/test_finder.py::test_match_scores_are_spread_not_clustered`
(a CoStar-column schema-migration issue in the sqlite test fixture, unrelated to mapping-audit
— present before this task, no new failures introduced).

## Files touched

- `web/routes/upload.py`
- `engine/mapping_audit.py`
- `engine/verify_mapping.py`
- `tests/test_upload_route.py`
- `APP_OVERVIEW.md`
- `.superpowers/sdd/task-5-report.md`
