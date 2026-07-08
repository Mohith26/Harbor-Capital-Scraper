# Post-Mapping Audit Layer Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** After the scrape/mapping finishes and before the analyst edits the upload preview, run a separate LLM that audits each segment's assigned column mapping against the actual cell values and surfaces contradictions as advisory, one-click-fixable flags.

**Architecture:** Extend the existing `engine/verify_mapping.py` audit to also return a `suggested_field`. Add a small `engine/mapping_audit.py` seam that runs the audit for one segment and normalizes the result. Call it for every segment (all mapping tiers) at the end of `upload_file`, concurrently, attaching `audit_flags` to each preview segment. The Alpine preview renders a summary banner, per-column `⚠` markers, and a "Change to `<field>`" button that reuses the existing `setMapping`.

**Tech Stack:** Python 3.9, FastAPI, pandas, OpenAI (gpt-4o via `engine.llm_mapping._chat_json`), Jinja2 + Alpine.js, pytest.

## Global Constraints

- Python interpreter for all commands: `.venv/bin/python` (Python 3.9.6).
- Run tests with `.venv/bin/python -m pytest`.
- Advisory only — the audit MUST NEVER block or slow the geocode/save path, and MUST degrade to `[]` (empty flags) when disabled, keyless, or on any error. A no-flag preview must be byte-identical to today.
- Kill switch: env var `COMP_MAPPING_AUDIT`, default ON; disabled only when set to exactly `"0"` (mirrors `COMP_LLM_MAPPER`).
- Audit model default `gpt-4o` (unchanged from the existing verifier), `temperature=0`, ≤5 sample rows.
- Immutability preference: build new dicts/lists; do not mutate schema dicts. (Attaching `audit_flags` to the freshly-built `segments_data` entries is fine — they are new dicts created in this request.)
- SALE_SCHEMA keys: `address, sale_price, building_size, price_per_sf, closing_date, year_built, cap_rate, buyer, seller, notes`.
- LEASE_SCHEMA keys: `address, tenant_name, leased_sf, rate_psf, lease_type, term_months, commencement_date, escalations, ti_allowance, free_rent, clear_height, building_type, year_built, notes`.

---

### Task 1: Extend `verify_mapping` with `suggested_field`

**Files:**
- Modify: `engine/verify_mapping.py`
- Test: `tests/test_verify_mapping.py` (create)

**Interfaces:**
- Consumes: `engine.llm_mapping._chat_json(prompt, model=...) -> dict` (existing).
- Produces: `verify_mapping(mappings: dict[str,str], sample_rows: list[dict], schema: dict) -> dict` returning
  `{"adjusted_confidence": {header: float}, "flags": [{"header": str, "reason": str, "suggested_field": str | None}]}`.
  The `flags` list now carries `suggested_field` (raw pass-through of the LLM's value: a non-empty string or `None`). Headers not in `mappings` are still dropped. `adjusted_confidence` is unchanged.

- [ ] **Step 1: Write the failing tests**

Create `tests/test_verify_mapping.py`:

```python
import engine.verify_mapping as vm

_SCHEMA = {
    "tenant_name": {"desc": "tenant", "type": "text"},
    "closing_date": {"desc": "closing date", "type": "date"},
}


def test_verify_mapping_parses_suggested_field(monkeypatch):
    monkeypatch.setattr(
        vm,
        "_chat_json",
        lambda prompt, model=None: {
            "adjusted_confidence": {"CLOSE DATE": 0.1},
            "flags": [
                {"header": "CLOSE DATE", "reason": "values are dates", "suggested_field": "closing_date"}
            ],
        },
    )

    out = vm.verify_mapping({"CLOSE DATE": "tenant_name"}, [{"CLOSE DATE": "5/1/22"}], _SCHEMA)

    assert out["flags"] == [
        {"header": "CLOSE DATE", "reason": "values are dates", "suggested_field": "closing_date"}
    ]
    assert out["adjusted_confidence"] == {"CLOSE DATE": 0.1}


def test_verify_mapping_missing_suggested_field_is_none(monkeypatch):
    monkeypatch.setattr(
        vm,
        "_chat_json",
        lambda prompt, model=None: {"flags": [{"header": "CLOSE DATE", "reason": "dates"}]},
    )

    out = vm.verify_mapping({"CLOSE DATE": "tenant_name"}, [], _SCHEMA)

    assert out["flags"][0]["suggested_field"] is None


def test_verify_mapping_drops_flags_for_unknown_headers(monkeypatch):
    monkeypatch.setattr(
        vm,
        "_chat_json",
        lambda prompt, model=None: {
            "flags": [{"header": "GHOST", "reason": "x", "suggested_field": "closing_date"}]
        },
    )

    out = vm.verify_mapping({"CLOSE DATE": "tenant_name"}, [], _SCHEMA)

    assert out["flags"] == []


def test_verify_mapping_llm_error_is_noop(monkeypatch):
    def _raise(*a, **k):
        raise RuntimeError("api down")

    monkeypatch.setattr(vm, "_chat_json", _raise)

    out = vm.verify_mapping({"CLOSE DATE": "tenant_name"}, [], _SCHEMA)

    assert out == {"adjusted_confidence": {}, "flags": []}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `.venv/bin/python -m pytest tests/test_verify_mapping.py -v`
Expected: `test_verify_mapping_parses_suggested_field` and `test_verify_mapping_missing_suggested_field_is_none` FAIL (returned flag dicts lack the `suggested_field` key). The other two PASS (already correct behavior).

- [ ] **Step 3: Update the prompt to request `suggested_field`**

In `engine/verify_mapping.py`, replace the final return string in `_build_prompt` (the block starting `'Return ONLY JSON: {"adjusted_confidence"...'`) with:

```python
        'Return ONLY JSON: {"adjusted_confidence": {"<raw_header>": 0.0-1.0}, '
        '"flags": [{"header": "<raw_header>", "reason": "<why it is suspicious>", '
        '"suggested_field": "<the target_field the VALUES actually look like, chosen '
        'ONLY from the target schema above, or null if unclear>"}]}. '
        "Only include headers you are adjusting or flagging."
```

- [ ] **Step 4: Parse `suggested_field` in the return builder**

In `engine/verify_mapping.py::verify_mapping`, replace the `flags = [...]` list comprehension with:

```python
    flags = []
    for f in (raw.get("flags") or []):
        header = f.get("header", "")
        if header not in mappings:
            continue
        suggested = f.get("suggested_field")
        if not isinstance(suggested, str) or not suggested.strip():
            suggested = None
        flags.append({
            "header": header,
            "reason": f.get("reason", ""),
            "suggested_field": suggested,
        })
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `.venv/bin/python -m pytest tests/test_verify_mapping.py -v`
Expected: 4 passed.

- [ ] **Step 6: Commit**

```bash
git add engine/verify_mapping.py tests/test_verify_mapping.py
git commit -m "feat: verify_mapping returns suggested_field per flag"
```

---

### Task 2: `engine/mapping_audit.py` — per-segment audit seam

**Files:**
- Create: `engine/mapping_audit.py`
- Test: `tests/test_mapping_audit.py` (create)

**Interfaces:**
- Consumes: `engine.verify_mapping.verify_mapping(...)` (Task 1); `engine.mapping.SALE_SCHEMA`, `engine.mapping.LEASE_SCHEMA`.
- Produces: `audit_segment(mappings: dict[str,str], sample_rows: list[dict], file_type: str) -> list[dict]`.
  Each item: `{"header": str, "reason": str, "suggested_field": str | None}`. Returns `[]` when the kill switch is off, `mappings` is empty, or the audit raises. Normalizes `suggested_field` to `None` when it is not a valid schema field for `file_type` or equals the header's current mapping.

- [ ] **Step 1: Write the failing tests**

Create `tests/test_mapping_audit.py`:

```python
import engine.mapping_audit as ma


def test_audit_segment_normalizes_suggestions(monkeypatch):
    monkeypatch.setenv("COMP_MAPPING_AUDIT", "1")
    monkeypatch.setattr(
        ma,
        "verify_mapping",
        lambda mappings, rows, schema: {
            "adjusted_confidence": {},
            "flags": [
                {"header": "CLOSE DATE", "reason": "dates", "suggested_field": "closing_date"},
                {"header": "BUYER", "reason": "weird", "suggested_field": "not_a_field"},
                {"header": "SELLER", "reason": "same", "suggested_field": "seller"},
            ],
        },
    )
    mappings = {"CLOSE DATE": "buyer", "BUYER": "buyer", "SELLER": "seller"}

    flags = ma.audit_segment(mappings, [{"CLOSE DATE": "5/1/22"}], "SALE")

    by = {f["header"]: f for f in flags}
    assert by["CLOSE DATE"]["suggested_field"] == "closing_date"  # valid + differs from current
    assert by["BUYER"]["suggested_field"] is None                 # not a schema field
    assert by["SELLER"]["suggested_field"] is None                # equals current mapping


def test_audit_segment_killswitch_off_skips_llm(monkeypatch):
    monkeypatch.setenv("COMP_MAPPING_AUDIT", "0")

    def _boom(*a, **k):
        raise AssertionError("verify_mapping must not be called when disabled")

    monkeypatch.setattr(ma, "verify_mapping", _boom)

    assert ma.audit_segment({"A": "buyer"}, [{"A": "x"}], "SALE") == []


def test_audit_segment_empty_mappings(monkeypatch):
    monkeypatch.setenv("COMP_MAPPING_AUDIT", "1")
    assert ma.audit_segment({}, [{"A": "x"}], "SALE") == []


def test_audit_segment_error_returns_empty(monkeypatch):
    monkeypatch.setenv("COMP_MAPPING_AUDIT", "1")

    def _raise(*a, **k):
        raise RuntimeError("api down")

    monkeypatch.setattr(ma, "verify_mapping", _raise)

    assert ma.audit_segment({"A": "buyer"}, [{"A": "x"}], "SALE") == []


def test_audit_segment_uses_lease_schema(monkeypatch):
    monkeypatch.setenv("COMP_MAPPING_AUDIT", "1")
    monkeypatch.setattr(
        ma,
        "verify_mapping",
        lambda mappings, rows, schema: {
            "flags": [{"header": "H", "reason": "r", "suggested_field": "tenant_name"}]
        },
    )

    flags = ma.audit_segment({"H": "rate_psf"}, [{"H": "Acme"}], "LEASE")

    assert flags[0]["suggested_field"] == "tenant_name"  # valid LEASE field
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `.venv/bin/python -m pytest tests/test_mapping_audit.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'engine.mapping_audit'`.

- [ ] **Step 3: Create the module**

Create `engine/mapping_audit.py`:

```python
"""Post-mapping audit layer — surfaces data-vs-mapping contradictions to the analyst.

Runs after the mapping stage on EVERY segment (all tiers), before the analyst
edits the preview. Advisory only: degrades to an empty list when disabled, when
there is no API key, or on any error.
"""
from __future__ import annotations

import os

from engine.mapping import LEASE_SCHEMA, SALE_SCHEMA
from engine.verify_mapping import verify_mapping

_MAX_SAMPLE_ROWS = 5


def _audit_enabled() -> bool:
    return os.environ.get("COMP_MAPPING_AUDIT", "1") != "0"


def _schema_for(file_type: str) -> dict:
    return LEASE_SCHEMA if (file_type or "").upper() in ("LEASE", "BOTH") else SALE_SCHEMA


def audit_segment(
    mappings: dict[str, str],
    sample_rows: list[dict],
    file_type: str,
) -> list[dict]:
    """Audit one segment's mapping against its sample values.

    Returns a list of {"header", "reason", "suggested_field"} dicts (advisory).
    Empty list when disabled, when there is nothing to check, or on any error.
    """
    if not _audit_enabled() or not mappings:
        return []

    schema = _schema_for(file_type)
    try:
        verdict = verify_mapping(mappings, list(sample_rows or [])[:_MAX_SAMPLE_ROWS], schema)
    except Exception:
        return []

    valid_targets = set(schema.keys())
    flags: list[dict] = []
    for f in verdict.get("flags") or []:
        header = f.get("header", "")
        if header not in mappings:
            continue
        suggested = f.get("suggested_field")
        if suggested not in valid_targets or suggested == mappings.get(header):
            suggested = None
        flags.append({
            "header": header,
            "reason": f.get("reason", ""),
            "suggested_field": suggested,
        })
    return flags
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `.venv/bin/python -m pytest tests/test_mapping_audit.py -v`
Expected: 5 passed.

- [ ] **Step 5: Commit**

```bash
git add engine/mapping_audit.py tests/test_mapping_audit.py
git commit -m "feat: add mapping_audit.audit_segment per-segment audit seam"
```

---

### Task 3: Wire the audit into `upload_file`

**Files:**
- Modify: `web/routes/upload.py` (imports near top; new `_attach_audit_flags` helper; call site in `upload_file`)
- Test: `tests/test_upload_route.py` (extend)

**Interfaces:**
- Consumes: `engine.mapping_audit.audit_segment(...)` (Task 2); job dict shape `{"segments": [SegmentResult], "raw_dfs": {segment_key: DataFrame}}`; `segments_data` entries carrying `"segment_key"`.
- Produces: `_attach_audit_flags(job: dict, segments_data: list[dict]) -> None` — mutates each `segments_data` entry in place, setting `entry["audit_flags"]` to a list (`[]` when degraded). Runs segments concurrently.

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_upload_route.py`:

```python
def test_attach_audit_flags_populates_segments_data(monkeypatch):
    import web.routes.upload as up

    raw_df = pd.DataFrame([{"CLOSE DATE": "5/1/22", "PRICE": "$10,000,000"}])
    seg = _segment("Sheet::0", "Sheet", "SALE", raw_df)
    job = {"segments": [seg], "raw_dfs": {seg.segment_key: raw_df.copy()}}
    segments_data = [{"segment_key": seg.segment_key, "voided": False}]

    monkeypatch.setattr(
        up,
        "audit_segment",
        lambda mappings, sample_rows, file_type: [
            {"header": "CLOSE DATE", "reason": "dates", "suggested_field": "closing_date"}
        ],
    )

    up._attach_audit_flags(job, segments_data)

    assert segments_data[0]["audit_flags"] == [
        {"header": "CLOSE DATE", "reason": "dates", "suggested_field": "closing_date"}
    ]


def test_attach_audit_flags_degrades_to_empty_on_error(monkeypatch):
    import web.routes.upload as up

    raw_df = pd.DataFrame([{"A": "x"}])
    seg = _segment("Sheet::0", "Sheet", "SALE", raw_df)
    job = {"segments": [seg], "raw_dfs": {seg.segment_key: raw_df.copy()}}
    segments_data = [{"segment_key": seg.segment_key, "voided": False}]

    def _boom(*a, **k):
        raise RuntimeError("down")

    monkeypatch.setattr(up, "audit_segment", _boom)

    up._attach_audit_flags(job, segments_data)

    assert segments_data[0]["audit_flags"] == []


def test_attach_audit_flags_handles_missing_raw_df(monkeypatch):
    import web.routes.upload as up

    raw_df = pd.DataFrame([{"A": "x"}])
    seg = _segment("Sheet::0", "Sheet", "SALE", raw_df)
    job = {"segments": [seg], "raw_dfs": {}}  # no raw df for this segment
    segments_data = [{"segment_key": seg.segment_key, "voided": False}]

    monkeypatch.setattr(
        up,
        "audit_segment",
        lambda *a, **k: [{"header": "A", "reason": "r", "suggested_field": None}],
    )

    up._attach_audit_flags(job, segments_data)

    assert segments_data[0]["audit_flags"] == []  # skipped: no raw df to sample
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `.venv/bin/python -m pytest tests/test_upload_route.py -k audit_flags -v`
Expected: FAIL — `AttributeError: module 'web.routes.upload' has no attribute '_attach_audit_flags'`.

- [ ] **Step 3: Add imports**

In `web/routes/upload.py`, add to the imports near the top (after the existing `import threading` line):

```python
from concurrent.futures import ThreadPoolExecutor
```

And after the existing `from engine.mapping import SALE_SCHEMA, LEASE_SCHEMA, dedupe_mappings_by_target` line, add:

```python
from engine.mapping_audit import audit_segment
```

- [ ] **Step 4: Add the `_attach_audit_flags` helper**

In `web/routes/upload.py`, add this function just above `@router.get("", response_class=HTMLResponse)` (i.e. after `_rate_header_for_mapping`):

```python
_AUDIT_MAX_WORKERS = 8


def _attach_audit_flags(job: dict, segments_data: list[dict]) -> None:
    """Audit each segment's mapping against its raw sample values (advisory).

    Sets ``entry["audit_flags"]`` on every segments_data entry to a list of
    {"header", "reason", "suggested_field"} dicts (``[]`` when clean or degraded).
    Best-effort and non-fatal: a per-segment failure yields ``[]`` for that
    segment. Segments are audited concurrently to bound wall-clock latency.
    """
    seg_by_key = {s.segment_key: s for s in job.get("segments", [])}
    raw_dfs = job.get("raw_dfs", {})

    def _flags_for(entry: dict) -> list[dict]:
        seg = seg_by_key.get(entry.get("segment_key"))
        raw_df = raw_dfs.get(entry.get("segment_key"))
        if seg is None or raw_df is None:
            return []
        mappings = seg.mapping_result.mappings or {}
        mapped_headers = [h for h in mappings if h in raw_df.columns]
        if not mapped_headers:
            return []
        sample_rows = raw_df[mapped_headers].head(5).to_dict("records")
        try:
            return audit_segment(mappings, sample_rows, seg.fingerprint.file_type)
        except Exception:
            return []

    if not segments_data:
        return
    with ThreadPoolExecutor(max_workers=min(_AUDIT_MAX_WORKERS, len(segments_data))) as pool:
        results = list(pool.map(_flags_for, segments_data))
    for entry, flags in zip(segments_data, results):
        entry["audit_flags"] = flags
```

- [ ] **Step 5: Call the helper before building `preview_state`**

In `web/routes/upload.py::upload_file`, find the block after the `except Exception as e:` handler that ends the try (the line `return HTMLResponse(f'<div class="text-red-600 p-4">Error processing file: {e}</div>')`) and before `# Determine schema fields for mapping dropdowns`. Insert the audit call there:

```python
    # Audit layer: check each segment's mapping against its data before the
    # analyst edits (advisory, best-effort, degrades to no flags).
    _attach_audit_flags(_jobs[job_id], segments_data)

    # Determine schema fields for mapping dropdowns
    first_type = segments_data[0]["file_type"] if segments_data else "sale"
```

(The `# Determine schema fields...` and `first_type = ...` lines already exist — insert the two new lines immediately above them.)

- [ ] **Step 6: Run tests to verify they pass**

Run: `.venv/bin/python -m pytest tests/test_upload_route.py -k audit_flags -v`
Expected: 3 passed.

- [ ] **Step 7: Run the full upload-route + engine audit suites**

Run: `.venv/bin/python -m pytest tests/test_upload_route.py tests/test_mapping_audit.py tests/test_verify_mapping.py -q`
Expected: all passed (existing upload-route tests still green).

- [ ] **Step 8: Commit**

```bash
git add web/routes/upload.py tests/test_upload_route.py
git commit -m "feat: run mapping audit on every segment at upload, attach audit_flags"
```

---

### Task 4: Surface flags in the preview UI

**Files:**
- Modify: `web/templates/partials/upload_preview.html` (Alpine helpers, banner markup, per-column marker, clear-on-toggle)

**Interfaces:**
- Consumes: `seg.audit_flags` (list of `{header, reason, suggested_field}`) now present on each segment via `preview_state` (Task 3); existing `setMapping(segIdx, header, field)` and `changeSegmentType(segIdx, newType)` methods.
- Produces: analyst-visible summary banner, `⚠` marker on flagged raw-header cells, and one-click "Change to `<field>`" that calls `setMapping` and clears the flag.

- [ ] **Step 1: Add Alpine helper methods**

In `web/templates/partials/upload_preview.html`, inside the `uploadPreview` component object, add these methods immediately after the `changeSegmentType(...)` method's closing `},`:

```javascript
        auditFlagsFor(segIdx) {
            const seg = this.segments[segIdx];
            return (seg && seg.audit_flags) ? seg.audit_flags : [];
        },

        auditFlagForHeader(segIdx, header) {
            return this.auditFlagsFor(segIdx).find(f => f.header === header) || null;
        },

        applyAuditSuggestion(segIdx, header, field) {
            this.setMapping(segIdx, header, field);
            this.dismissAuditFlag(segIdx, header);
        },

        dismissAuditFlag(segIdx, header) {
            const seg = this.segments[segIdx];
            if (!seg || !seg.audit_flags) return;
            seg.audit_flags = seg.audit_flags.filter(f => f.header !== header);
        },
```

- [ ] **Step 2: Clear stale flags when the segment type is toggled**

In the same file, inside `changeSegmentType`, add a line that clears the segment's audit flags (they were computed for the old schema). Change:

```javascript
            seg.file_type = t;
            const fields = this.schemaFieldsByType[t] || [];
```

to:

```javascript
            seg.file_type = t;
            seg.audit_flags = [];  // flags were for the old schema; stale after a type change
            const fields = this.schemaFieldsByType[t] || [];
```

- [ ] **Step 3: Add the summary banner**

In the same file, inside `<template x-if="!seg.voided">` → its top-level `<div>`, add the banner as the FIRST child (immediately after `<div>` and before the `<!-- Schema field chips (draggable) -->` comment):

```html
                    <!-- Mapping audit warnings (advisory) -->
                    <template x-if="auditFlagsFor(segIdx).length">
                        <div class="bg-amber-50 border border-amber-300 text-amber-900 rounded-lg p-3 mb-3 text-xs">
                            <div class="font-semibold mb-2"
                                 x-text="'⚠ ' + auditFlagsFor(segIdx).length + ' possible mapping issue' + (auditFlagsFor(segIdx).length === 1 ? '' : 's') + ' found — review before saving'"></div>
                            <template x-for="flag in auditFlagsFor(segIdx)" :key="flag.header">
                                <div class="flex items-center flex-wrap gap-2 py-1 border-t border-amber-200 first:border-t-0">
                                    <span class="font-medium" x-text="flag.header"></span>
                                    <span class="text-amber-700">&rarr; <span x-text="seg.mappings[flag.header] || 'unmapped'"></span></span>
                                    <span class="text-amber-800" x-text="'— ' + flag.reason"></span>
                                    <template x-if="flag.suggested_field">
                                        <button type="button"
                                                class="btn-outline text-xs py-0.5 px-2"
                                                @click="applyAuditSuggestion(segIdx, flag.header, flag.suggested_field)"
                                                x-text="'Change to ' + flag.suggested_field"></button>
                                    </template>
                                    <button type="button" class="text-amber-600 underline"
                                            @click="dismissAuditFlag(segIdx, flag.header)">dismiss</button>
                                </div>
                            </template>
                        </div>
                    </template>

```

- [ ] **Step 4: Add a per-column `⚠` marker on flagged headers**

In the same file, in the raw-data preview table's header cell (`<th ...>`), after the two existing `<template x-if="...mappings[header]...">` badge blocks and before the closing `</th>`, add:

```html
                                            <template x-if="auditFlagForHeader(segIdx, header)">
                                                <span class="ml-1 text-amber-600 cursor-help"
                                                      :title="auditFlagForHeader(segIdx, header).reason">&#9888;</span>
                                            </template>
```

- [ ] **Step 5: Verify the template renders (smoke test via the running app)**

There is no frontend unit-test harness in this repo, so verify by rendering the real preview:

Run the app locally:
```bash
COMP_MAPPING_AUDIT=0 .venv/bin/python -m uvicorn web.main:app --port 8011
```
(Use `COMP_MAPPING_AUDIT=0` first so no LLM/key is needed.) Log in, upload any comp spreadsheet, and confirm the preview renders exactly as before (no banner, no `⚠`), the SALE/LEASE toggle still works, and the dropdown editor + chips still work. Stop the server.

Expected: preview identical to current behavior with the audit disabled (proves the markup degrades cleanly).

- [ ] **Step 6: Verify flags render + one-click fix (with a stubbed flag)**

Temporarily confirm the UI path without paying for an LLM: in a Python shell, monkeypatch is not available in-app, so instead verify with the audit ENABLED against a file you know misclassifies (e.g. an `IOS Sale Comps` file whose inner sheet is named "Lease Comps"), OR temporarily hardcode a flag by setting `entry["audit_flags"]` in `_attach_audit_flags` to a single test flag, load the page, and confirm: (a) banner shows the count, (b) the flagged column header shows `⚠` with the reason on hover, (c) "Change to `<field>`" re-maps the column (badge updates) and removes the flag, (d) "dismiss" removes the flag. Revert any temporary hardcoding before committing.

Expected: all four behaviors work; no console errors.

- [ ] **Step 7: Commit**

```bash
git add web/templates/partials/upload_preview.html
git commit -m "feat: surface mapping-audit flags in upload preview with one-click fix"
```

---

### Task 5: Full regression + verification pass

**Files:** none (verification only)

- [ ] **Step 1: Run the full test suite**

Run: `.venv/bin/python -m pytest -q`
Expected: all tests pass (baseline suite + the new `test_verify_mapping.py`, `test_mapping_audit.py`, and the added `test_upload_route.py` cases). If any pre-existing test needs live network/embeddings and is skipped/xfail in this environment, note it — do not "fix" by weakening the new tests.

- [ ] **Step 2: Confirm the kill switch and offline degradation end-to-end**

With no `OPENAI_API_KEY` set (or `COMP_MAPPING_AUDIT=0`), confirm via `_attach_audit_flags` behavior that flags come back `[]` and the preview is unaffected. This is already covered by unit tests; spot-check by running:

Run: `COMP_MAPPING_AUDIT=0 .venv/bin/python -m pytest tests/test_mapping_audit.py -q`
Expected: passed.

- [ ] **Step 3: Final commit (if any doc/cleanup changes remain)**

```bash
git add -A
git commit -m "chore: mapping audit layer verification pass" || echo "nothing to commit"
```

---

## Self-Review

**Spec coverage:**
- §2.1 extend `verify_mapping` → Task 1. ✓
- §2.2 `engine/mapping_audit.py` (kill switch, ≤5 rows, schema normalization) → Task 2. ✓
- §2.3 `upload_file` glue, concurrent, all tiers, both branches → Task 3 (PDF branch also sets `raw_dfs` and passes through `_attach_audit_flags`). ✓
- §2.4 UI banner + badge + one-click + clear-on-toggle → Task 4. ✓
- §3 data flow → Tasks 2–4. ✓
- §4 error handling / offline no-op → Tasks 2, 3, 5. ✓
- §5 testing (verify_mapping, audit_segment, upload glue, frontend manual) → Tasks 1–4 + 5. ✓
- Kill switch default-on, gpt-4o, audit all tiers, clear-flags-on-toggle → Global Constraints + Tasks 2/4. ✓

**Placeholder scan:** No TBD/TODO; every code step shows full code; every command has expected output. ✓ (Task 4 Step 6 intentionally describes a manual UI check because the repo has no frontend test harness — it gives concrete, actionable steps, not a vague "test it".)

**Type consistency:** `audit_segment(mappings, sample_rows, file_type) -> list[dict]` used identically in Tasks 2 and 3; flag dict keys `header`/`reason`/`suggested_field` consistent across Tasks 1–4; `_attach_audit_flags(job, segments_data)` signature consistent between Task 3 definition and its tests. ✓
