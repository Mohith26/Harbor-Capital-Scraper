# Post-mapping audit layer — "does the data match the mapping?"

**Date:** 2026-07-08
**Status:** Approved (design)
**Component:** Comp tool upload pipeline (`Harbor-Capital-Scraper`)

## Problem

On the Upload & Process page, each detected segment is column-mapped
automatically by `run_mapping_stage` (exact / fuzzy / broker template lookups,
the Tier-4 gpt-4o LLM mapper, or the offline embedding/heuristic fallback). The
mapping can be wrong — a column of dates mapped to `tenant_name`, square footage
mapped to a price field, a monthly rate mapped where annual is expected, etc.

There is an existing verifier (`engine/verify_mapping.py`) that audits a proposed
mapping against sample values, but it has two limitations that make it invisible
to the analyst:

1. It runs **only inside the Tier-4 LLM path** (`engine/pipeline.py::_llm_fallback`).
   Segments matched by exact / fuzzy / broker lookups or the embedding fallback
   get **no** data-vs-mapping consistency check at all.
2. Its output is used **only to nudge internal confidence** for the auto-accept
   threshold — the flags are **never surfaced in the preview**, so the analyst
   never sees them before editing.

## Goal

Add a dedicated layer that runs **after mapping is complete and before the user
can edit the preview**, on **every active segment regardless of mapping tier**.
A separate LLM audits the assigned mapping against the actual cell values and
returns contradictions. The preview surfaces these as **advisory, non-blocking**
flags with a **one-click fix** where the LLM can suggest the correct field.

Analyst stays in control (never-fabricate ethos): flags are advisory; the analyst
can accept, fix, or ignore.

## Chosen approach

**Audit** (not blind re-classification): the LLM is shown the current mapping AND
the sample data and hunts for contradictions. This reuses and extends the
existing `verify_mapping` rather than building a new independent classifier.

**Surfacing:** advisory badges + summary banner + one-click "Change to `<field>`"
where a suggestion exists. Non-blocking — the Geocode & Save path is never gated.

### Approved decisions

1. **Scope:** audit **all active segments, every tier** (not only the LLM tier).
2. **Kill switch:** `COMP_MAPPING_AUDIT` env var, **default on** (mirrors
   `COMP_LLM_MAPPER`; set to `0` to disable). Model configurable via env, default
   **gpt-4o** (same model the current verifier uses; `temperature=0`, ≤5 sample
   rows → inexpensive).
3. **Graceful degradation:** no API key / API error / switch off → empty flags;
   the preview renders **identical to today** and the save path is unaffected.
4. **SALE/LEASE toggle interaction:** toggling a segment's `file_type` changes its
   schema and makes flags stale → **clear that segment's `audit_flags`** on
   toggle (v1). A "re-check" button is a possible later enhancement.

## Architecture

Small, isolated units communicating through explicit interfaces.

### 2.1 `engine/verify_mapping.py` — extend (backward-compatible)

Each flag additionally carries `suggested_field` — the schema field the data
actually looks like, or `null`. The prompt is extended to request it; the parser
reads it. The existing Tier-4 caller ignores the new key, so nothing breaks.

Return shape becomes:

```json
{
  "adjusted_confidence": {"<raw_header>": 0.0},
  "flags": [
    {"header": "<raw_header>", "reason": "<why>", "suggested_field": "<field|null>"}
  ]
}
```

### 2.2 `engine/mapping_audit.py` — new (the testable seam)

```python
def audit_segment(
    mappings: dict[str, str],
    sample_rows: list[dict],
    file_type: str,
) -> list[dict]:
    """Return normalized audit flags for one segment.

    [{ "header": str, "reason": str, "suggested_field": str | None }]

    - No-op ([]) when the kill switch is off, there is no API key, or the LLM
      errors (delegates to verify_mapping's existing try/except no-op).
    - Picks the schema for file_type (SALE_SCHEMA / LEASE_SCHEMA).
    - Normalizes suggestions: drop suggested_field when it is not a valid schema
      field for this type, or when it equals the header's current mapping.
    - Only returns flags whose header is in the current mappings.
    """
```

- Reads the kill switch `COMP_MAPPING_AUDIT` (default on).
- The caller (`upload_file`) passes the raw sample records; `audit_segment`
  truncates them to ≤5 before calling the LLM.
- Calls `verify_mapping`, normalizes suggestions against the schema.

### 2.3 `web/routes/upload.py::upload_file` — glue

After all segments are mapped and before `segments_data` entries are finalized:

- For each segment, compute sample rows from its **raw** df (the values the
  analyst sees in the preview) and call `audit_segment`.
- Run segments **concurrently** with a bounded `ThreadPoolExecutor` to keep
  wall-clock low on multi-sheet files.
- Attach `audit_flags` (a list; `[]` when clean or degraded) to each
  `segments_data` entry. Applies to both the Excel/CSV and PDF branches.

No change to `apply_mappings`, `save_to_db`, or the geocode path.

### 2.4 `web/templates/partials/upload_preview.html` — UI

- **State:** `audit_flags` already arrives per segment in `initial.segments`.
- **Summary banner** (per active segment, when it has flags): `⚠ N possible
  mapping issue(s) found`.
- **Column badge:** on each raw header that appears in the segment's
  `audit_flags`, render a `⚠` marker; the reason shows on hover (`title`).
- **One-click fix:** when a flag has a `suggested_field`, render a
  `Change to <field>` button that calls the existing `setMapping(segIdx, header,
  suggested_field)` and then clears that flag.
- **Helpers:** `auditFlagFor(segIdx, header)`, `segmentFlagCount(segIdx)`,
  `applyAuditSuggestion(segIdx, header, field)`, `dismissAuditFlag(segIdx, header)`.
- **Toggle interaction:** in the existing `changeSegmentType`, clear
  `seg.audit_flags` (stale after a schema change).

## Data flow

```
raw df + mappings + file_type
        │  (upload_file, per segment, concurrent)
        ▼
engine.mapping_audit.audit_segment
        │  → verify_mapping (gpt-4o, sees mapping + data)
        ▼
[{header, reason, suggested_field}]   (normalized against schema)
        ▼
segments_data[i]["audit_flags"]  →  preview_state  →  Alpine
        ▼
badge + banner + one-click "Change to <field>" (reuses setMapping)
```

## Error handling

- `verify_mapping` already degrades to a no-op result on any exception.
- `audit_segment` returns `[]` when: kill switch off, no `OPENAI_API_KEY`, or any
  audit exception. A per-segment audit failure must not fail the whole upload —
  the executor collects per-segment results and treats a failed one as `[]`.
- Preview with no flags is byte-identical to today's behavior.

## Testing

- **`verify_mapping`** (`tests/`): mock `_chat_json` to return a flag with
  `suggested_field`; assert it is parsed and passed through; assert flags for
  headers not in `mappings` are dropped.
- **`mapping_audit.audit_segment`**: monkeypatch `verify_mapping` /
  the LLM to return canned output; assert (a) invalid suggested_field dropped,
  (b) suggestion equal to current mapping dropped, (c) kill switch off → `[]`,
  (d) LLM error → `[]`.
- **`upload_file`** (route test, `tests/test_upload_route.py` style): monkeypatch
  the audit to return canned flags; assert `audit_flags` reach `segments_data`;
  and that the kill-switch-off / no-key path yields `[]` without breaking the
  preview render.
- **Frontend Alpine** (badges, banner, one-click, clear-on-toggle): verified
  manually, optional Playwright follow-up.

## Out of scope (YAGNI)

- Re-running the audit after an edit or type toggle (v1 just clears flags).
- Persisting audit outcomes to the learning store.
- Auditing during the `apply_mappings` / save path (upload-time only).
- Blocking the save path on unresolved flags (explicitly rejected — advisory).

## Files touched

- `engine/verify_mapping.py` — extend prompt + parser with `suggested_field`.
- `engine/mapping_audit.py` — **new** orchestration/normalization helper.
- `web/routes/upload.py` — call the audit in `upload_file`; attach `audit_flags`.
- `web/templates/partials/upload_preview.html` — badges, banner, one-click,
  clear-on-toggle.
- `tests/` — unit tests for the two engine units + a route test.
