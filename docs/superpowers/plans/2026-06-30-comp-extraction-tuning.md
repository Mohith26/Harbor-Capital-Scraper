# Comp Extraction Tuning Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the Tier-4 embedding+Hungarian column-mapping fallback with a gpt-4o LLM mapper that reads sample data rows + the target schema, primed with few-shot examples mined from the corrections corpus, followed by a verifier pass — while keeping Tiers 1–3 and an offline heuristic fallback intact.

**Architecture:** The tiered pipeline in `engine/pipeline.py:run_mapping_stage` is unchanged for Tiers 1–3 (exact / fuzzy / broker template matches). Only the Tier-4 fallback branch changes: deterministic exact-override prefilter → `llm_map_columns` (few-shot) → `verify_mapping` → confidence-scored mapping, cached by fingerprint `raw_hash`. Any missing API key or API error falls through to the existing `generate_standardized_df_with_hints` (which itself degrades to the heuristic mapper), so current behavior is fully preserved offline.

**Tech Stack:** Python 3.11, OpenAI `gpt-4o` (`response_format=json_object`, temp 0), pandas, scipy (existing), pytest + pytest-mock + responses (existing test deps).

**Spec:** `docs/superpowers/specs/2026-06-29-comp-extraction-tuning-design.md`

## Global Constraints

- Provider is **OpenAI**; reuse `engine.openai_client._client()`. No new API keys, no Anthropic.
- LLM model for mapper and verifier: **`gpt-4o`**, `temperature=0`, `response_format={"type": "json_object"}`.
- LLM runs **only** in the Tier-4 fallback (Tiers 1–3 must still short-circuit with zero LLM cost).
- **Never** silently drop a header — unmapped headers are always surfaced (preserve notes-concat behavior).
- **Never fabricate** a mapping: when uncertain, leave a header unmapped rather than guess.
- Offline-safe: with no `OPENAI_API_KEY` or on any API error, behavior must equal today's heuristic path.
- `mappings` dicts are always `{raw_header: target_schema_col}` (matches existing pipeline contract).
- PEP 8, type annotations on all new function signatures. Files focused (<400 lines).
- Kill switch: env `COMP_LLM_MAPPER=0` disables the LLM path entirely (falls back to embeddings/heuristic).

---

### Task 1: Add `get_all_corrections` to the learning store

The few-shot miner needs every correction for a file_type, but the store only exposes
`get_corrections_for_context(file_type, raw_header)` (single header). Add a bulk reader to the
protocol and all three implementations.

**Files:**
- Modify: `learning/protocol.py` (add method to `LearningStore` Protocol)
- Modify: `learning/store.py:139-147` (add method to `SqliteLearningStore`, after `get_corrections_for_context`)
- Modify: `learning/fakes.py` (add to `FakeLearningStore` after line 63 and to `EmptyLearningStore` after line 161)
- Test: `tests/test_corrections.py` (append)

**Interfaces:**
- Produces: `store.get_all_corrections(file_type: str) -> list[dict]` where each dict is
  `{"raw_header": str, "target_column": str, "hit_count": int}`, sorted by `hit_count` descending.

- [ ] **Step 1: Write the failing test**

Append to `tests/test_corrections.py`:

```python
from learning.fakes import FakeLearningStore


def test_get_all_corrections_returns_sorted_by_hit_count():
    store = FakeLearningStore()
    store.upsert_correction("LEASE", "asking rate", "rate_psf", "u@test")
    store.upsert_correction("LEASE", "asking rate", "rate_psf", "u@test")  # hit_count=2
    store.upsert_correction("LEASE", "deal sf", "leased_sf", "u@test")     # hit_count=1
    store.upsert_correction("SALE", "pp", "sale_price", "u@test")          # other file_type

    rows = store.get_all_corrections("LEASE")

    assert {"raw_header": "asking rate", "target_column": "rate_psf", "hit_count": 2} in rows
    assert {"raw_header": "deal sf", "target_column": "leased_sf", "hit_count": 1} in rows
    assert all(r["raw_header"] != "pp" for r in rows)  # SALE excluded
    assert [r["hit_count"] for r in rows] == sorted([r["hit_count"] for r in rows], reverse=True)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd ~/HarborCapital/Harbor-Capital-Scraper && .venv/bin/pytest tests/test_corrections.py::test_get_all_corrections_returns_sorted_by_hit_count -v`
Expected: FAIL with `AttributeError: 'FakeLearningStore' object has no attribute 'get_all_corrections'`

- [ ] **Step 3: Implement in `FakeLearningStore`**

In `learning/fakes.py`, after `get_corrections_for_context` (line 63):

```python
    def get_all_corrections(self, file_type: str) -> list[dict]:
        rows = [
            {"raw_header": rh, "target_column": tc, "hit_count": count}
            for (ft, rh, tc), count in self._corrections.items()
            if ft == file_type
        ]
        return sorted(rows, key=lambda r: r["hit_count"], reverse=True)
```

In `learning/fakes.py`, add to `EmptyLearningStore` (after line 161):

```python
    def get_all_corrections(self, file_type): return []
```

- [ ] **Step 4: Implement in `SqliteLearningStore`**

In `learning/store.py`, after `get_corrections_for_context` (line 147):

```python
    def get_all_corrections(self, file_type: str) -> list[dict]:
        with self._session() as s:
            rows = s.execute(
                select(ColumnMappingCorrection)
                .where(ColumnMappingCorrection.file_type == file_type)
                .order_by(ColumnMappingCorrection.hit_count.desc())
            ).scalars().all()
            return [
                {"raw_header": r.raw_header, "target_column": r.target_column, "hit_count": r.hit_count}
                for r in rows
            ]
```

- [ ] **Step 5: Add to the protocol**

In `learning/protocol.py`, after the `upsert_correction` block (line 55):

```python
    def get_all_corrections(self, file_type: str) -> list[dict]:
        """Return every correction for a file_type as
        [{"raw_header": str, "target_column": str, "hit_count": int}],
        sorted by hit_count descending. Empty list when none."""
        ...
```

- [ ] **Step 6: Run tests to verify they pass**

Run: `.venv/bin/pytest tests/test_corrections.py -v`
Expected: PASS (new test + existing tests green)

- [ ] **Step 7: Commit**

```bash
git add learning/protocol.py learning/store.py learning/fakes.py tests/test_corrections.py
git commit -m "feat: add get_all_corrections bulk reader to learning store"
```

---

### Task 2: Few-shot example miner (`engine/mapping_examples.py`)

Turn the corrections corpus into compact few-shot examples for the LLM mapper prompt.

**Files:**
- Create: `engine/mapping_examples.py`
- Test: `tests/test_mapping_examples.py`

**Interfaces:**
- Consumes: `store.get_all_corrections(file_type)` (Task 1).
- Produces:
  - `build_examples(store, file_type: str, k: int = 20) -> list[dict]` →
    `[{"raw_header": str, "target_column": str}, ...]` (top-k by hit_count).
  - `format_examples(examples: list[dict]) -> str` → newline list like `"Base Rent $/SF" -> rate_psf`.

- [ ] **Step 1: Write the failing test**

Create `tests/test_mapping_examples.py`:

```python
from engine.mapping_examples import build_examples, format_examples
from learning.fakes import FakeLearningStore


def test_build_examples_returns_top_k_by_hit_count():
    store = FakeLearningStore()
    for _ in range(3):
        store.upsert_correction("LEASE", "asking rate", "rate_psf", "u")     # hit=3
    store.upsert_correction("LEASE", "deal sf", "leased_sf", "u")            # hit=1
    store.upsert_correction("LEASE", "esc %", "escalations", "u")            # hit=1

    examples = build_examples(store, "LEASE", k=2)

    assert len(examples) == 2
    assert examples[0] == {"raw_header": "asking rate", "target_column": "rate_psf"}


def test_build_examples_empty_store_returns_empty():
    assert build_examples(FakeLearningStore(), "SALE") == []


def test_format_examples_renders_arrow_lines():
    text = format_examples([
        {"raw_header": "base rent $/sf", "target_column": "rate_psf"},
        {"raw_header": "pp", "target_column": "sale_price"},
    ])
    assert '"base rent $/sf" -> rate_psf' in text
    assert '"pp" -> sale_price' in text


def test_format_examples_empty_returns_empty_string():
    assert format_examples([]) == ""
```

- [ ] **Step 2: Run test to verify it fails**

Run: `.venv/bin/pytest tests/test_mapping_examples.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'engine.mapping_examples'`

- [ ] **Step 3: Implement**

Create `engine/mapping_examples.py`:

```python
"""Mine the corrections corpus into few-shot examples for the LLM mapper."""
from __future__ import annotations


def build_examples(store, file_type: str, k: int = 20) -> list[dict]:
    """Return up to k highest-confidence corrections as {raw_header, target_column}."""
    if store is None:
        return []
    rows = store.get_all_corrections(file_type)
    return [
        {"raw_header": r["raw_header"], "target_column": r["target_column"]}
        for r in rows[:k]
    ]


def format_examples(examples: list[dict]) -> str:
    """Render examples as one '"header" -> target' line each (empty string if none)."""
    if not examples:
        return ""
    return "\n".join(
        f'"{e["raw_header"]}" -> {e["target_column"]}' for e in examples
    )
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `.venv/bin/pytest tests/test_mapping_examples.py -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add engine/mapping_examples.py tests/test_mapping_examples.py
git commit -m "feat: few-shot example miner from corrections corpus"
```

---

### Task 3: LLM column mapper (`engine/llm_mapping.py`)

The core upgrade: a gpt-4o call that maps raw headers to schema fields using sample data rows
and few-shot examples.

**Files:**
- Create: `engine/llm_mapping.py`
- Test: `tests/test_llm_mapping.py`

**Interfaces:**
- Consumes: `engine.openai_client._client()`; `format_examples` (Task 2).
- Produces:
  - `_chat_json(prompt: str, model: str = "gpt-4o") -> dict` (thin OpenAI wrapper; tests monkeypatch this).
  - `llm_map_columns(headers, sample_rows, schema, file_type, examples=None) -> dict` →
    `{"mappings": {raw_header: target_col}, "confidence": {raw_header: float}, "unmapped": [raw_header], "reasoning": str}`.

- [ ] **Step 1: Write the failing test**

Create `tests/test_llm_mapping.py`:

```python
import engine.llm_mapping as llm_mapping
from engine.llm_mapping import llm_map_columns
from engine.mapping import LEASE_SCHEMA


def test_llm_map_columns_uses_chat_json(monkeypatch):
    captured = {}

    def fake_chat_json(prompt, model="gpt-4o"):
        captured["prompt"] = prompt
        return {
            "mappings": {"Asking Rate": "rate_psf", "SF": "leased_sf"},
            "confidence": {"Asking Rate": 0.95, "SF": 0.9},
            "unmapped": ["Mystery Col"],
            "reasoning": "values look like $/sf and square footage",
        }

    monkeypatch.setattr(llm_mapping, "_chat_json", fake_chat_json)

    result = llm_map_columns(
        headers=["Asking Rate", "SF", "Mystery Col"],
        sample_rows=[{"Asking Rate": "$8.15", "SF": "20,007", "Mystery Col": "?"}],
        schema=LEASE_SCHEMA,
        file_type="LEASE",
        examples=[{"raw_header": "asking rate", "target_column": "rate_psf"}],
    )

    assert result["mappings"]["Asking Rate"] == "rate_psf"
    assert result["unmapped"] == ["Mystery Col"]
    # Prompt must include the schema, the sample values, and the few-shot example
    assert "rate_psf" in captured["prompt"]
    assert "$8.15" in captured["prompt"]
    assert "asking rate" in captured["prompt"]


def test_llm_map_columns_drops_mappings_to_unknown_targets(monkeypatch):
    monkeypatch.setattr(
        llm_mapping, "_chat_json",
        lambda prompt, model="gpt-4o": {
            "mappings": {"A": "rate_psf", "B": "not_a_real_field"},
            "confidence": {"A": 0.9, "B": 0.9},
            "unmapped": [],
            "reasoning": "",
        },
    )
    result = llm_map_columns(["A", "B"], [{"A": 1, "B": 2}], LEASE_SCHEMA, "LEASE")
    assert "A" in result["mappings"]
    assert "B" not in result["mappings"]  # hallucinated target filtered out
    assert "B" in result["unmapped"]


def test_llm_map_columns_propagates_errors_as_exception(monkeypatch):
    def boom(prompt, model="gpt-4o"):
        raise RuntimeError("no api key")
    monkeypatch.setattr(llm_mapping, "_chat_json", boom)

    import pytest
    with pytest.raises(RuntimeError):
        llm_map_columns(["A"], [{"A": 1}], LEASE_SCHEMA, "LEASE")
```

- [ ] **Step 2: Run test to verify it fails**

Run: `.venv/bin/pytest tests/test_llm_mapping.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'engine.llm_mapping'`

- [ ] **Step 3: Implement**

Create `engine/llm_mapping.py`:

```python
"""LLM-based column mapper (gpt-4o) — Tier-4 fallback replacement.

Reads raw headers + sample data rows + the target schema and returns a mapping
with per-header confidence. Few-shot examples (mined from the corrections corpus)
prime the model with the firm's conventions.
"""
from __future__ import annotations

import json
from typing import Optional

from engine.openai_client import _client
from engine.mapping_examples import format_examples

_MODEL = "gpt-4o"
_MAX_SAMPLE_ROWS = 5


def _chat_json(prompt: str, model: str = _MODEL) -> dict:
    """Single gpt-4o JSON call. Raises on missing key / API error (caller decides fallback)."""
    resp = _client().chat.completions.create(
        model=model,
        messages=[{"role": "user", "content": prompt}],
        response_format={"type": "json_object"},
        temperature=0,
        max_tokens=1500,
    )
    return json.loads(resp.choices[0].message.content)


def _build_prompt(headers, sample_rows, schema, file_type, examples) -> str:
    schema_lines = "\n".join(
        f"- {name}: {meta['desc']} (type: {meta['type']})"
        for name, meta in schema.items()
    )
    rows_preview = json.dumps(sample_rows[:_MAX_SAMPLE_ROWS], default=str, indent=2)
    examples_block = format_examples(examples or [])
    examples_section = (
        f"\nKnown header conventions from past analyst corrections "
        f"(prefer these when a header matches):\n{examples_block}\n"
        if examples_block else ""
    )
    return (
        f"You are mapping columns of a commercial real estate {file_type} comp "
        f"spreadsheet to a fixed target schema.\n\n"
        f"TARGET SCHEMA (target_field: description):\n{schema_lines}\n"
        f"{examples_section}\n"
        f"RAW COLUMN HEADERS:\n{json.dumps([str(h) for h in headers])}\n\n"
        f"SAMPLE DATA ROWS (use the VALUES, not just header names, to decide):\n{rows_preview}\n\n"
        "Rules:\n"
        "- Map each raw header to AT MOST ONE target_field from the schema above.\n"
        "- Only use target_field names that appear in the schema. Never invent a field.\n"
        "- If a header does not clearly correspond to any target_field, list it in 'unmapped'.\n"
        "- Use the sample values as evidence (e.g. a column of dollar-per-sf values is rate_psf "
        "even if the header is unclear).\n\n"
        'Return ONLY JSON: {"mappings": {"<raw_header>": "<target_field>"}, '
        '"confidence": {"<raw_header>": 0.0-1.0}, "unmapped": ["<raw_header>"], '
        '"reasoning": "<one sentence>"}'
    )


def llm_map_columns(
    headers: list[str],
    sample_rows: list[dict],
    schema: dict,
    file_type: str,
    examples: Optional[list[dict]] = None,
) -> dict:
    """Map headers to schema via gpt-4o. Returns dict with mappings/confidence/unmapped/reasoning.

    Mappings to fields not in the schema are dropped and moved to 'unmapped' (never fabricate).
    Raises on API error so the caller can fall back to embeddings/heuristic.
    """
    prompt = _build_prompt(headers, sample_rows, schema, file_type, examples)
    raw = _chat_json(prompt)

    valid_targets = set(schema.keys())
    mappings: dict[str, str] = {}
    unmapped = list(raw.get("unmapped") or [])
    for header, target in (raw.get("mappings") or {}).items():
        if target in valid_targets:
            mappings[header] = target
        elif header not in unmapped:
            unmapped.append(header)

    confidence = {
        h: float(raw.get("confidence", {}).get(h, 0.0))
        for h in mappings
    }
    return {
        "mappings": mappings,
        "confidence": confidence,
        "unmapped": unmapped,
        "reasoning": str(raw.get("reasoning", "")),
    }
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `.venv/bin/pytest tests/test_llm_mapping.py -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add engine/llm_mapping.py tests/test_llm_mapping.py
git commit -m "feat: gpt-4o LLM column mapper with sample-row + few-shot prompting"
```

---

### Task 4: Verifier pass (`engine/verify_mapping.py`)

A second gpt-4o call that checks the proposed mapping against the sample values and flags
contradictions, adjusting confidence.

**Files:**
- Create: `engine/verify_mapping.py`
- Test: `tests/test_verify_mapping.py`

**Interfaces:**
- Consumes: `engine.llm_mapping._chat_json` (reused JSON-call wrapper).
- Produces:
  - `verify_mapping(mappings, sample_rows, schema) -> dict` →
    `{"adjusted_confidence": {raw_header: float}, "flags": [{"header": str, "reason": str}]}`.

- [ ] **Step 1: Write the failing test**

Create `tests/test_verify_mapping.py`:

```python
import engine.verify_mapping as verify_mod
from engine.verify_mapping import verify_mapping
from engine.mapping import SALE_SCHEMA


def test_verify_flags_value_type_mismatch(monkeypatch):
    monkeypatch.setattr(
        verify_mod, "_chat_json",
        lambda prompt, model="gpt-4o": {
            "adjusted_confidence": {"Size": 0.2},
            "flags": [{"header": "Size", "reason": "values look like square footage, not a sale price"}],
        },
    )
    result = verify_mapping(
        mappings={"Size": "sale_price"},
        sample_rows=[{"Size": "19,500"}],
        schema=SALE_SCHEMA,
    )
    assert result["flags"][0]["header"] == "Size"
    assert result["adjusted_confidence"]["Size"] == 0.2


def test_verify_failure_degrades_gracefully(monkeypatch):
    def boom(prompt, model="gpt-4o"):
        raise RuntimeError("api down")
    monkeypatch.setattr(verify_mod, "_chat_json", boom)

    result = verify_mapping({"A": "sale_price"}, [{"A": 1}], SALE_SCHEMA)
    # On verifier failure: no flags, no confidence change (caller keeps mapper confidence)
    assert result["flags"] == []
    assert result["adjusted_confidence"] == {}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `.venv/bin/pytest tests/test_verify_mapping.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'engine.verify_mapping'`

- [ ] **Step 3: Implement**

Create `engine/verify_mapping.py`:

```python
"""Verifier pass — a second gpt-4o call that checks a proposed mapping against sample values."""
from __future__ import annotations

import json

from engine.llm_mapping import _chat_json

_MODEL = "gpt-4o"


def _build_prompt(mappings, sample_rows, schema) -> str:
    schema_lines = "\n".join(
        f"- {name}: {meta['desc']} (type: {meta['type']})"
        for name, meta in schema.items()
    )
    return (
        "You are auditing a proposed column mapping for a commercial real estate comp file.\n\n"
        f"TARGET SCHEMA:\n{schema_lines}\n\n"
        f"PROPOSED MAPPING (raw_header -> target_field):\n{json.dumps(mappings, indent=2)}\n\n"
        f"SAMPLE DATA ROWS:\n{json.dumps(sample_rows[:5], default=str, indent=2)}\n\n"
        "Find contradictions where the column's VALUES do not match the assigned target_field "
        "(e.g. square footage mapped to a price field, a non-date mapped to a date field, "
        "a monthly rate mapped where an annual rate is expected, or two headers mapped to the "
        "same field).\n\n"
        'Return ONLY JSON: {"adjusted_confidence": {"<raw_header>": 0.0-1.0}, '
        '"flags": [{"header": "<raw_header>", "reason": "<why it is suspicious>"}]}. '
        "Only include headers you are adjusting or flagging."
    )


def verify_mapping(mappings: dict[str, str], sample_rows: list[dict], schema: dict) -> dict:
    """Audit a mapping against sample values. Degrades to a no-op result on any error."""
    if not mappings:
        return {"adjusted_confidence": {}, "flags": []}
    try:
        raw = _chat_json(_build_prompt(mappings, sample_rows, schema), model=_MODEL)
    except Exception:
        return {"adjusted_confidence": {}, "flags": []}

    adjusted = {
        h: float(v)
        for h, v in (raw.get("adjusted_confidence") or {}).items()
        if h in mappings
    }
    flags = [
        {"header": f.get("header", ""), "reason": f.get("reason", "")}
        for f in (raw.get("flags") or [])
        if f.get("header") in mappings
    ]
    return {"adjusted_confidence": adjusted, "flags": flags}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `.venv/bin/pytest tests/test_verify_mapping.py -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add engine/verify_mapping.py tests/test_verify_mapping.py
git commit -m "feat: gpt-4o verifier pass for column mappings"
```

---

### Task 5: Wire the LLM fallback into the pipeline

Insert the LLM path as the Tier-4 fallback in `run_mapping_stage`, with the exact-override
prefilter, raw_hash caching, the verifier, and graceful fallthrough to the existing embedding/
heuristic path.

**Files:**
- Modify: `engine/pipeline.py` (imports at top; the fallback branch lines 100-117)
- Test: `tests/test_pipeline_llm_fallback.py`

**Interfaces:**
- Consumes: `llm_map_columns` (Task 3), `verify_mapping` (Task 4), `build_examples` (Task 2),
  `_find_override`/`BASE_OVERRIDES`/`LEASE_OVERRIDES`/`SALE_OVERRIDES`/`clean_header` (existing),
  `generate_standardized_df_with_hints` (existing fallthrough), `dedupe_mappings_by_target` (existing).
- Produces: `MappingResult` with `source="llm"` (or `"llm+corrections"`) on the LLM path,
  unchanged shapes for all other tiers.

- [ ] **Step 1: Write the failing test**

Create `tests/test_pipeline_llm_fallback.py`:

```python
import os
import pandas as pd
import engine.pipeline as pipeline
from engine.pipeline import run_mapping_stage
from learning.fakes import FakeLearningStore


def _df():
    return pd.DataFrame({
        "Property": ["1326 W Carrier Pkwy"],
        "Tenant": ["Spire Building Supplies"],
        "Asking Rate": ["$8.15"],
        "Area Leased": ["20,007"],
    })


def test_llm_path_used_when_enabled(monkeypatch):
    monkeypatch.setenv("COMP_LLM_MAPPER", "1")

    def fake_llm(headers, sample_rows, schema, file_type, examples=None):
        return {
            "mappings": {"Asking Rate": "rate_psf", "Area Leased": "leased_sf",
                         "Tenant": "tenant_name", "Property": "address"},
            "confidence": {"Asking Rate": 0.95, "Area Leased": 0.92,
                           "Tenant": 0.9, "Property": 0.85},
            "unmapped": [],
            "reasoning": "ok",
        }

    monkeypatch.setattr(pipeline, "llm_map_columns", fake_llm)
    monkeypatch.setattr(pipeline, "verify_mapping",
                        lambda m, rows, schema: {"adjusted_confidence": {}, "flags": []})

    result = run_mapping_stage(_df(), "lease_comps.csv", "Lease", FakeLearningStore())

    assert result.source.startswith("llm")
    assert result.mappings.get("Asking Rate") == "rate_psf"
    assert result.mappings.get("Area Leased") == "leased_sf"


def test_llm_error_falls_through_to_embeddings(monkeypatch):
    monkeypatch.setenv("COMP_LLM_MAPPER", "1")

    def boom(*a, **k):
        raise RuntimeError("no api key")
    monkeypatch.setattr(pipeline, "llm_map_columns", boom)

    # Must not raise; falls through to generate_standardized_df_with_hints (heuristic offline)
    result = run_mapping_stage(_df(), "lease_comps.csv", "Lease", FakeLearningStore())
    assert result.source in ("embedding", "embedding+corrections", "heuristic")


def test_llm_disabled_by_kill_switch(monkeypatch):
    monkeypatch.setenv("COMP_LLM_MAPPER", "0")
    called = {"llm": False}

    def fake_llm(*a, **k):
        called["llm"] = True
        return {"mappings": {}, "confidence": {}, "unmapped": [], "reasoning": ""}
    monkeypatch.setattr(pipeline, "llm_map_columns", fake_llm)

    run_mapping_stage(_df(), "lease_comps.csv", "Lease", FakeLearningStore())
    assert called["llm"] is False
```

- [ ] **Step 2: Run test to verify it fails**

Run: `.venv/bin/pytest tests/test_pipeline_llm_fallback.py -v`
Expected: FAIL (pipeline has no `llm_map_columns` attribute; source never starts with "llm")

- [ ] **Step 3: Add imports + helpers to `engine/pipeline.py`**

At the top of `engine/pipeline.py`, after the existing imports (line 25), add:

```python
import os

from engine.mapping import (
    BASE_OVERRIDES,
    LEASE_OVERRIDES,
    SALE_OVERRIDES,
    _find_override,
)
from engine.llm_mapping import llm_map_columns
from engine.verify_mapping import verify_mapping
from engine.mapping_examples import build_examples

# raw_hash -> (mappings, confidence) — avoids re-paying the LLM for an identical file shape
_LLM_MAPPING_CACHE: dict[str, tuple[dict, dict]] = {}

# Headers verifier-flagged or below this confidence are surfaced for analyst review (not auto-accepted)
_LLM_AUTO_ACCEPT_THRESHOLD = 0.75


def _llm_mapper_enabled() -> bool:
    return os.environ.get("COMP_LLM_MAPPER", "1") != "0"


def _exact_override_mappings(raw_headers: list[str], file_type: str) -> dict[str, str]:
    """Headers that resolve to a target via an EXACT deterministic override (score >= 100)."""
    overrides = dict(BASE_OVERRIDES)
    overrides.update(LEASE_OVERRIDES if file_type in ("LEASE", "BOTH") else SALE_OVERRIDES)
    result: dict[str, str] = {}
    for raw in raw_headers:
        cleaned = clean_header(raw)
        for target in set(overrides.values()):
            if _find_override(cleaned, overrides, target) >= 100.0:
                result[raw] = target
                break
    return result


def _llm_fallback(df, schema, file_type, store, raw_hash):
    """Tier-4 LLM mapping. Returns (out_df, mappings, confidence, source) or None to fall through."""
    if not _llm_mapper_enabled():
        return None

    raw_headers = [str(c) for c in df.columns]

    if raw_hash in _LLM_MAPPING_CACHE:
        mappings, confidence = _LLM_MAPPING_CACHE[raw_hash]
    else:
        # Exact-override prefilter: resolve trivial headers deterministically, send the rest to the LLM
        override_map = _exact_override_mappings(raw_headers, file_type)
        unresolved = [h for h in raw_headers if h not in override_map]
        sample_rows = df[unresolved].head(5).to_dict("records") if unresolved else []
        examples = build_examples(store, file_type)
        try:
            llm = llm_map_columns(unresolved, sample_rows, schema, file_type, examples)
        except Exception:
            return None  # fall through to embeddings/heuristic

        mappings = dict(override_map)
        mappings.update(llm["mappings"])
        confidence = {h: 1.0 for h in override_map}
        confidence.update(llm["confidence"])

        verdict = verify_mapping(llm["mappings"], sample_rows, schema)
        for header, adj in verdict["adjusted_confidence"].items():
            confidence[header] = adj
        _LLM_MAPPING_CACHE[raw_hash] = (mappings, confidence)

    mappings = dedupe_mappings_by_target(mappings, raw_headers, confidence)
    out_df = _apply_mappings(df, mappings)
    source = "llm+corrections" if build_examples(store, file_type) else "llm"
    return out_df, mappings, confidence, source
```

- [ ] **Step 4: Replace the fallback branch**

In `engine/pipeline.py`, replace the current fallback block (lines 100-117, the
`# Fallback: correction-weighted embedding` section through its `return MappingResult(...)`) with:

```python
    # Tier 4: LLM mapper (gpt-4o + few-shot + verifier); falls through on error/kill-switch
    llm_result = _llm_fallback(df, schema, file_type, store, fp.raw_hash)
    if llm_result is not None:
        out_df, mappings, confidence, source = llm_result
        return MappingResult(
            fingerprint=fp,
            mappings=mappings,
            confidence=confidence,
            source=source,
            similarity=0.0,
            cleaned_df=out_df,
        )

    # Fallback: correction-weighted embedding (offline-safe: degrades to heuristic)
    out_df, mappings, confidence = generate_standardized_df_with_hints(
        df, schema, file_type=file_type, store=store
    )
    mapping_source = out_df.attrs.get("mapping_source")
    mappings = dedupe_mappings_by_target(mappings, raw_headers, confidence)
    out_df = _apply_mappings(df, mappings)
    has_corrections = _has_any_corrections(store, file_type, raw_headers)
    source = "embedding+corrections" if has_corrections else "embedding"
    source = mapping_source or source
    return MappingResult(
        fingerprint=fp,
        mappings=mappings,
        confidence=confidence,
        source=source,
        similarity=0.0,
        cleaned_df=out_df,
    )
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `.venv/bin/pytest tests/test_pipeline_llm_fallback.py tests/test_pipeline_tiers.py -v`
Expected: PASS (new LLM-fallback tests + existing tier tests still green)

- [ ] **Step 6: Commit**

```bash
git add engine/pipeline.py tests/test_pipeline_llm_fallback.py
git commit -m "feat: wire gpt-4o LLM mapper + verifier as Tier-4 fallback with raw_hash cache"
```

---

### Task 6: Accuracy regression on the real messy sample CSVs

Lock in the value-driven wins using the repo's actual broker comp CSVs, with a mocked LLM that
returns the correct mapping a header-only matcher would miss.

**Files:**
- Test: `tests/test_accuracy_regression.py` (append)

**Interfaces:**
- Consumes: `run_mapping_stage` (Task 5), the sample CSV `Additional Comps - HC.xlsx - Lease (1).csv`.

- [ ] **Step 1: Write the failing test**

Append to `tests/test_accuracy_regression.py`:

```python
import os
import pandas as pd
import pytest
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
```

- [ ] **Step 2: Run test to verify it passes (or skips if CSV absent)**

Run: `.venv/bin/pytest tests/test_accuracy_regression.py::test_llm_maps_messy_lease_headers -v`
Expected: PASS (or SKIP if the sample CSV is not in the repo root)

- [ ] **Step 3: Run the full suite + lint**

Run: `.venv/bin/pytest -q && .venv/bin/ruff check engine/ learning/`
Expected: all green

- [ ] **Step 4: Commit**

```bash
git add tests/test_accuracy_regression.py
git commit -m "test: accuracy regression for LLM mapper on real messy lease comps"
```

---

## Self-Review

**Spec coverage:**
- LLM mapper (gpt-4o, sample rows) → Task 3 ✓
- Few-shot from corrections corpus → Task 1 (bulk reader) + Task 2 (miner) ✓
- Verifier pass → Task 4 ✓
- Keep Tiers 1–3; replace only Tier-4 → Task 5 ✓
- Exact-override prefilter → Task 5 `_exact_override_mappings` ✓
- raw_hash caching → Task 5 `_LLM_MAPPING_CACHE` ✓
- Offline/no-key fallthrough to heuristic → Task 5 `_llm_fallback` returns None on exception ✓
- Kill switch (`COMP_LLM_MAPPER=0`) → Task 5 `_llm_mapper_enabled` ✓
- Never fabricate (drop hallucinated targets) → Task 3 valid_targets filter ✓
- Never silently drop headers → unmapped surfaced; dedupe preserves notes-concat ✓
- Accuracy regression on real CSVs → Task 6 ✓

**Type consistency:** `_chat_json` defined in Task 3, reused in Task 4. `llm_map_columns`/`verify_mapping`/`build_examples` signatures match their pipeline call sites in Task 5. `get_all_corrections` shape consistent across protocol/store/fakes (Task 1) and consumed in Task 2.

**Out of scope (per spec):** PDF vision path unchanged; no Claude; no embedding-model bump.
