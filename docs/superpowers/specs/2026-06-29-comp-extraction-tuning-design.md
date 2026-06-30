# Comp Extraction Tuning — Design Spec

**Date:** 2026-06-29
**Status:** Approved design (pre-plan)
**Goal owner:** Harbor Capital Comp Database ("the comp tool", `Harbor-Capital-Scraper`)
**Companion spec:** `2026-06-29-costar-comp-enrichment-design.md`

---

## 1. Problem & Goal

Today's column-mapping fallback (Tier 4) uses `text-embedding-3-small` cosine similarity + the
Hungarian algorithm (`engine/mapping.py`, accept threshold 0.55). It matches on **header names
only** and ignores the actual cell values, so it misfires on ambiguous or junk headers
(e.g. "Market Asking Rate", "$/yr", merged-cell titles) and forces analysts to correct mappings.

**Goal:** Raise first-pass mapping accuracy (cost is acceptable) by replacing the Tier-4 fallback
with an **LLM mapper (gpt-4o)** that reads sample data rows + the target schema, primed with
**few-shot examples mined from the corrections corpus**, followed by a **verifier pass** that
catches contradictions before the analyst review screen.

## 2. Decisions (locked during brainstorming)

| Decision | Choice |
|---|---|
| Model / provider | **OpenAI `gpt-4o`** (same provider; no new keys) |
| Architecture | **Keep the tiered pipeline**; replace only the Tier-4 embedding fallback |
| Inputs to mapper | Target schema + raw headers + **sample data rows** (values, not just names) |
| Learning integration | **Few-shot examples mined from `ColumnMappingCorrection`** (the existing flywheel) |
| Quality gate | **Add a verifier pass** (second gpt-4o call) before review |
| Offline behavior | Existing heuristic mapper stays as the no-API-key / API-error fallback |

## 3. Architecture

```
load → segment → classify (lease/sale)
   → Tier 1 exact (raw_hash)        ┐
   → Tier 2 fuzzy (Jaccard ≥0.80)   │  cheap, $0, unchanged — short-circuit on hit
   → Tier 3 broker (Jaccard ≥0.60)  ┘
   → Tier 4 (MISS): override-prefilter → LLM mapper (few-shot) → verifier   ◀── NEW
   → clean / rate logic
   → review UI (auto-accept high-confidence + flag low/contradictory)
   → save → learning writeback (feeds future few-shot)
```

LLM only runs when Tiers 1–3 miss (a genuinely novel header layout). Known templates stay free.

## 4. Components

### 4.1 LLM mapper — `engine/llm_mapping.py` (NEW)
- `llm_map_columns(headers, sample_rows, schema, file_type, examples) -> LLMMapping`
  where `LLMMapping = {mappings: {raw_header: target_field}, confidence: {field: float}, unmapped: [headers], reasoning: str}`.
- Model `gpt-4o`, `temperature=0`, `response_format=json_object`.
- Prompt includes: the LEASE/SALE schema (field name + description + type), the raw headers,
  **K sample rows** (default 5) so the model uses values (`$8.15` + nearby `NNN` → `rate_psf`),
  and the few-shot block (§4.2).
- Replaces `generate_standardized_df_with_hints` as the primary Tier-4 path.

### 4.2 Few-shot miner — `engine/mapping_examples.py` (NEW)
- `build_examples(store, file_type, k=20) -> list[Example]`: read `ColumnMappingCorrection`
  (`learning/store.py:get_corrections_for_context`) ranked by `hit_count`; format compact pairs
  (`"Base Rent $/SF" → rate_psf`). Cap token budget. This pipes analyst corrections straight into
  the LLM so the firm's conventions are learned on first pass.
- `BASE_OVERRIDES` stays as a **deterministic pre-filter**: headers it resolves exactly are removed
  before the LLM call (cheaper, fewer tokens); only the remainder is sent.

### 4.3 Verifier — `engine/verify_mapping.py` (NEW)
- `verify_mapping(mapping, sample_rows, schema) -> VerifyResult` with adjusted confidence + flags.
- Second `gpt-4o` call asked to catch contradictions: value/type mismatch (mapped to `sale_price`
  but values look like SF), date↔numeric confusion, monthly/annual rate-unit mismatch, duplicate
  target assignment.
- **Auto-accept policy:** field where mapper is high-confidence AND verifier agrees → pre-accepted
  in the review UI (analyst skims, doesn't re-map). Flagged fields show the verifier's reason.

### 4.4 Pipeline integration — `engine/pipeline.py:run_mapping_stage`
- Tier-4 branch becomes: override-prefilter → `llm_map_columns` → `verify_mapping`.
- Confidence from the LLM/verifier replaces the 0.55 cosine threshold for accept/flag decisions.
- **Cache by fingerprint `raw_hash`** (mirrors Tier-1) so re-uploading the same file shape doesn't
  re-pay for the LLM.

### 4.5 Learning writeback — unchanged
- `learning/corrections.py:persist_with_learning` still records accepted mappings + diffs
  corrections to `learning_local.db`. Corrections now also enrich the few-shot corpus → compounding.

## 5. Confidence & review UX
- Per-field confidence drives the review screen: high+verified → green/auto-accepted;
  medium → shown for confirm; low/flagged → highlighted with reason.
- **No header silently dropped** — `unmapped` headers always surfaced (notes-concat behavior preserved).

## 6. Cost model
- Tiers 1–3 hits → **$0** (no LLM).
- Novel file → ~2 `gpt-4o` calls (map + verify) on the override-unresolved header subset.
- Results cached by `raw_hash`; re-uploads of the same shape → $0.
- Acceptable per the goal ("tune even if it costs more"); still bounded by the tier short-circuit.

## 7. Error handling / fallback
- No `OPENAI_API_KEY` or API failure → fall back to existing `_generate_standardized_df_heuristic`
  (current behavior fully preserved, offline-safe).
- Truncated/invalid JSON → lenient parse + one retry, then heuristic fallback.
- Verifier failure → ship mapper result with un-adjusted confidence (degrade gracefully, never block).

## 8. Testing
- Extend `tests/test_mapping.py`, `tests/test_mapping_hints.py`, `tests/test_accuracy_regression.py`:
  mock the OpenAI client (repo uses `responses`/`pytest-mock`) with recorded mapper + verifier
  responses; feed the messy sample CSVs (`Additional Comps - HC…`, `Aldine Westfield Rankin…`) and
  assert correct mappings (including value-driven cases header-only matching gets wrong).
- Unit-test the few-shot miner against a seeded learning DB.
- Unit-test the verifier catches a deliberately-wrong mapping (e.g. SF→`sale_price`).
- Regression: confirm the heuristic fallback path still passes with **no API key set**.

## 9. Out of scope (this round)
- PDF vision path (`engine/vision_pdf.py`, already `gpt-4o`) — unchanged, but the verifier is
  designed to be reusable on PDF-extracted rows in a later round.
- Switching the column-mapper to Claude (kept OpenAI per decision; revisit if accuracy/cost warrants).
- Bigger embedding model — moot once the LLM mapper replaces the embedding fallback.

## 10. Key risks
- LLM mapper latency on large header sets → mitigated by override-prefilter (fewer headers sent) +
  raw_hash caching + tier short-circuit.
- Over-trusting auto-accept → conservative thresholds; verifier must agree; analyst can still edit.
- Few-shot corpus could encode a bad past correction → cap by `hit_count` (repeated, trusted) and
  keep examples compact.
