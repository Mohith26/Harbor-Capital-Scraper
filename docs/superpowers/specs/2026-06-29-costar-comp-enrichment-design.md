# CoStar Comp Enrichment — Design Spec

**Date:** 2026-06-29
**Status:** Approved design (pre-plan)
**Goal owner:** Harbor Capital Comp Database ("the comp tool", `Harbor-Capital-Scraper`)
**Companion spec:** `2026-06-29-comp-extraction-tuning-design.md`

---

## 1. Problem & Goal

When an analyst uploads a comp spreadsheet, the comp tool stores the deal's facts (address,
price, rate, size, …) but has **no link to CoStar's record** of that property. Analysts must
manually look each comp up on CoStar to pull authoritative building specs and market context.

**Goal:** Given a comp's address, automatically resolve it to its CoStar property and pull a
**rich set of physical building specs** (clear height, site coverage, dock doors, column
spacing, sprinkler, power, construction, office SF, land acres, rail, parking, year built,
class, RBA), plus submarket rent/vacancy/cap, and attach them to the comp record — **without
overwriting the analyst's own entered values**.

## 2. Decisions (locked during brainstorming)

| Decision | Choice | Rationale |
|---|---|---|
| Execution model | **On-demand local CLI** (`costar enrich`) | CoStar scraping must run where the analyst's authenticated Chrome is (compliance — never bypass login/Akamai/rate limits). The Railway app cannot reach CoStar. |
| Data captured | **As many physical property specs as CoStar exposes** + submarket benchmarks | Industrial comps live or die on building specs; demographics are secondary/optional. |
| Conflict policy | **Never overwrite analyst values**; store CoStar values alongside for an "analyst vs CoStar" comparison | Analyst data is ground truth; CoStar is corroboration, not replacement. |
| Code location | Enrichment **worker lives in `CoStar-Market-Extraction`**; comp tool gains DB columns + light UI | That repo owns CDP/Chrome, the parser, the LLM extractor, and the new `scrape` CLI. Avoids duplicating browser logic into the cloud app. |

## 3. Architecture (cross-repo)

```
┌─────────────────────────────┐         ┌──────────────────────────────────────┐
│ Harbor-Capital-Scraper      │         │ CoStar-Market-Extraction (LOCAL)       │
│ (comp tool, Railway/cloud)  │         │  — analyst's machine, CoStar Chrome    │
│                             │         │                                        │
│ upload → save               │         │  `costar enrich` CLI                   │
│   sets costar_status=pending│         │    1. comp_sink: read pending comps    │
│                             │  Supabase    2. lookup: address → CoStar PID    │
│ DB: SaleComp / LeaseComp    │◀───────▶│    3. scraper: pull industrial specs   │
│   + costar_* columns        │ (shared)│    4. comp_sink: write specs back      │
│ UI: badge, deep-link,       │         │  (CDP → product.costar.com, rate-      │
│     candidate-picker        │         │   limited, resumable, never fabricate) │
└─────────────────────────────┘         └──────────────────────────────────────┘
```

Both processes talk to the **same Supabase Postgres** (`SUPABASE_DB_URL`). The cloud app never
touches CoStar; the local worker never serves web traffic.

## 4. Components

### 4.1 Address → CoStar PID resolver — `costar_market/lookup.py` (NEW; the missing primitive)
- **Phase 0 discovery spike (manual, ~30 min, prerequisite):** attach to CoStar Chrome via CDP,
  register `page.on("response")`, type a known address into CoStar's property search, and capture
  the autocomplete/lookup **XHR** — its URL, method, request payload, and response schema. Record
  findings in this repo (e.g. `docs/costar-search-endpoint.md`).
- `resolve_address(addr: str) -> list[Candidate]` where `Candidate = {pid, label, address, type, score}`.
  - Primary path: call the discovered search XHR via Playwright's authenticated context.
  - Fallback path: drive the search box in the DOM and read rendered results (slower, used only if
    no clean XHR exists).
- **Match-confidence policy:**
  - exactly one strong match → auto-accept (`costar_status='enriched'`).
  - multiple / weak matches → `costar_status='ambiguous'`, persist candidate list for the analyst.
  - zero matches → `costar_status='not_found'`.
  - **Never guess.**

### 4.2 Industrial spec scraper — extend `costar_market/cdp_scraper.py` + `parser.py`/`text_extractor.py`
- Reuse the existing `CAPTURE_PLAN` navigation (`/detail/lookup/{pid}/...` and the
  `feat/scrape-cli-and-real-costar-extraction` branch's `/detail/all-properties/{pid}` + real-layout
  parsing).
- **Extend the extraction schema** to the full industrial field set (§6). Regex pre-pass first,
  then the existing LLM extractor (`extraction/llm_client.py`) fills gaps. Missing → `null`
  (never fabricated).

### 4.3 Comp DB adapter — `costar_market/comp_sink.py` (NEW)
- `read_pending(limit) -> list[CompRow]`: comps where `costar_status='pending'`, with address fields.
- `write_enrichment(comp_id, comp_type, result)`: writes `costar_property_id`, `costar_url`,
  `costar_specs` (JSON), `costar_status`, `costar_enriched_at`, and (for ambiguous) `costar_candidates`.
- **Never writes to analyst columns.** CoStar values live only in `costar_*`.

### 4.4 Enrichment CLI — `costar enrich` (extend `costar_market/cli.py`)
- `costar enrich [--limit N] [--type sale|lease|both] [--dry-run]`.
- Orchestrates read → resolve → scrape → writeback for each pending comp.
- Rate-limited delays between properties; **idempotent/resumable** (skips already-enriched);
  structured per-comp logging (resolved/ambiguous/not_found/error).

### 4.5 Comp tool changes — `Harbor-Capital-Scraper`
- **Migration** adding to `SaleComp` and `LeaseComp` (`database.py`): `costar_property_id` (str,
  nullable), `costar_url` (str, nullable), `costar_specs` (JSON, nullable),
  `costar_status` (str, default `'pending'`), `costar_candidates` (JSON, nullable),
  `costar_enriched_at` (datetime, nullable).
- On save (`web/routes/upload.py` → `persist_with_learning`): new comps default `costar_status='pending'`.
- **UI** (Database view + Detail): CoStar status badge, deep-link to `costar_url`, an
  **analyst-vs-CoStar** comparison panel for overlapping fields, and a **candidate-picker** when
  status is `ambiguous`. Picking a candidate writes the chosen `costar_property_id` and flips
  `costar_status` back to `'pending'` (so the next local `costar enrich` run scrapes that specific
  property) — the cloud UI never scrapes CoStar itself.

## 5. Data flow
`upload → save (cloud)` → `costar_status='pending'` → analyst runs `costar enrich` locally with
CoStar logged in → resolver → (enriched | ambiguous | not_found) → scraper pulls specs → adapter
writes `costar_*` back to Supabase → comp tool surfaces link, specs, and comparison.

## 6. Industrial spec field set (`costar_specs` JSON)
Identity/size: `rba_sf`, `year_built`, `year_renovated`, `building_class`, `property_subtype`,
`construction_type` (tilt-wall / metal / masonry / precast), `num_buildings`, `stories`.
Industrial physical: `clear_height_ft`, `dock_high_doors`, `drive_in_doors`, `column_spacing`,
`truck_court_depth_ft`, `office_sf`, `office_pct`, `sprinkler_type` (e.g. ESFR),
`power_amps`, `power_volts`, `rail_served` (bool), `crane` (bool/details).
Site: `land_acres`, `site_coverage_pct` (building coverage / FAR), `parking_spaces`, `parking_ratio`.
Market context: `submkt_vacancy_pct`, `submkt_avg_rent_psf`, `submkt_cap_rate`,
`submkt_net_absorption_sf`, `submkt_under_construction_sf`.
Provenance: `costar_property_id`, `captured_at`, per-field source page.
> Final list reconciled against actual CoStar layout during the Phase-0 spike; unknown fields stay null.

## 7. Error handling & compliance
- Auth/Akamai block → reuse `NotAuthenticatedError` ("log into CoStar in Chrome, then re-run").
- Ambiguous → store candidates, surface to analyst; do not auto-pick.
- Not found → flag, continue.
- Rate limits → per-property delay; CLI processes serially by default.
- Partial scrape → persist whatever resolved with provenance.
- Re-runs safe (idempotent). Never fabricate. Never overwrite analyst data.

## 8. Testing
- HTML fixtures of CoStar detail pages → unit-test the extended spec parser.
- Mock resolver XHR responses → test candidate parsing + confidence policy (single/multi/none).
- `comp_sink` adapter tested against SQLite (`costar_*` writeback, never-overwrite invariant).
- CLI tested with a mocked browser + mocked comp DB (resumability, dry-run, status transitions).
- Phase-0 endpoint discovery is manual/exploratory (not unit-tested).

## 9. Out of scope (this round)
- Demographics capture (population/income/walk score) — deferred; field set focuses on building specs.
- Cloud-side queueing / "Enrich" button UX — the on-demand CLI is the MVP trigger.
- Bulk re-enrichment scheduling.

## 10. Key risks
- **CoStar address-search endpoint shape is unknown** until the Phase-0 spike — it gates the
  resolver. If no clean XHR exists, fall back to DOM-driven search (slower but viable).
- CoStar layout drift can break the spec parser → mitigated by regex+LLM hybrid and fixtures.
- Compliance: keep serial + delayed; this mirrors the existing compliant CDP approach.
