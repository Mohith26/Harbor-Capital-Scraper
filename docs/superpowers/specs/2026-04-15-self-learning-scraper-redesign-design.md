# Self-Learning Comp Scraper Redesign

**Date:** 2026-04-15
**Branch:** `claude/thirsty-wilbur`
**Status:** Approved — ready for implementation plan

## Problem

The current `comp_engine.py` pipeline is a single-pass, stateless column mapper. It uses embeddings + the Hungarian algorithm to map input columns to a target schema on every upload, plus hand-curated override dictionaries. It has zero memory across uploads — every file re-does the same guesswork from scratch, and every manual correction a user makes is thrown away on save.

The two biggest accuracy pain points are:

1. **Column mapping** — fields misidentified (price vs. price/sf, acres vs. sf, rate monthly vs. annual).
2. **Address geocoding** — partial addresses, wrong city matches, building-name noise that Google can't parse.

Secondary pain points include rate-unit misclassification, weird mini-table layouts, and the complete absence of PDF support (two of the 16 sample files are PDFs).

## Goals

- **Higher accuracy.** More token usage is acceptable; the goal is right answers, not cheap answers.
- **Self-learning.** Every upload, every manual correction, every confirmed mapping teaches the engine for next time. Accuracy should climb as analyst usage grows.
- **Reproducible baseline.** Anyone cloning the repo should start with the same accuracy floor — learning state is seeded from committed JSON files.
- **Local-first rollout.** New pipeline runs locally for user acceptance testing before any prod deploy.

## Non-Goals

- Ranking/search quality of Comp Finder is out of scope.
- Rebuilding the Streamlit UI layout is out of scope (the user is making parallel UI changes on another branch; this design integrates via data contracts, not widget structure).
- Multi-state support — all properties are guaranteed Texas.

## Signal Sources for Learning

Three confirmed signals feed the learning loop:

| Signal | Strength | Captured When |
|---|---|---|
| Manual column mapping overrides | Strong | User changes mapping in review UI before save |
| Labeled reference sample files | Strong | Bootstrapped once from `sample comp files/` via `tools/seed_from_samples.py` |
| Implicit acceptance (clean save) | Medium | User clicks Save with no edits → fingerprint `sample_count += 1`, `confidence += 0.05` |

Explicitly out of scope for v1:
- Post-save record edits in DB View (Phase 2 / future)
- Record deletions (weak and noisy)

## Architecture — Approach 2 (Staged Pipeline with Learning Injection Points)

### Module Layout

```
comp_engine.py          (thin facade, preserves public API for app.py)
engine/
  __init__.py
  loaders.py            # xlsx/csv/pdf loaders → LoadedSegment[]
  vision_pdf.py         # GPT-4o vision PDF extractor
  fingerprint.py        # header-set hashing, broker extraction, tier lookup
  mapping.py            # column mapping stage (embedding + Hungarian, with correction priors)
  cleaning.py           # _to_float, rate logic, numeric coercion
  geocoding.py          # Google wrapper + LLM cleanup fallback + alias/override lookup
  validation.py         # row-level validation, warnings
  pipeline.py           # orchestrator: load → fingerprint → map → clean → geocode → validate
learning/
  __init__.py
  store.py              # LearningStore class: dual backend (JSON seed + Supabase live)
  schemas.py            # SQLAlchemy models for learning tables
  fingerprints.py       # hashing + tier matching logic
  corrections.py        # persist manual overrides, writeback hooks
  geocode_cache.py      # alias + override lookups
learning_data/          # committed seed data (reproducible baseline)
  seed_fingerprints.json
  seed_corrections.json
  seed_geocode_aliases.json
  seed_brokers.json
  labeled_samples/
    *.json              # one per sample file segment, hand + auto labeled
tools/
  seed_from_samples.py  # one-shot: run pipeline on 16 sample files → seed JSON
  rebuild_learning_from_seed.py  # wipe + reload learning tables from learning_data/
  sync_learning.py      # pull Supabase live learnings → refresh seed JSON
tests/
  fixtures/             # mini samples for each stage
  test_loaders.py
  test_vision_pdf.py
  test_fingerprint.py
  test_mapping.py
  test_cleaning.py
  test_geocoding.py
  test_validation.py
  test_pipeline.py
  test_learning_store.py
```

`comp_engine.py` keeps its current public functions (`process_all_sheets`, `apply_manual_mapping`, `process_file_to_clean_output`) as thin shims calling into `engine.pipeline` — so `app.py` diff is minimal.

### Stage Data Contracts

Every pipeline stage is a pure function with explicit typed input and output. This makes each stage mockable in tests and learning injection points unambiguous.

```python
# engine/types.py

@dataclass
class LoadedSegment:
    df: pd.DataFrame
    source_path: str
    sheet_name: str | None
    segment_title: str | None
    segment_index: int
    loader: str                      # "xlsx" | "csv" | "pdf_vision"
    raw_headers: list[str]

@dataclass
class Fingerprint:
    hash: str                        # sha256(file_type + "|" + "|".join(sorted(clean_headers)))
    broker: str | None               # "JLL" | None
    file_type: str                   # LEASE | SALE | BOTH | UNKNOWN
    clean_headers: list[str]

@dataclass
class FingerprintMatch:
    tier: int                        # 1=exact, 2=fuzzy, 3=broker, 4=none
    blueprint: dict[str, str] | None # {target_field: input_header}
    confidence: float                # 0.0-1.0
    source_id: int | None

@dataclass
class MappingResult:
    mappings: dict[str, str]         # target_field → input_header
    confidence: dict[str, float]     # per-field 0.0-1.0
    source: str                      # "fingerprint_exact" | "fingerprint_fuzzy" | "fingerprint_broker" | "corrections_weighted" | "embedding_fallback" | "vision_identity"
    file_type: str

@dataclass
class CleanedRows:
    df: pd.DataFrame
    rate_basis: str | None
    warnings: list[str]

@dataclass
class GeocodedRows:
    df: pd.DataFrame
    geocode_sources: list[str]       # per-row: "override"|"cache"|"google"|"llm_cleanup"|"miss"
    warnings: list[str]

@dataclass
class SegmentResult:
    segment: LoadedSegment
    fingerprint: Fingerprint
    fingerprint_match: FingerprintMatch
    mapping: MappingResult
    cleaned: CleanedRows
    geocoded: GeocodedRows
    broker_llm_guess: str | None
    loader: str

@dataclass
class PipelineResult:
    segments: list[SegmentResult]
    combined_df: pd.DataFrame
    confidence_by_segment: dict
    mappings_by_segment: dict
    warnings: list[str]
```

Stage signatures:

```python
def load(path: str, filename: str) -> list[LoadedSegment]
def fingerprint(seg: LoadedSegment) -> Fingerprint
def match_fingerprint(fp: Fingerprint, store: LearningStore) -> FingerprintMatch
def map_columns(seg: LoadedSegment, fp: Fingerprint, fp_match: FingerprintMatch, store: LearningStore) -> MappingResult
def clean(seg: LoadedSegment, mapping: MappingResult) -> CleanedRows
def geocode(cleaned: CleanedRows, store: LearningStore, api_key: str) -> GeocodedRows
def validate(geocoded: GeocodedRows, mapping: MappingResult) -> GeocodedRows
```

Orchestrator:

```python
def run_pipeline(path: str, filename: str, store: LearningStore, google_api_key: str) -> PipelineResult
```

## Learning Store Schema

Dual backend. JSON seed files in `learning_data/` are the committed baseline (reviewable, reproducible). Supabase tables are live (growing from prod usage). On lookup, store checks live tables first and falls back to seed. On write, always writes live. `tools/rebuild_learning_from_seed.py` can wipe live tables and reinsert from seed at any time.

### `template_fingerprints`

```
id                 PK
fingerprint_hash   TEXT UNIQUE    -- sha256 of file_type + sorted clean headers
broker             TEXT NULL      -- "JLL", "CBRE", etc., extracted on first sight
file_type          TEXT           -- LEASE | SALE | BOTH
header_set         JSONB          -- ["address","size sf","rate",...]
mapping_blueprint  JSONB          -- {"address":"Property","rate_psf":"Rate/SF/Yr",...}
rate_unit_hint     TEXT NULL      -- "monthly" | "annual" | null
sample_count       INT            -- times confirmed by humans
last_seen_at       TIMESTAMPTZ
confidence         FLOAT          -- 0.0-1.0, decays if not reconfirmed
created_by         TEXT           -- user email or "seed"
```

### `column_mapping_corrections`

```
id              PK
clean_header    TEXT          -- normalized input header
target_field    TEXT          -- schema field it maps to
file_type       TEXT          -- LEASE | SALE
broker          TEXT NULL
vote_count      INT           -- times humans confirmed this mapping
last_confirmed  TIMESTAMPTZ
UNIQUE(clean_header, target_field, file_type, broker)
```

Used as Tier 4 correction-weighted bonus when no fingerprint match.

### `geocode_aliases`

```
raw_text_hash    PK            -- sha256 of normalized raw_text
raw_text         TEXT
formatted_addr   TEXT
latitude         FLOAT
longitude        FLOAT
city             TEXT
zip_code         TEXT
source           TEXT          -- "google" | "llm_cleanup_then_google"
created_at       TIMESTAMPTZ
hit_count        INT
```

### `geocode_overrides`

```
raw_text_hash    PK
raw_text         TEXT
corrected_addr   TEXT
latitude         FLOAT
longitude        FLOAT
city             TEXT
zip_code         TEXT
corrected_by     TEXT
corrected_at     TIMESTAMPTZ
```

Human corrections always win over `geocode_aliases`.

### `brokers`

```
id                 PK
canonical_name     TEXT UNIQUE   -- "JLL"
aliases            JSONB         -- ["jones lang lasalle","jll capital markets"]
first_seen_at      TIMESTAMPTZ
upload_count       INT
confidence         FLOAT
is_brokerage       BOOL          -- False for "INTERNAL"
created_by         TEXT          -- "llm_auto" | "user_<email>"
```

### `pdf_extraction_corrections`

```
id              PK
pdf_hash        TEXT
page_num        INT
row_index       INT
field           TEXT
original_value  TEXT
corrected_value TEXT
corrected_by    TEXT
corrected_at    TIMESTAMPTZ
```

## Tiered Fingerprint Matching

Column mapping lookup runs in strict tier order. First hit wins.

### Tier 1 — Exact hash match

```sql
SELECT * FROM template_fingerprints WHERE fingerprint_hash = ?
```

Hit → return blueprint verbatim, confidence 1.0, source `"fingerprint_exact"`.
Cost: one indexed query.

### Tier 2 — Fuzzy header overlap

Pull all fingerprints with same `file_type`. Compute Jaccard similarity `|A ∩ B| / |A ∪ B|` of header sets. If best ≥ 0.80 → return blueprint with fields dropped for missing source headers, confidence = jaccard, source `"fingerprint_fuzzy"`.
Cost: ~100 set compares in memory.

### Tier 3 — Broker template

If a broker was extracted and tiers 1/2 missed, pull all fingerprints `WHERE broker = ?`. Find best Jaccard ≥ 0.60 (lower threshold because broker identity compensates). Return blueprint, confidence = jaccard × 0.9, source `"fingerprint_broker"`.

### Tier 4 — Correction-weighted embedding fallback

No blueprint match. Fall through to the existing embedding + Hungarian algorithm, but pull all `column_mapping_corrections` for this `(file_type, broker)` context. For any input column whose `clean_header` matches a prior correction, add a **+0.30 score bonus** to the mapping matrix cell for that target field. This is how the engine learns from individual header votes even when no full template matches.

### Blueprint Reuse Rules

- If a blueprint says `address → "Property"` but the current file has no `"Property"` column, drop that entry silently.
- If the current file has columns the blueprint doesn't cover, run the embedding fallback on just those uncovered columns and merge results.
- Never throw away a fingerprint match — always merge + backfill.

## Broker Detection

Brokers are discovered, not hardcoded. No regex list. Every new broker enters the system either via LLM extraction on upload or via a manual dropdown entry.

### Auto-Discovery Flow

On every upload, one cheap LLM call (~200 tokens):

```
Prompt: "Extract the brokerage/source name from this CRE spreadsheet.
Look at filename, sheet titles, and first 10 rows of raw data.
If it's clearly from a known brokerage (JLL, CBRE, Colliers, Cushman & Wakefield,
Newmark, Marcus & Millichap, Avison Young, Stream Realty, Partners Real Estate,
Lee & Associates, Transwestern, etc.), return the canonical name.
If it appears to be an internal/client sheet with no brokerage branding, return 'INTERNAL'.
If uncertain, return 'UNKNOWN'.
Respond JSON: {\"broker\": \"...\", \"confidence\": 0.0-1.0, \"evidence\": \"...\"}"
```

Inputs: filename, sheet name, first 10 raw rows joined as text.

### Alias Merging

If LLM returns "Jones Lang LaSalle" and a `brokers` row already has alias "jones lang lasalle" → merge, bump `upload_count`. If the new canonical name has ≥80% string similarity to an existing row → flag for user confirmation in UI.

### UI Integration

The upload review panel shows:

```
Detected source: [JLL ▼]  (auto-detected, 95% conf)
Options: [JLL] [CBRE] [Colliers] ... [+ New broker] [Internal/Harbor] [Unknown]
```

User can:
- Confirm implicitly by saving
- Switch to a different existing broker (adds alias to that broker's row)
- Type a new broker name (creates new `brokers` row)
- Pick "Internal" (broker = NULL for fingerprinting, flagged as Harbor-internal)
- Pick "Unknown" (broker = NULL, no broker-scoped learning)

### Sanity Check

If LLM guesses JLL but Tier 3 lookup for broker=JLL returns < 0.10 Jaccard overlap against existing JLL templates, the UI surfaces a warning: *"LLM guessed JLL but your file looks nothing like prior JLL templates — are you sure?"*

### Broker-Optional Pipeline

The entire pipeline works with `broker = NULL`. Tiers 1, 2, and 4 all run normally. Tier 3 simply doesn't fire. Harbor-internal sheets and unknown-broker files process fine; they just don't get broker-scoped learning boosts.

## Geocoding Pipeline

Three-layer lookup. Override always wins. Cache beats Google. LLM cleanup only on Google miss.

```python
def geocode_row(raw_text: str, store: LearningStore, api_key: str) -> GeocodeAnswer:
    key = hash_normalized(raw_text)

    # 1. Human override — always wins
    override = store.get_geocode_override(key)
    if override:
        return GeocodeAnswer(**override, source="override")

    # 2. Cache — prior Google/LLM answer
    cached = store.get_geocode_alias(key)
    if cached:
        store.bump_hit_count(key)
        return GeocodeAnswer(**cached, source="cache")

    # 3. Fresh Google lookup
    answer = _fetch_google(raw_text, api_key)
    if answer.is_valid():
        store.insert_geocode_alias(key, raw_text, answer, source="google")
        return answer

    # 3a. Google missed → LLM cleanup, retry Google
    cleaned = _llm_address_cleanup(raw_text)
    if cleaned and cleaned != raw_text:
        answer = _fetch_google(cleaned, api_key)
        if answer.is_valid():
            store.insert_geocode_alias(key, raw_text, answer, source="llm_cleanup_then_google")
            return answer

    # 4. Miss
    return GeocodeAnswer(raw=raw_text, source="miss", warning="Could not geocode")
```

### Normalization

```python
def normalize_raw_address(text: str) -> str:
    # lowercase, collapse whitespace, strip most punctuation (keep "/")
    # remove "bldg X", "ste Y", "unit Z", "floor N", "#N"
    # normalize trailing " tx" / ", texas" / "texas"
    return normalized
```

Hash = `sha256(normalized)`. Matching survives casing and whitespace noise.

### LLM Address Cleanup

Only fires on Google miss. ~150 tokens per call, ~5–10% of rows.

```
Prompt: "Clean this commercial real estate address string for geocoding.
Strip building names, suite numbers, notes, and parenthetical text.
Extract the street address, city, state, and zip if present.
Assume Texas if state is missing.
Return JSON: {\"cleaned\": \"...\", \"confidence\": 0.0-1.0}"
```

### Texas-Only Assumption

All properties guaranteed Texas. `_fetch_google` always appends `", TX"` if not present and passes `components=country:US|administrative_area:TX`. Out-of-Texas result = definite error, routed to LLM cleanup retry then to miss state.

### Batch Optimization

When processing a file, collect all unique `raw_text` values first, batch them through the cache/override lookup in one query per table, only hit Google for the real misses. On a 200-row file where 180 are already cached, this is ~20 Google calls instead of 200.

### Override Capture on Save

Any row where the user edits address, city, zip, latitude, or longitude in the review grid gets flagged `_geocode_overridden`. On save:

```python
store.record_geocode_override(
    raw_text=row['raw_address_data'],
    corrected_addr=row['address'],
    lat=row['latitude'], lng=row['longitude'],
    city=row['city'], zip_code=row['zip_code'],
    user=user_email,
)
```

This also upserts into `geocode_aliases` so future lookups hit cache + override.

## Vision PDF Extraction

New `engine/vision_pdf.py`. Activated by `.pdf` extension in `loaders.load()`.

### Flow

```python
def load_pdf_vision(path: str, filename: str) -> list[LoadedSegment]:
    pages = pdf_to_images(path, dpi=150)  # via pdf2image + poppler
    all_rows = []
    for page_num, img in enumerate(pages):
        response = openai_vision_extract(img, page_num, filename)
        if response.tables:
            all_rows.extend(response.tables)

    df = pd.DataFrame(all_rows)
    return [LoadedSegment(
        df=df,
        source_path=path,
        sheet_name=f"PDF pages 1-{len(pages)}",
        segment_title=None,
        segment_index=0,
        loader="pdf_vision",
        raw_headers=list(df.columns),
    )]
```

### Vision Prompt

One call per page. Uses `gpt-4o` (or `gpt-4o-mini` in cheap mode, controlled by config flag). `response_format={"type":"json_object"}` for structured output.

```
You are parsing a commercial real estate comp report PDF page for a Texas-based firm.

Extract every property data row you see on this page. For each row, return:
- file_type: "LEASE" or "SALE"
- address: full street address if visible (include city/state/zip if shown)
- tenant_name (leases only): tenant/lessee name
- leased_sf / building_size: square footage as integer
- sale_price (sales): dollar amount as number
- price_per_sf (sales): $/sf as number
- rate_psf (leases): rent rate as number; also return rate_basis: "monthly"|"annual" if shown
- closing_date / commencement_date: MM/DD/YYYY
- year_built, cap_rate, buyer, seller, lease_type, term_months, ti_allowance,
  free_rent, clear_height, building_type, escalations, notes

Skip header rows, footnotes, and marketing fluff.
Return JSON: {"rows": [...], "page_note": "what this page contains"}
If the page has no property data, return {"rows": [], "page_note": "..."}
```

### Bypass Mapping Stage

Vision output already uses target schema field names. Pipeline flags segment as `loader="pdf_vision"` and `mapping.map_columns()` short-circuits to identity mapping (every target field = itself, confidence 1.0, source `"vision_identity"`). Cleaning/geocoding stages run normally.

### PDF Fingerprinting

Hash = `sha256(filename_stem + page_count + first_200_chars_of_page_1_text)`. Same PDF layout re-uploaded → cache hit via `template_fingerprints` → skip vision call, reuse prior extraction stored in `pdf_extraction_corrections`.

### Cost Controls

- Max 20 pages per PDF by default (configurable). Pages beyond warn user.
- Config: `VISION_MODEL=gpt-4o-mini` (cheap) vs `gpt-4o` (accurate).
- Cached extractions never re-call vision.

### PDF Correction Learning

Any row edited in the review grid from a vision-extracted segment:

```python
store.record_pdf_correction(
    pdf_hash=fp.hash,
    page_num=row['_page_num'],
    row_index=row['_row_index'],
    field=changed_field,
    original_value=pre_edit_value,
    corrected_value=post_edit_value,
    corrected_by=user_email,
)
```

Next upload of the same PDF hash → reuse cached extraction + apply stored corrections automatically.

### Dependencies

- `pdf2image` (Python)
- `pillow` (Python)
- `poppler-utils` (system binary — added to Dockerfile)

## Manual Override + Writeback Hooks

This is where the learning loop actually closes. The UI is being redesigned in a parallel branch, so this design stays deliberately abstract about widget layout. It only specifies the data contracts the writeback layer needs.

### Required UI Signals

The writeback function needs three things from the UI at save time, regardless of widget structure:

1. **Final mapping per segment** — `dict[target_field, input_header]` reflecting any user edits
2. **Per-row geocode overrides** — list of rows where address/city/zip/lat/lng were edited, with the original `raw_address_data` and the corrected values
3. **Confirmed broker** — the final broker selection (possibly a newly-typed name)

### `persist_with_learning` Function

New function in `learning/corrections.py`, called from `app.py`'s save handler:

```python
def persist_with_learning(
    pipeline_result: PipelineResult,
    edited_df: pd.DataFrame,
    user_email: str,
    broker_confirmed: str | None,
    store: LearningStore,
):
    # 1. Save the actual comp records (existing save_records_to_db logic)
    saved_ids = save_records_to_db(edited_df)

    # 2. Broker — confirm or correct
    if broker_confirmed and broker_confirmed != pipeline_result.segments[0].broker_llm_guess:
        store.record_broker_correction(
            fingerprint_hash=pipeline_result.segments[0].fingerprint.hash,
            llm_guess=pipeline_result.segments[0].broker_llm_guess,
            confirmed=broker_confirmed,
            user=user_email,
        )
    if broker_confirmed:
        store.upsert_broker(broker_confirmed, user_email)

    # 3. Template fingerprint + mapping blueprint
    for seg in pipeline_result.segments:
        final_mapping = extract_final_mapping(edited_df, seg)
        store.record_accepted_mapping(
            fp=seg.fingerprint,
            final_mapping=final_mapping,
            user=user_email,
        )
        # Also upserts column_mapping_corrections votes per header

    # 4. Geocode overrides for edited rows
    for _, row in edited_df.iterrows():
        if row.get('_geocode_overridden'):
            store.record_geocode_override(
                raw_text=row['raw_address_data'],
                corrected_addr=row['address'],
                lat=row['latitude'], lng=row['longitude'],
                city=row['city'], zip_code=row['zip_code'],
                user=user_email,
            )

    # 5. PDF row corrections for vision-loaded segments
    for seg in pipeline_result.segments:
        if seg.loader == "pdf_vision":
            store.record_pdf_corrections(seg, edited_df, user_email)
```

### Implicit vs Explicit Correction Semantics

- **Implicit accept** (clean save, no edits): `sample_count += 1`, `confidence += 0.05` on the fingerprint. No blueprint changes.
- **Explicit correct**: the user edit strengthens the correction vote but does NOT overwrite the existing blueprint until `vote_count ≥ 2` for the disagreeing header→field pair. Prevents one analyst accidentally poisoning a template.

### Show-Your-Work Banner

At the top of the review panel, a small info banner makes the learning loop visible:

> 🧠 This template matched a prior JLL sales comps layout (confidence 94%). 12 of 14 columns were mapped automatically using the learned blueprint. Edit anything below if it looks wrong — your corrections improve future uploads.

Analyst role restriction: analysts can edit before save (corrections feed learning); admins can also edit post-save in the DB View but the post-save hook is **Phase 2 / deferred**, not part of this redesign.

## Seed Labeling Workflow

Bootstrap the learning store with labeled ground truth from the 16 sample files. One-shot tool + review UI.

### `tools/seed_from_samples.py`

```python
def main():
    sample_dir = "sample comp files"
    output_dir = "learning_data/labeled_samples"

    for filename in list_sample_files(sample_dir):
        result = run_pipeline(
            path=os.path.join(sample_dir, filename),
            filename=filename,
            store=EmptyLearningStore(),  # pure baseline
            google_api_key=os.environ["GOOGLE_MAPS_API_KEY"],
        )
        for seg in result.segments:
            labeled = {
                'filename': filename,
                'sheet': seg.segment.sheet_name,
                'segment_index': seg.segment.segment_index,
                'fingerprint': asdict(seg.fingerprint),
                'broker_llm_guess': seg.broker_llm_guess,
                'file_type': seg.mapping.file_type,
                'auto_mapping': seg.mapping.mappings,
                'auto_confidence': seg.mapping.confidence,
                'needs_review': flag_low_confidence(seg),  # fields below 0.70
                'sample_rows': seg.segment.df.head(5).to_dict('records'),
                'status': 'pending_review',
            }
            write_json(labeled, f"{output_dir}/{slug(filename)}__{seg.segment.segment_index}.json")
```

### Seed Review UI

New Streamlit page `"🎓 Seed Review"`, admin-only:

- Lists all `learning_data/labeled_samples/*.json` with status badges
- Pending review → opens an interactive editor showing raw sample rows, auto-detected mapping with confidence bars, broker dropdown
- User confirms broker, fixes low-confidence mappings, fixes rate units
- On Approve: JSON updates to `status: approved`, fingerprint + blueprint inserted into live learning store, corrections fire into `column_mapping_corrections`
- Approved samples also write to `learning_data/seed_fingerprints.json`, `seed_corrections.json`, `seed_brokers.json` — the committed baseline files

### Hybrid Automation

Any field the auto-pipeline labels with confidence ≥ 0.90 is pre-marked "approved" in the JSON. User only reviews sub-0.90 fields. Realistic estimate for 16 samples: ~40% of fields need human eye, ~60% auto-approved. Total review time: 15–20 minutes.

### Replay Guarantee

Seed JSON files are a recipe, not state. `tools/rebuild_learning_from_seed.py` wipes the learning tables and reinserts everything from `learning_data/`. Anyone clones the repo, runs that command, gets identical starting accuracy.

## Testing Strategy

### Unit Tests

One file per engine/learning module. Fast, no network. Mocks via `unittest.mock.patch` for OpenAI calls and `responses` for Google.

```
tests/
  fixtures/
    mini_lease.csv
    mini_sale.xlsx
    mini_broker_jll.xlsx
    mini_broker_jll_q2.xlsx    # same template, different rows → fingerprint match
    mini_pdf.pdf
    mini_fuzzy.xlsx            # 80% overlap with mini_broker_jll
  test_loaders.py
  test_vision_pdf.py
  test_fingerprint.py
  test_mapping.py
  test_cleaning.py
  test_geocoding.py
  test_validation.py
  test_pipeline.py
  test_learning_store.py
```

### `FakeLearningStore`

In-memory dict-backed implementation of the `LearningStore` protocol. Every test that needs the store uses this — no DB required for unit tests.

### Mock Boundaries

- `openai.embeddings.create` — deterministic np arrays via patch
- `openai.chat.completions.create` — fixture JSON responses for vision, LLM cleanup, broker detection
- `requests.get` (Google geocoding) — fixture JSON per test via `responses`

### Integration Test

One real end-to-end test using `sample comp files/` against a SQLite learning store. Gated with `pytest -m integration`, skipped by default. Uses live OpenAI + Google if keys present, otherwise auto-skips.

### Accuracy Regression Test (Quality Gate)

```python
def test_accuracy_regression():
    store = LearningStore(backend="sqlite:///:memory:")
    store.load_seed("learning_data/")

    results = {}
    for sample_file in SAMPLE_FILES:
        expected = load_labeled_expected(sample_file)
        actual = run_pipeline(sample_file, store=store, api_key=OS_KEY)
        results[sample_file] = compare(expected, actual)

    assert all(r.mapping_accuracy >= 0.90 for r in results.values())
    assert all(r.rate_unit_accuracy >= 0.95 for r in results.values())
    assert all(r.geocode_success >= 0.85 for r in results.values())
```

Marked `@pytest.mark.integration`. Runs only when explicitly requested. Becomes the quality gate — any engine change that drops below threshold fails.

### Coverage

- Target: ≥85% on `engine/` and `learning/`
- No target on `app.py` (UI — manual test)

## Migration + Rollout Plan

### Phase 0 — Prep
- Create `engine/` and `learning/` package skeletons
- Add `tests/fixtures/` with mini sample files
- Add deps: `pdf2image`, `pillow`, `pytest`, `responses`
- Add `poppler-utils` to Dockerfile
- Commit. No behavior change.

### Phase 1 — Learning Store Backend
- SQLAlchemy models in `learning/schemas.py`
- `Base.metadata.create_all(engine)` on first boot (no Alembic)
- `LearningStore`, `FakeLearningStore`, `EmptyLearningStore` implementations
- Seed JSON loader
- Unit tests for store
- Runs in prod but nothing queries it → zero user impact

### Phase 2 — Extract Existing Logic Into Stages
- Move current functions into `engine/` modules without behavior change
- `comp_engine.py` becomes a facade re-exporting from new locations
- Golden-file tests: current engine vs new-split engine on all 16 samples must be byte-identical

### Phase 3 — Fingerprinting + Tiered Lookup
- Build `fingerprint.py` + `match_fingerprint()`
- Wire into `mapping.py`: check store before Hungarian algo
- With empty store, Tier 4 handles everything → identical to current behavior
- Tests with `FakeLearningStore`

### Phase 4 — Writeback Hooks
- `persist_with_learning()` in `learning/corrections.py`
- Wire into `app.py` save handler (minimal diff)
- Analyst save = fingerprint + mapping recorded to live Supabase
- Still no PDF, no LLM geocode cleanup, no vision

### Phase 5 — Geocode Learning
- `geocode_cache.py` + override lookup
- Wire into `geocoding.py` lookup order
- Manual override capture in review grid
- LLM address cleanup fallback

### Phase 6 — Broker Detection
- LLM broker extraction + `brokers` table
- Broker dropdown integration point (merges with parallel UI branch)
- Tier 3 activates

### Phase 7 — Vision PDF
- `vision_pdf.py` + PDF loader branch
- Vision mapping bypasses tiers (identity mapping)
- PDF fingerprinting
- Mocked vision tests

### Phase 8 — Seed the Store
- Run `tools/seed_from_samples.py` locally
- Approve in Seed Review UI
- Commit `learning_data/*.json`
- Verify with `tools/rebuild_learning_from_seed.py` against local SQLite

### Phase 9 — Regression Gate + Cleanup
- Enable accuracy regression test in CI (manual trigger)
- Delete unused code in old `comp_engine.py` internals
- Update `APP_OVERVIEW.md`

### Rollback

Each phase is independently revertable. Phases 1–3 are purely additive (no behavior change). Phase 4 is the first phase with user-visible effect; it's one function call that can be commented out.

### Local Testing Gate

After Phase 8, spin up local Streamlit instance. User tests with real files, iterates on tweaks. **No prod deploy until user sign-off.** Explicit user requirement.

## Integration Note: Parallel UI Branch

The user is making UI changes to the manual mapping interface on another branch in parallel. This design deliberately stays abstract about widget structure in the writeback section. The only contract with the UI is:

1. Final mapping per segment (dict)
2. Per-row geocode overrides (list of flagged rows)
3. Confirmed broker (string)

These three values feed `persist_with_learning()` from whatever UI layout ultimately exists. When the parallel branch merges, integration is: call the same function from the new save handler. Zero coupling to widget layout.

## Dependencies Added

- `pdf2image` — PDF rendering for vision pipeline
- `pillow` — image handling
- `pytest` — test runner (if not already present)
- `responses` — HTTP mock library for geocode tests
- `poppler-utils` (system) — PDF rendering binary (Dockerfile)

## Open Questions (Deferred to Implementation Plan)

None blocking. All design decisions resolved in Q1–Q7 clarification round and the 10 design sections above.

## Success Criteria

- Accuracy regression test passes: ≥90% mapping accuracy, ≥95% rate unit accuracy, ≥85% geocoding success on all 16 sample files
- Implicit save on a repeat template = zero LLM calls for mapping (fingerprint hit)
- Geocoding Google call count drops ≥80% on second-upload of same-address files
- PDF files successfully extract via vision + get mapped to schema
- User approves local test run before any prod push
