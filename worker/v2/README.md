# Harbor Comp Database v2

Live at https://comps.harborcap.dev (behind Cloudflare Access). Runs entirely in
the `harbor-comps` Worker: D1 (`harbor-comps`) for data, R2
(`harbor-comps-files`) for uploaded files. The legacy FastAPI container still
serves `/database`, `/analytics` ("Classic view"); `/finder` and `/upload`
now redirect to the v2 app.

## Pipeline (Add comps)

1. **Render** (`render.js`) - every sheet cell with its Excel address, raw
   numbers behind formatted text (`$6.50 [=6.5]`), dates (`[date 2024-10-01]`),
   hidden rows/sheets, merged ranges, hyperlink targets + map-link coordinates.
   PDFs are rebuilt into layout-preserving lines; pasted Slack/email text is
   read line by line; scanned PDFs are rendered to images in the browser and
   read by a vision model.
2. **Survey** (LLM) - maps every table: sale / lease / listing / land, header
   rows, data rows, totals to skip, subject rows, column units, summary vs
   detail tables, mislabeled sheet names.
3. **Extract** (LLM, per ~22-row chunk) - schema records (`SCHEMA.md`, ~60
   fields incl. building features) with a cell citation for every value.
4. **Review** (LLM) - the model re-reads the rows next to its own output and
   patches misses (comment-cell facts, title implications, tenant columns,
   listings, units).
5. **Verify** (code) - every number must match its cited cell, allowing only
   the schema's unit conversions; anything else is flagged "unverified".
6. **Validate** (`validate.js`) - unit repairs (monthly vs annual rent, decimal
   caps, $K prices, TI totals, land SF in acre columns, inches, 19xx dates),
   realistic-bounds checks, cross-field checks (price/SF vs price/size,
   leased > building, coverage, free rent vs term), completeness.
7. **Locate** (`geo.js`) - Google then Esri geocoding with a quality grade; the
   broker's address is never overwritten.
8. **Match & dedupe** (`dedupe.js`, `store.js`) - same building (address
   normalization incl. FM/IH/BW8/suffix/direction/ranges/building letters +
   distance + name) then same deal (date/price/tenant/SF windows). Outcomes:
   new / same building / duplicate (merge missing fields, keep every source) /
   conflict (held for a human). `POST /api/v2/admin/dedupe-sweep` re-runs this
   over the whole DB.

Models: `LLM_PROVIDER=deepseek` (DeepSeek key rotated 2026-11-07; OpenAI key on the Worker has no credits and is skipped automatically).
Spreadsheets use `deepseek-flash`, PDFs/text `deepseek-v4-pro`; OpenAI
(`gpt-5.5`) is supported and automatically skipped while out of credits.

### Accuracy (21-file hand-extracted gold set, 393 records, 5,781 fields)

| | record recall | precision | field accuracy | core fields |
|---|---|---|---|---|
| Legacy tool (column mapping) | 54.2% | 95.5% | 55.8% | 82.5% |
| v2 (production config) | 98.0% | 100% | 97.6% | 99.0% |

Re-run: `node worker/v2/eval/run-remote.mjs <out> deepseek deepseek-flash <files>`
then `node worker/v2/eval/compare.mjs <gold> <out> --diffs`. Gold files are
kept outside the (public) repo because they contain broker data.

## Building features

Stored per building as observations (`v2_feature_obs`) with source, reference
and confidence; the property row holds the winning value and conflicts are
surfaced. Sources, best first: manual edit > broker sheet > county appraisal /
CoStar index > parcel map > web listing > satellite read.

- CoStar index: 7,124 TX industrial buildings (clear height, docks, drive-ins,
  class, year built, land, type) from Harbor's CoStar export.
- County appraisal: TxGIO StratMap parcel (all counties: owner, acres,
  account) then building SF / year built / quality from HCAD (2026 CAMA table
  in D1, 71,261 F1/F2 accounts), DCAD, Collin CAD, and the True Prodigy CAD
  API (Travis, Tarrant, Montgomery, Denton, Webb, Ellis, Johnson, Rockwall).
- Satellite: Esri World Imagery tile read by a vision model (loading side,
  rail, yard, tenancy) - low confidence, on demand.
- Web listings: OpenAI web search (only when OpenAI has credits).

A cron (every 10 min) enriches pending buildings; the drawer has
"Re-source features", "+ Satellite read" and "Edit features".

## Comp finder

Free text (deal summary, address, submarket, size band, Slack shorthand) is
parsed by an LLM (raced across providers, deterministic fallback), locations
resolve via address geocoding, a gazetteer of Harbor's submarket vocabulary
and CoStar submarket centroids. When an address is given, the subject's own
size / vintage / clear height / product are looked up and used as targets.
Scoring is per factor (location, size, recency, product, clear height,
vintage, features, data completeness) with hard filters only where the
analyst was explicit; near misses are shown and labelled; deals at the subject
building are listed separately; an LLM writes a one-line note per top comp.

## Ops

- Deploy: `npx wrangler deploy` (container image pinned by digest - no Docker needed).
- Schema: `worker/v2/migrations/0001_v2.sql`.
- Tests: `node worker/v2/tests/edge.test.mjs` (address, dedupe, units, dates, parsing).
- Admin (service token or ADMIN_EMAILS): `/api/v2/admin/dedupe-sweep`,
  `/admin/revalidate`, `/admin/refresh`, `/admin/ingest`, `/admin/jobs`.
