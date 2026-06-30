# CoStar Comp Enrichment Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Given a comp's address, automatically resolve it to its CoStar property, scrape a rich set of physical building specs, and attach them to the comp record — via an on-demand LOCAL `costar enrich` CLI that never overwrites analyst-entered values.

**Architecture:** A local enrichment worker lives in the **`CoStar-Market-Extraction`** repo (it already owns the CDP/Chrome attach, the `scrape` CLI, and the CoStar page parsers). It reads comps marked `costar_status='pending'` from the comp tool's shared Supabase, resolves each address → CoStar property ID, scrapes the detail page, extracts industrial specs, and writes `costar_*` fields back. The **`Harbor-Capital-Scraper`** (comp tool, cloud) only gains DB columns + light UI. Scraping runs only where the analyst's authenticated CoStar Chrome is.

**Tech Stack:** Python 3.11, Playwright-over-CDP (existing), SQLAlchemy (existing in both repos), OpenAI/Anthropic LLM seam for gap-fill (existing `llm_extractor`), pytest.

**Spec:** `docs/superpowers/specs/2026-06-29-costar-comp-enrichment-design.md`

## Global Constraints

- **Two repos.** Each task header states its repo. Worker code → `~/HarborCapital/CoStar-Market-Extraction`. DB/UI → `~/HarborCapital/Harbor-Capital-Scraper`.
- **Compliance:** all CoStar access is via CDP to the analyst's already-logged-in Chrome on `localhost:9222`. Never bypass login/Akamai. Process serially with per-property delays. The cloud app NEVER scrapes CoStar.
- **Never overwrite analyst data.** Worker writes ONLY `costar_*` columns. Analyst columns (address, price, sf, …) are read-only to the worker.
- **Never fabricate.** A spec field CoStar doesn't expose stays absent/None. A weak/ambiguous address match is recorded as `ambiguous`, never guessed.
- **Idempotent / resumable:** `read_pending` returns only `costar_status='pending'`, so completed comps are skipped on re-run.
- CoStar URL patterns (verbatim from `costar_market/cdp_scraper.py`): base `https://product.costar.com`; detail `/detail/lookup/{pid}/summary`; PID regex `r"/detail/(?:lookup|all-properties)/(\d+)(?:/|$|\?)"`.
- `costar_status` values: `pending` → (`enriched` | `ambiguous` | `not_found` | `error`).
- PEP 8, type annotations, files <400 lines. Tests: pytest.

---

### Task 0: Phase-0 CoStar address-search endpoint discovery (MANUAL — gates Task 1's live path)

CoStar's search is an SPA; URL params don't carry filters. We must observe the internal
address-search XHR before we can call it. This is a manual, exploratory task with a documented
deliverable. The pure parsing/match logic (Task 1) is built and tested independently of this, so
the plan is not blocked — but the resolver's LIVE path needs these findings.

**Repo:** `CoStar-Market-Extraction`
**Files:**
- Create: `docs/costar-search-endpoint.md`
- Create: `tests/fixtures/costar_search_response.json` (a real captured response, secrets/PII trimmed)

- [ ] **Step 1: Launch Chrome with CDP and log into CoStar**

```bash
# Use the project's launcher if present, else:
/Applications/Google\ Chrome.app/Contents/MacOS/Google\ Chrome \
  --remote-debugging-port=9222 --user-data-dir="$HOME/.chrome-costar-profile" &
```
Log into `https://product.costar.com` in that Chrome window.

- [ ] **Step 2: Instrument network and search by address**

In a Python REPL inside the repo venv:

```python
from costar_market.cdp_scraper import attach_to_chrome
browser = attach_to_chrome(9222)
page = browser.contexts[0].pages[0]
page.on("response", lambda r: print(r.request.method, r.url) if "search" in r.url.lower() or "suggest" in r.url.lower() or "autocomplete" in r.url.lower() else None)
# Now, in the Chrome UI, type a full address into CoStar's property search box and pick a result.
```
Watch the printed requests. Identify the XHR that returns property candidates for the typed
address (often an autocomplete/suggest or a search POST returning JSON).

- [ ] **Step 3: Capture the request + response**

For the identified request, record in `docs/costar-search-endpoint.md`:
- Full URL (with path), HTTP method.
- Request payload / query params (which field carries the address text).
- Response JSON shape: where the candidate list lives, and per-candidate where the property ID,
  display address, and property type are.
Save one real response body to `tests/fixtures/costar_search_response.json` (trim any account/PII).

- [ ] **Step 4: Reconcile Task 1's parser to the real shape**

Note in the doc the exact JSON paths so Task 1's `parse_candidates` matches reality. If the shape
differs from Task 1's assumed shape, update `parse_candidates` + its fixture accordingly.

- [ ] **Step 5: Commit**

```bash
git add docs/costar-search-endpoint.md tests/fixtures/costar_search_response.json
git commit -m "docs: capture CoStar address-search endpoint shape (phase 0 spike)"
```

---

### Task 1: Address→PID resolver — parsing + match policy (`costar_market/lookup.py`)

The missing primitive. Pure functions (candidate parsing + match selection) are fully unit-tested;
the live XHR call is a thin, separately-noted wrapper that uses the Task-0 findings.

**Repo:** `CoStar-Market-Extraction`
**Files:**
- Create: `costar_market/lookup.py`
- Create: `tests/test_lookup.py`
- Use: `tests/fixtures/costar_search_response.json` (Task 0)

**Interfaces:**
- Produces:
  - `Candidate` (frozen dataclass): `pid: str`, `label: str`, `address: str`, `property_type: str | None`, `score: float`.
  - `parse_candidates(payload: dict) -> list[Candidate]`.
  - `choose_match(candidates: list[Candidate], query_address: str) -> tuple[str, str | None, list[Candidate]]`
    → `(status, pid_or_None, candidates)`, status ∈ `{"matched", "ambiguous", "not_found"}`.
  - `resolve_address(page, address: str) -> tuple[str, str | None, list[Candidate]]` (live; calls `_fetch_search_results` then parse+choose).

- [ ] **Step 1: Write the failing test**

Create `tests/test_lookup.py`:

```python
from costar_market.lookup import Candidate, parse_candidates, choose_match


def test_parse_candidates_extracts_pid_and_address():
    payload = {"results": [
        {"id": "12345", "displayAddress": "1326 W Carrier Pkwy, Grand Prairie, TX",
         "propertyType": "Industrial"},
        {"id": "67890", "displayAddress": "615 S Wisteria St, Mansfield, TX",
         "propertyType": "Industrial"},
    ]}
    cands = parse_candidates(payload)
    assert cands[0].pid == "12345"
    assert "Carrier" in cands[0].address
    assert cands[0].property_type == "Industrial"


def test_choose_match_single_candidate_is_matched():
    c = [Candidate(pid="12345", label="x", address="1326 W Carrier Pkwy, Grand Prairie, TX",
                   property_type="Industrial", score=0.0)]
    status, pid, _ = choose_match(c, "1326 W Carrier Pkwy, Grand Prairie TX")
    assert status == "matched"
    assert pid == "12345"


def test_choose_match_no_candidates_is_not_found():
    status, pid, cands = choose_match([], "anywhere")
    assert status == "not_found"
    assert pid is None
    assert cands == []


def test_choose_match_clear_winner_is_matched():
    c = [
        Candidate("1", "x", "1326 W Carrier Pkwy, Grand Prairie, TX", "Industrial", 0.0),
        Candidate("2", "y", "9 Nowhere Rd, El Paso, TX", "Industrial", 0.0),
    ]
    status, pid, _ = choose_match(c, "1326 W Carrier Pkwy, Grand Prairie TX")
    assert status == "matched"
    assert pid == "1"


def test_choose_match_two_close_addresses_is_ambiguous():
    c = [
        Candidate("1", "x", "1326 W Carrier Pkwy, Grand Prairie, TX", "Industrial", 0.0),
        Candidate("2", "y", "1326 W Carrier Pkwy Ste B, Grand Prairie, TX", "Industrial", 0.0),
    ]
    status, pid, cands = choose_match(c, "1326 W Carrier Pkwy, Grand Prairie TX")
    assert status == "ambiguous"
    assert pid is None
    assert len(cands) == 2
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd ~/HarborCapital/CoStar-Market-Extraction && .venv/bin/pytest tests/test_lookup.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'costar_market.lookup'`

- [ ] **Step 3: Implement**

Create `costar_market/lookup.py`:

```python
"""Address -> CoStar property-ID resolver.

parse_candidates / choose_match are pure and unit-tested. resolve_address performs
the live XHR call (shape captured in docs/costar-search-endpoint.md, Phase-0 spike).
"""
from __future__ import annotations

from dataclasses import dataclass
from difflib import SequenceMatcher

_MATCH_THRESHOLD = 0.72   # min similarity for the top candidate to count as a match
_AMBIGUITY_MARGIN = 0.08  # if 2nd-best is within this of the best, it's ambiguous


@dataclass(frozen=True)
class Candidate:
    pid: str
    label: str
    address: str
    property_type: str | None
    score: float


def _norm(text: str) -> str:
    return " ".join((text or "").lower().replace(",", " ").split())


def parse_candidates(payload: dict) -> list[Candidate]:
    """Parse the CoStar address-search JSON into Candidates.

    Result-list and field paths reconciled to the real response in the Phase-0 spike
    (docs/costar-search-endpoint.md). Default shape: payload['results'][*] with
    'id', 'displayAddress', 'propertyType'.
    """
    results = payload.get("results") or payload.get("items") or []
    cands: list[Candidate] = []
    for r in results:
        pid = str(r.get("id") or r.get("propertyId") or "").strip()
        if not pid:
            continue
        address = str(r.get("displayAddress") or r.get("address") or "").strip()
        cands.append(Candidate(
            pid=pid,
            label=address or pid,
            address=address,
            property_type=r.get("propertyType"),
            score=0.0,
        ))
    return cands


def choose_match(
    candidates: list[Candidate], query_address: str
) -> tuple[str, str | None, list[Candidate]]:
    """Pick the best candidate. Returns (status, pid|None, scored_candidates)."""
    if not candidates:
        return "not_found", None, []

    q = _norm(query_address)
    scored = sorted(
        (Candidate(c.pid, c.label, c.address, c.property_type,
                   SequenceMatcher(None, q, _norm(c.address)).ratio())
         for c in candidates),
        key=lambda c: c.score, reverse=True,
    )

    if len(scored) == 1:
        top = scored[0]
        return ("matched", top.pid, scored) if (top.score >= _MATCH_THRESHOLD or len(candidates) == 1) else ("ambiguous", None, scored)

    best, second = scored[0], scored[1]
    if best.score >= _MATCH_THRESHOLD and (best.score - second.score) >= _AMBIGUITY_MARGIN:
        return "matched", best.pid, scored
    return "ambiguous", None, scored


def _fetch_search_results(page, address: str) -> dict:
    """LIVE: call CoStar's address-search XHR via the authenticated page context.

    Endpoint URL/method/params come from docs/costar-search-endpoint.md (Phase-0 spike).
    Implemented after Task 0. Example shape (replace with captured values):

        resp = page.request.get(
            "https://product.costar.com/<search-path>",
            params={"<addressParam>": address},
        )
        return resp.json()
    """
    raise NotImplementedError(
        "Wire to the CoStar search endpoint captured in docs/costar-search-endpoint.md"
    )


def resolve_address(page, address: str) -> tuple[str, str | None, list[Candidate]]:
    """LIVE: resolve an address to a CoStar pid via search XHR + match policy."""
    payload = _fetch_search_results(page, address)
    return choose_match(parse_candidates(payload), address)
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `.venv/bin/pytest tests/test_lookup.py -v`
Expected: PASS (pure functions). `_fetch_search_results` is wired live in/after Task 0.

- [ ] **Step 5: Commit**

```bash
git add costar_market/lookup.py tests/test_lookup.py
git commit -m "feat: address->CoStar pid resolver (parse + match policy)"
```

---

### Task 2: Industrial spec extractor (`costar_market/industrial_specs.py`)

Pull the rich physical field set from a scraped CoStar summary, regex first, optional LLM gap-fill.

**Repo:** `CoStar-Market-Extraction`
**Files:**
- Create: `costar_market/industrial_specs.py`
- Create: `tests/test_industrial_specs.py`
- Create: `tests/fixtures/costar_summary_sample.txt` (paste a REAL scraped `summary.txt` here during Phase 0; a starter is given below)

**Interfaces:**
- Produces: `extract_industrial_specs(summary_text: str, llm=None) -> dict` — keys present only when found
  (subset of: `rba_sf`, `year_built`, `building_class`, `construction_type`, `clear_height_ft`,
  `dock_high_doors`, `drive_in_doors`, `office_sf`, `land_acres`, `sprinkler_type`, `power_amps`,
  `parking_ratio`, `submkt_vacancy_pct`, `submkt_avg_rent_psf`, `submkt_cap_rate`).

- [ ] **Step 1: Create the fixture**

Create `tests/fixtures/costar_summary_sample.txt` (replace with a real capture in Phase 0):

```
1326 W Carrier Pkwy | Grand Prairie, TX 75050
Building Class: B
Year Built: 1998
RBA: 120,500 SF
Clear Height: 28'
Dock High Doors: 14
Drive In Doors: 2
Office SF: 8,000 SF
Land Area: 6.2 AC
Construction: Tilt-Wall
Sprinklers: ESFR
Power: 1,200 A
Parking Ratio: 1.50/1,000 SF
Submarket Vacancy: 5.4%
Submarket Asking Rent: $7.44/SF
Submarket Cap Rate: 6.1%
```

- [ ] **Step 2: Write the failing test**

Create `tests/test_industrial_specs.py`:

```python
import os
from costar_market.industrial_specs import extract_industrial_specs

_FIX = os.path.join(os.path.dirname(__file__), "fixtures", "costar_summary_sample.txt")


def _text():
    with open(_FIX, encoding="utf-8") as f:
        return f.read()


def test_extracts_core_physical_specs():
    specs = extract_industrial_specs(_text())
    assert specs["rba_sf"] == 120500
    assert specs["year_built"] == 1998
    assert specs["clear_height_ft"] == 28
    assert specs["dock_high_doors"] == 14
    assert specs["drive_in_doors"] == 2
    assert specs["land_acres"] == 6.2
    assert specs["construction_type"].lower().startswith("tilt")
    assert specs["sprinkler_type"] == "ESFR"


def test_extracts_submarket_context():
    specs = extract_industrial_specs(_text())
    assert specs["submkt_vacancy_pct"] == 5.4
    assert specs["submkt_avg_rent_psf"] == 7.44
    assert specs["submkt_cap_rate"] == 6.1


def test_missing_fields_are_absent_not_fabricated():
    specs = extract_industrial_specs("1 Main St | Dallas, TX\nYear Built: 2001")
    assert specs["year_built"] == 2001
    assert "clear_height_ft" not in specs  # never fabricated when absent


def test_llm_gap_fill_only_for_missing_fields():
    calls = {"n": 0}

    def fake_llm(text, missing_fields):
        calls["n"] += 1
        return {"clear_height_ft": 32}

    specs = extract_industrial_specs("Year Built: 2001", llm=fake_llm)
    assert specs["year_built"] == 2001       # regex
    assert specs["clear_height_ft"] == 32     # llm gap-fill
    assert calls["n"] == 1
```

- [ ] **Step 3: Run test to verify it fails**

Run: `.venv/bin/pytest tests/test_industrial_specs.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'costar_market.industrial_specs'`

- [ ] **Step 4: Implement**

Create `costar_market/industrial_specs.py`:

```python
"""Extract rich industrial physical specs from a scraped CoStar summary.

Regex pre-pass; optional LLM gap-fill (seam) for fields regex misses. Never fabricates:
a field absent from the source is simply not in the returned dict.
"""
from __future__ import annotations

import re
from typing import Callable, Optional

# All numeric-bearing fields the worker tries to capture (drives LLM gap-fill list)
_FIELDS = [
    "rba_sf", "year_built", "building_class", "construction_type", "clear_height_ft",
    "dock_high_doors", "drive_in_doors", "office_sf", "land_acres", "sprinkler_type",
    "power_amps", "parking_ratio", "submkt_vacancy_pct", "submkt_avg_rent_psf",
    "submkt_cap_rate",
]


def _int(s: str) -> int:
    return int(s.replace(",", ""))


def _regex_specs(text: str) -> dict:
    t = text
    out: dict = {}

    def search(pattern, key, cast):
        m = re.search(pattern, t, re.IGNORECASE)
        if m:
            try:
                out[key] = cast(m.group(1))
            except (ValueError, IndexError):
                pass

    search(r"RBA[:\s]+([\d,]+)\s*SF", "rba_sf", _int)
    search(r"Year Built[:\s]+(\d{4})", "year_built", int)
    search(r"Building Class[:\s]+([A-C])", "building_class", str.strip)
    search(r"Construction[:\s]+([A-Za-z\- ]+)", "construction_type", str.strip)
    search(r"Clear Height[:\s]+(\d+)", "clear_height_ft", int)
    search(r"Dock High Doors[:\s]+(\d+)", "dock_high_doors", int)
    search(r"Drive In Doors[:\s]+(\d+)", "drive_in_doors", int)
    search(r"Office SF[:\s]+([\d,]+)", "office_sf", _int)
    search(r"Land Area[:\s]+([\d.]+)\s*AC", "land_acres", float)
    search(r"Sprinklers?[:\s]+([A-Za-z]+)", "sprinkler_type", str.strip)
    search(r"Power[:\s]+([\d,]+)\s*A", "power_amps", _int)
    search(r"Parking Ratio[:\s]+([\d.]+)", "parking_ratio", float)
    search(r"Submarket Vacancy[:\s]+([\d.]+)%", "submkt_vacancy_pct", float)
    search(r"Submarket Asking Rent[:\s]+\$([\d.]+)", "submkt_avg_rent_psf", float)
    search(r"Submarket Cap Rate[:\s]+([\d.]+)%", "submkt_cap_rate", float)
    return out


def extract_industrial_specs(
    summary_text: str,
    llm: Optional[Callable[[str, list[str]], dict]] = None,
) -> dict:
    """Return found specs. Regex first; if llm given, fill ONLY still-missing fields.

    llm(summary_text, missing_fields) -> {field: value}. Returned values for fields
    not in `missing_fields` are ignored (no overwrite of regex hits).
    """
    specs = _regex_specs(summary_text)
    if llm is not None:
        missing = [f for f in _FIELDS if f not in specs]
        if missing:
            filled = llm(summary_text, missing) or {}
            for field, value in filled.items():
                if field in missing and value is not None:
                    specs[field] = value
    return specs
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `.venv/bin/pytest tests/test_industrial_specs.py -v`
Expected: PASS
> NOTE: During Phase 0, replace the fixture with a REAL `summary.txt` and adjust regex patterns
> to the actual CoStar label text. The test asserts the contract; the patterns track reality.

- [ ] **Step 6: Commit**

```bash
git add costar_market/industrial_specs.py tests/test_industrial_specs.py tests/fixtures/costar_summary_sample.txt
git commit -m "feat: industrial spec extractor (regex + LLM gap-fill, never fabricate)"
```

---

### Task 3: Comp DB adapter (`costar_market/comp_sink.py`)

Read pending comps from the comp tool's shared DB and write `costar_*` fields back — never
touching analyst columns.

**Repo:** `CoStar-Market-Extraction`
**Files:**
- Create: `costar_market/comp_sink.py`
- Create: `tests/test_comp_sink.py`

**Interfaces:**
- Produces:
  - `CompSink(db_url: str)`.
  - `read_pending(comp_type: str, limit: int = 50) -> list[dict]` → rows `{id, address, city, zip_code}` where `costar_status='pending'`. `comp_type` ∈ `{"sale","lease"}`.
  - `write_enrichment(comp_type, comp_id, costar_property_id, costar_url, specs: dict) -> None` (sets status `enriched` + `costar_enriched_at`).
  - `write_status(comp_type, comp_id, status, candidates: list | None = None) -> None`.

- [ ] **Step 1: Write the failing test**

Create `tests/test_comp_sink.py`:

```python
import json
import pytest
from sqlalchemy import create_engine, text
from costar_market.comp_sink import CompSink

_DDL = """
CREATE TABLE sale_comps (
    id INTEGER PRIMARY KEY, address TEXT, city TEXT, zip_code TEXT,
    sale_price REAL, building_size REAL,
    costar_property_id TEXT, costar_url TEXT, costar_specs TEXT,
    costar_status TEXT DEFAULT 'pending', costar_candidates TEXT, costar_enriched_at TEXT
);
"""


@pytest.fixture
def db_url(tmp_path):
    url = f"sqlite:///{tmp_path/'comps.db'}"
    eng = create_engine(url)
    with eng.begin() as c:
        c.execute(text(_DDL))
        c.execute(text(
            "INSERT INTO sale_comps (id, address, city, zip_code, sale_price, costar_status) "
            "VALUES (1, '615 S Wisteria St', 'Mansfield', '76063', 3217500, 'pending'), "
            "(2, 'Done St', 'Dallas', '75201', 100, 'enriched')"
        ))
    return url


def test_read_pending_returns_only_pending(db_url):
    rows = CompSink(db_url).read_pending("sale")
    ids = [r["id"] for r in rows]
    assert 1 in ids and 2 not in ids
    assert rows[0]["address"] == "615 S Wisteria St"


def test_write_enrichment_sets_costar_fields_only(db_url):
    sink = CompSink(db_url)
    sink.write_enrichment("sale", 1, "12345",
                          "https://product.costar.com/detail/lookup/12345/summary",
                          {"clear_height_ft": 28, "rba_sf": 120500})
    eng = create_engine(db_url)
    with eng.connect() as c:
        row = c.execute(text("SELECT * FROM sale_comps WHERE id=1")).mappings().one()
    assert row["costar_property_id"] == "12345"
    assert row["costar_status"] == "enriched"
    assert json.loads(row["costar_specs"])["clear_height_ft"] == 28
    assert row["costar_enriched_at"] is not None
    assert row["sale_price"] == 3217500  # analyst column untouched


def test_write_status_ambiguous_stores_candidates(db_url):
    sink = CompSink(db_url)
    sink.write_status("sale", 1, "ambiguous",
                      candidates=[{"pid": "1", "address": "a"}, {"pid": "2", "address": "b"}])
    eng = create_engine(db_url)
    with eng.connect() as c:
        row = c.execute(text("SELECT costar_status, costar_candidates FROM sale_comps WHERE id=1")).mappings().one()
    assert row["costar_status"] == "ambiguous"
    assert len(json.loads(row["costar_candidates"])) == 2
```

- [ ] **Step 2: Run test to verify it fails**

Run: `.venv/bin/pytest tests/test_comp_sink.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'costar_market.comp_sink'`

- [ ] **Step 3: Implement**

Create `costar_market/comp_sink.py`:

```python
"""Adapter to the comp tool's shared DB. Writes ONLY costar_* columns; never analyst data."""
from __future__ import annotations

import json
from datetime import datetime

from sqlalchemy import create_engine, text

_TABLES = {"sale": "sale_comps", "lease": "lease_comps"}


class CompSink:
    def __init__(self, db_url: str):
        if db_url not in (None, ""):
            self._engine = create_engine(db_url)
        else:
            raise ValueError("CompSink requires a db_url (COMP_DB_URL)")

    def _table(self, comp_type: str) -> str:
        if comp_type not in _TABLES:
            raise ValueError(f"comp_type must be 'sale' or 'lease', got {comp_type!r}")
        return _TABLES[comp_type]

    def read_pending(self, comp_type: str, limit: int = 50) -> list[dict]:
        tbl = self._table(comp_type)
        with self._engine.connect() as c:
            rows = c.execute(text(
                f"SELECT id, address, city, zip_code FROM {tbl} "
                f"WHERE costar_status = 'pending' ORDER BY id LIMIT :lim"
            ), {"lim": limit}).mappings().all()
        return [dict(r) for r in rows]

    def write_enrichment(self, comp_type, comp_id, costar_property_id, costar_url, specs: dict) -> None:
        tbl = self._table(comp_type)
        with self._engine.begin() as c:
            c.execute(text(
                f"UPDATE {tbl} SET costar_property_id=:pid, costar_url=:url, "
                f"costar_specs=:specs, costar_status='enriched', costar_enriched_at=:ts "
                f"WHERE id=:id"
            ), {"pid": costar_property_id, "url": costar_url,
                "specs": json.dumps(specs), "ts": datetime.utcnow().isoformat() + "Z",
                "id": comp_id})

    def write_status(self, comp_type, comp_id, status, candidates=None) -> None:
        tbl = self._table(comp_type)
        with self._engine.begin() as c:
            c.execute(text(
                f"UPDATE {tbl} SET costar_status=:st, costar_candidates=:cands WHERE id=:id"
            ), {"st": status,
                "cands": json.dumps(candidates) if candidates is not None else None,
                "id": comp_id})
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `.venv/bin/pytest tests/test_comp_sink.py -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add costar_market/comp_sink.py tests/test_comp_sink.py
git commit -m "feat: comp DB sink (writes costar_* only, never analyst columns)"
```

---

### Task 4: Enrichment orchestration + `costar enrich` CLI

Wire read_pending → resolve → scrape → extract → writeback, with injectable seams for tests, and
expose it as a CLI subcommand.

**Repo:** `CoStar-Market-Extraction`
**Files:**
- Create: `costar_market/enrich.py`
- Modify: `costar_market/cli.py` (add `enrich` subparser; pattern mirrors the existing `scrape` subcommand at lines 299-328 and 400-426)
- Create: `tests/test_enrich.py`

**Interfaces:**
- Consumes: `CompSink` (Task 3), `resolve_address` (Task 1), `scrape_property`/`url_for`/`CAPTURE_PLAN`/`attach_to_chrome` (`cdp_scraper.py`), `extract_industrial_specs` (Task 2).
- Produces: `run_enrich(sink, page, resolver, scraper, extractor, comp_type, limit=50, dry_run=False, delay=1.0, sleep=time.sleep) -> dict` returning counts `{"enriched","ambiguous","not_found","error"}`.

- [ ] **Step 1: Write the failing test**

Create `tests/test_enrich.py`:

```python
from costar_market.enrich import run_enrich
from costar_market.lookup import Candidate


class FakeSink:
    def __init__(self, pending):
        self._pending = pending
        self.enriched = []
        self.statuses = []

    def read_pending(self, comp_type, limit=50):
        return self._pending

    def write_enrichment(self, comp_type, comp_id, pid, url, specs):
        self.enriched.append((comp_id, pid, specs))

    def write_status(self, comp_type, comp_id, status, candidates=None):
        self.statuses.append((comp_id, status))


def test_run_enrich_matched_writes_specs():
    sink = FakeSink([{"id": 1, "address": "1326 W Carrier Pkwy, Grand Prairie TX",
                      "city": "Grand Prairie", "zip_code": "75050"}])

    def resolver(page, address):
        return "matched", "12345", [Candidate("12345", "x", address, "Industrial", 1.0)]

    def scraper(page, costar_url):
        return "summary text here"

    def extractor(text):
        return {"clear_height_ft": 28}

    counts = run_enrich(sink, page=object(), resolver=resolver, scraper=scraper,
                        extractor=extractor, comp_type="sale", sleep=lambda s: None)

    assert counts["enriched"] == 1
    assert sink.enriched[0][1] == "12345"
    assert sink.enriched[0][2]["clear_height_ft"] == 28


def test_run_enrich_ambiguous_writes_candidates_no_scrape():
    sink = FakeSink([{"id": 2, "address": "amb", "city": "x", "zip_code": "y"}])
    scraped = {"n": 0}

    def resolver(page, address):
        return "ambiguous", None, [Candidate("1", "a", "a", None, 0.5),
                                    Candidate("2", "b", "b", None, 0.5)]

    def scraper(page, costar_url):
        scraped["n"] += 1
        return ""

    counts = run_enrich(sink, page=object(), resolver=resolver, scraper=scraper,
                        extractor=lambda t: {}, comp_type="sale", sleep=lambda s: None)

    assert counts["ambiguous"] == 1
    assert scraped["n"] == 0                # ambiguous never scrapes
    assert sink.statuses[0] == (2, "ambiguous")


def test_run_enrich_dry_run_writes_nothing():
    sink = FakeSink([{"id": 1, "address": "a", "city": "x", "zip_code": "y"}])
    counts = run_enrich(sink, page=object(),
                        resolver=lambda p, a: ("matched", "1", []),
                        scraper=lambda p, u: "t", extractor=lambda t: {"x": 1},
                        comp_type="sale", dry_run=True, sleep=lambda s: None)
    assert sink.enriched == []
    assert counts["enriched"] == 1          # counted, not written
```

- [ ] **Step 2: Run test to verify it fails**

Run: `.venv/bin/pytest tests/test_enrich.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'costar_market.enrich'`

- [ ] **Step 3: Implement orchestration**

Create `costar_market/enrich.py`:

```python
"""On-demand local CoStar enrichment of pending comps."""
from __future__ import annotations

import time
from typing import Callable

from costar_market.cdp_scraper import COSTAR_BASE


def _detail_url(pid: str) -> str:
    return f"{COSTAR_BASE}/detail/lookup/{pid}/summary"


def run_enrich(
    sink,
    page,
    resolver: Callable,
    scraper: Callable,
    extractor: Callable,
    comp_type: str,
    limit: int = 50,
    dry_run: bool = False,
    delay: float = 1.0,
    sleep: Callable = time.sleep,
) -> dict:
    """Resolve + scrape + write each pending comp. Serial + delayed (compliance).

    resolver(page, address) -> (status, pid|None, candidates)
    scraper(page, costar_url) -> summary_text
    extractor(summary_text) -> specs dict
    """
    counts = {"enriched": 0, "ambiguous": 0, "not_found": 0, "error": 0}
    for row in sink.read_pending(comp_type, limit=limit):
        comp_id, address = row["id"], row.get("address") or ""
        try:
            status, pid, candidates = resolver(page, address)
            if status == "not_found":
                counts["not_found"] += 1
                if not dry_run:
                    sink.write_status(comp_type, comp_id, "not_found")
            elif status == "ambiguous":
                counts["ambiguous"] += 1
                if not dry_run:
                    sink.write_status(comp_type, comp_id, "ambiguous",
                                      candidates=[c.__dict__ for c in candidates])
            else:  # matched
                url = _detail_url(pid)
                specs = extractor(scraper(page, url)) if not dry_run else {}
                counts["enriched"] += 1
                if not dry_run:
                    sink.write_enrichment(comp_type, comp_id, pid, url, specs)
        except Exception:
            counts["error"] += 1
            if not dry_run:
                sink.write_status(comp_type, comp_id, "error")
        sleep(delay)
    return counts
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `.venv/bin/pytest tests/test_enrich.py -v`
Expected: PASS

- [ ] **Step 5: Add the `enrich` CLI subcommand**

In `costar_market/cli.py`, add a runner after `_run_scrape` (line 328):

```python
def _run_enrich(args: argparse.Namespace) -> int:
    """Resolve + scrape pending comps from the comp DB via Chrome on :9222."""
    import os
    from costar_market.cdp_scraper import attach_to_chrome, scrape_property, parse_property_id
    from costar_market.comp_sink import CompSink
    from costar_market.lookup import resolve_address
    from costar_market.industrial_specs import extract_industrial_specs

    _load_dotenv_if_present()
    db_url = args.comp_db or os.environ.get("COMP_DB_URL")
    if not db_url:
        print("Error: --comp-db or COMP_DB_URL is required.", file=sys.stderr)
        return 2

    sink = CompSink(db_url)
    browser = attach_to_chrome(args.port)
    page = browser.contexts[0].pages[0]

    def scraper(pg, costar_url):
        from pathlib import Path
        arts = scrape_property(browser, costar_url, Path(args.out_root))
        summ = arts.captures.get("summary")
        if summ and summ.file_path:
            return summ.file_path.read_text(encoding="utf-8")
        return ""

    from costar_market.enrich import run_enrich
    counts = run_enrich(
        sink, page,
        resolver=resolve_address,
        scraper=scraper,
        extractor=extract_industrial_specs,
        comp_type=args.type,
        limit=args.limit,
        dry_run=args.dry_run,
    )
    print(f"Enrich complete: {counts}")
    return 0
```

In `_build_parser` (after the `scrape` subparser block, line 426), add:

```python
    enrich = subparsers.add_parser(
        "enrich",
        help="Resolve + scrape pending comps from the comp DB via Chrome on :9222.",
    )
    enrich.add_argument("--comp-db", default=None, help="Comp tool DB URL. Falls back to COMP_DB_URL.")
    enrich.add_argument("--type", choices=["sale", "lease"], default="sale", help="Which comp table.")
    enrich.add_argument("--limit", type=int, default=50, help="Max pending comps this run.")
    enrich.add_argument("--port", type=int, default=9222, help="Chrome CDP port.")
    enrich.add_argument("--out-root", default="data/raw", help="Raw artifact dir.")
    enrich.add_argument("--dry-run", action="store_true", help="Resolve + count without writing back.")
    enrich.set_defaults(func=_run_enrich)
```

Add `"enrich"` to the known-subcommands set in `main` (line 442).

- [ ] **Step 6: Test the CLI parser wiring**

Append to `tests/test_cli.py`:

```python
def test_enrich_subcommand_parses():
    from costar_market.cli import _build_parser
    args = _build_parser().parse_args(["enrich", "--type", "lease", "--limit", "5", "--dry-run"])
    assert args.type == "lease"
    assert args.limit == 5
    assert args.dry_run is True
```

Run: `.venv/bin/pytest tests/test_cli.py::test_enrich_subcommand_parses tests/test_enrich.py -v`
Expected: PASS

- [ ] **Step 7: Commit**

```bash
git add costar_market/enrich.py costar_market/cli.py tests/test_enrich.py tests/test_cli.py
git commit -m "feat: costar enrich CLI + orchestration (resolve->scrape->extract->writeback)"
```

---

### Task 5: Comp tool DB columns (`Harbor-Capital-Scraper`)

Add the `costar_*` columns to both comp models (default `pending`) and an idempotent migration for
the live Supabase DB.

**Repo:** `Harbor-Capital-Scraper`
**Files:**
- Modify: `database.py:15-35` (SaleComp) and `database.py:37-61` (LeaseComp)
- Create: `scripts/add_costar_columns.py`
- Test: `tests/test_costar_columns.py`

**Interfaces:**
- Produces: `SaleComp`/`LeaseComp` with `costar_property_id`, `costar_url`, `costar_specs`,
  `costar_status` (default `'pending'`), `costar_candidates`, `costar_enriched_at`.

- [ ] **Step 1: Write the failing test**

Create `tests/test_costar_columns.py`:

```python
from sqlalchemy import create_engine, inspect
from sqlalchemy.orm import sessionmaker
import database


def test_sale_comp_has_costar_columns():
    cols = {c.name for c in database.SaleComp.__table__.columns}
    assert {"costar_property_id", "costar_url", "costar_specs",
            "costar_status", "costar_candidates", "costar_enriched_at"} <= cols


def test_new_sale_comp_defaults_to_pending(tmp_path):
    eng = create_engine(f"sqlite:///{tmp_path/'t.db'}")
    database.Base.metadata.create_all(eng)
    Session = sessionmaker(bind=eng)
    with Session() as s:
        row = database.SaleComp(address="1 Main St")
        s.add(row)
        s.commit()
        s.refresh(row)
        assert row.costar_status == "pending"


def test_lease_comp_has_costar_columns():
    cols = {c.name for c in database.LeaseComp.__table__.columns}
    assert "costar_status" in cols and "costar_specs" in cols
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd ~/HarborCapital/Harbor-Capital-Scraper && .venv/bin/pytest tests/test_costar_columns.py -v`
Expected: FAIL (`costar_*` columns absent)

- [ ] **Step 3: Add columns to the models**

In `database.py`, add to `SaleComp` (after line 34, before `created_at`) AND to `LeaseComp`
(after line 60, before `created_at`) — identical block:

```python
    costar_property_id = Column(String)
    costar_url = Column(String)
    costar_specs = Column(Text)            # JSON blob of CoStar-derived specs
    costar_status = Column(String, default='pending', server_default='pending')
    costar_candidates = Column(Text)       # JSON list of candidate matches (when ambiguous)
    costar_enriched_at = Column(DateTime)
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `.venv/bin/pytest tests/test_costar_columns.py -v`
Expected: PASS

- [ ] **Step 5: Idempotent migration for live Supabase**

Create `scripts/add_costar_columns.py`:

```python
"""Idempotent migration: add costar_* columns to sale_comps and lease_comps.

Run against the live DB once: python scripts/add_costar_columns.py
Safe to re-run (ADD COLUMN IF NOT EXISTS).
"""
import os
from sqlalchemy import create_engine, text

DB_URL = os.environ.get("SUPABASE_DB_URL", "sqlite:///comps.db")

_COLS = [
    "costar_property_id VARCHAR",
    "costar_url VARCHAR",
    "costar_specs TEXT",
    "costar_status VARCHAR DEFAULT 'pending'",
    "costar_candidates TEXT",
    "costar_enriched_at TIMESTAMP",
]


def main() -> None:
    eng = create_engine(DB_URL)
    with eng.begin() as c:
        for table in ("sale_comps", "lease_comps"):
            for col in _COLS:
                c.execute(text(f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS {col}"))
    print("costar_* columns ensured on sale_comps and lease_comps.")


if __name__ == "__main__":
    main()
```

- [ ] **Step 6: Commit**

```bash
git add database.py scripts/add_costar_columns.py tests/test_costar_columns.py
git commit -m "feat: add costar_* columns to comp models + idempotent supabase migration"
```

---

### Task 6: Comp tool UI — CoStar badge, deep-link, comparison & candidate-picker (`Harbor-Capital-Scraper`)

Surface enrichment in the Database/Detail views and let an analyst resolve `ambiguous` comps by
picking a candidate (which re-queues that comp for the next local `enrich` run).

**Repo:** `Harbor-Capital-Scraper`
**Files:**
- Create: `web/costar_view.py` (pure helpers — testable without the web stack)
- Modify: `web/routes/database.py` (add the candidate-pick route; follow existing route patterns)
- Modify: the database/detail template (add badge + link + comparison panel + picker form)
- Test: `tests/test_costar_view.py`

**Interfaces:**
- Produces:
  - `costar_badge(status: str) -> dict` → `{"label": str, "css": str}` for template rendering.
  - `comparison_rows(analyst: dict, costar_specs: dict) -> list[dict]` → `[{field, analyst_value, costar_value, agree}]` for overlapping fields.
  - `select_costar_candidate(db_url, comp_type, comp_id, pid) -> None` → set chosen pid + `costar_status='pending'` (re-queues; the cloud app never scrapes).

- [ ] **Step 1: Write the failing test**

Create `tests/test_costar_view.py`:

```python
import json
from sqlalchemy import create_engine, text
from web.costar_view import costar_badge, comparison_rows, select_costar_candidate


def test_costar_badge_maps_status():
    assert costar_badge("enriched")["label"].lower().startswith("costar")
    assert costar_badge("ambiguous")["css"]
    assert costar_badge("not_found")["label"]
    assert costar_badge("pending")["label"]


def test_comparison_rows_flags_agreement():
    rows = comparison_rows(
        analyst={"building_size": 120000, "year_built": 1998},
        costar_specs={"rba_sf": 120500, "year_built": 1998},
    )
    by_field = {r["field"]: r for r in rows}
    assert by_field["year_built"]["agree"] is True
    # building_size vs rba_sf differ slightly -> not agree
    assert by_field["building_size"]["agree"] is False


def test_select_candidate_requeues_pending(tmp_path):
    url = f"sqlite:///{tmp_path/'c.db'}"
    eng = create_engine(url)
    with eng.begin() as c:
        c.execute(text(
            "CREATE TABLE sale_comps (id INTEGER PRIMARY KEY, costar_property_id TEXT, "
            "costar_status TEXT, costar_candidates TEXT)"
        ))
        c.execute(text(
            "INSERT INTO sale_comps (id, costar_status, costar_candidates) "
            "VALUES (1, 'ambiguous', '[]')"
        ))
    select_costar_candidate(url, "sale", 1, "777")
    with eng.connect() as c:
        row = c.execute(text("SELECT costar_property_id, costar_status FROM sale_comps WHERE id=1")).mappings().one()
    assert row["costar_property_id"] == "777"
    assert row["costar_status"] == "pending"   # re-queued for local enrich
```

- [ ] **Step 2: Run test to verify it fails**

Run: `.venv/bin/pytest tests/test_costar_view.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'web.costar_view'`

- [ ] **Step 3: Implement the helpers**

Create `web/costar_view.py`:

```python
"""Pure presentation + action helpers for CoStar enrichment in the comp UI."""
from __future__ import annotations

from sqlalchemy import create_engine, text

_BADGES = {
    "enriched": {"label": "CoStar ✓", "css": "badge-success"},
    "ambiguous": {"label": "CoStar: choose", "css": "badge-warn"},
    "not_found": {"label": "CoStar: none", "css": "badge-muted"},
    "pending": {"label": "CoStar: pending", "css": "badge-pending"},
    "error": {"label": "CoStar: error", "css": "badge-error"},
}

# Overlapping fields: (analyst_column, costar_specs_key)
_OVERLAP = [
    ("building_size", "rba_sf"),
    ("leased_sf", "rba_sf"),
    ("year_built", "year_built"),
    ("clear_height", "clear_height_ft"),
    ("cap_rate", "submkt_cap_rate"),
]

_TOL = 0.02  # 2% tolerance for numeric agreement


def costar_badge(status: str) -> dict:
    return _BADGES.get(status or "pending", _BADGES["pending"])


def _agree(a, b) -> bool:
    if a is None or b is None:
        return False
    try:
        a, b = float(a), float(b)
    except (TypeError, ValueError):
        return str(a).strip().lower() == str(b).strip().lower()
    if a == b:
        return True
    denom = max(abs(a), abs(b)) or 1.0
    return abs(a - b) / denom <= _TOL


def comparison_rows(analyst: dict, costar_specs: dict) -> list[dict]:
    rows = []
    for acol, ckey in _OVERLAP:
        if acol in analyst or ckey in costar_specs:
            av, cv = analyst.get(acol), costar_specs.get(ckey)
            if av is None and cv is None:
                continue
            rows.append({"field": acol, "analyst_value": av,
                         "costar_value": cv, "agree": _agree(av, cv)})
    return rows


def select_costar_candidate(db_url: str, comp_type: str, comp_id: int, pid: str) -> None:
    table = {"sale": "sale_comps", "lease": "lease_comps"}[comp_type]
    eng = create_engine(db_url)
    with eng.begin() as c:
        c.execute(text(
            f"UPDATE {table} SET costar_property_id=:pid, costar_status='pending' WHERE id=:id"
        ), {"pid": pid, "id": comp_id})
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `.venv/bin/pytest tests/test_costar_view.py -v`
Expected: PASS

- [ ] **Step 5: Wire the route + template**

In `web/routes/database.py`, add a POST route (follow the file's existing router + dependency
patterns for `db_url`/auth):

```python
from fastapi import Form
from web.costar_view import select_costar_candidate
from web.config import SUPABASE_DB_URL

@database_router.post("/database/{comp_type}/{comp_id}/costar-candidate")
def pick_costar_candidate(comp_type: str, comp_id: int, pid: str = Form(...)):
    select_costar_candidate(SUPABASE_DB_URL, comp_type, comp_id, pid)
    return RedirectResponse(url="/database", status_code=303)
```

In the database/detail template, render: the `costar_badge(...)` chip; an `<a href="{{ costar_url }}">`
deep-link when `costar_status == 'enriched'`; a comparison table from `comparison_rows(...)`; and,
when `costar_status == 'ambiguous'`, a small `<form method="post">` posting the chosen candidate
`pid` to the route above.

- [ ] **Step 6: Manual visual verification**

Run the app locally (`bash start.sh` or `uvicorn web.main:app --port 8000`), open `/database`,
and confirm: pending/enriched/ambiguous badges render; the deep-link opens the CoStar page;
the comparison panel shows analyst-vs-CoStar; the picker re-queues an ambiguous comp (status → pending).

- [ ] **Step 7: Run suite + commit**

```bash
.venv/bin/pytest tests/test_costar_view.py -v
git add web/costar_view.py web/routes/database.py tests/test_costar_view.py
# plus the modified template file
git commit -m "feat: CoStar badge, deep-link, analyst-vs-CoStar comparison + candidate picker"
```

---

## Self-Review

**Spec coverage:**
- On-demand local CLI (`costar enrich`) → Task 4 ✓
- Address→PID resolver + Phase-0 discovery → Task 0 (spike) + Task 1 (parse/match/live) ✓
- Rich industrial spec field set, never fabricate → Task 2 ✓
- Comp DB adapter, never overwrite analyst columns → Task 3 ✓
- New `costar_*` columns + status='pending' default → Task 5 ✓
- UI badge / deep-link / analyst-vs-CoStar comparison / candidate-picker (re-queues, cloud never scrapes) → Task 6 ✓
- Compliance (CDP serial + delays), idempotent/resumable → Task 4 `run_enrich` (delay/sleep; read_pending only 'pending') ✓
- Error/ambiguous/not_found handling → Task 4 status writes ✓

**Type consistency:** `resolve_address`/`choose_match` return `(status, pid|None, candidates)` consumed identically in Task 4. `Candidate.__dict__` serialized for ambiguous candidates (Task 4) and read by the picker (Task 6). `CompSink` method signatures (Task 3) match the `enrich` CLI seam (Task 4). `extract_industrial_specs(text)` single-arg call in the CLI scraper seam matches Task 2 (llm defaults None).

**Cross-repo note:** Tasks 0–4 are in `CoStar-Market-Extraction`; Tasks 5–6 in `Harbor-Capital-Scraper`. Tasks 5 (columns) and 1/2 (pure logic) have no cross-repo dependency and can proceed in parallel; Task 4's live run depends on Task 0 (endpoint) + Task 5 (columns present in the DB it reads).

**Known live-only gaps (not unit-testable):** `lookup._fetch_search_results` (filled from Task 0); real CoStar `summary.txt` label text for Task 2 regexes (reconciled in Task 0); Task 6 template rendering (manual visual check, Step 6).
