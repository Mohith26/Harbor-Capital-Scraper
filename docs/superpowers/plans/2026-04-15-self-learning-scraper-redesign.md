# Self-Learning Comp Scraper Redesign — Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the stateless `comp_engine.py` column-mapping pipeline with a staged, self-learning engine that fingerprints templates, remembers human corrections, supports PDFs via vision, and gets more accurate as more files are uploaded and reviewed.

**Architecture:** Split current monolithic `comp_engine.py` into typed pipeline stages (`engine/`) with learning injection at stage boundaries backed by a dual JSON-seed + Supabase-live `LearningStore` (`learning/`). See `docs/superpowers/specs/2026-04-15-self-learning-scraper-redesign-design.md` for full design rationale.

**Tech Stack:** Python 3.11, Streamlit, SQLAlchemy 2.x, Supabase Postgres, OpenAI (embeddings + GPT-4o + vision), Google Maps Geocoding API, pandas, scipy, pdf2image, pytest, responses.

**Reference spec:** [`docs/superpowers/specs/2026-04-15-self-learning-scraper-redesign-design.md`](../specs/2026-04-15-self-learning-scraper-redesign-design.md)

**Branch:** `claude/thirsty-wilbur` (worktree at `.claude/worktrees/thirsty-wilbur/`)

**Deployment gate:** After Phase 8 completes, spin up local Streamlit instance for user acceptance testing. **DO NOT push to main or deploy to Railway until user signs off.**

---

## File Structure

### Files Created

```
engine/
  __init__.py                              # package marker, re-exports pipeline.run_pipeline
  types.py                                 # all @dataclass types
  loaders.py                               # xlsx/csv segmentation (ported from comp_engine.py)
  vision_pdf.py                            # GPT-4o vision PDF loader
  fingerprint.py                           # header-set hashing + tiered matching
  mapping.py                               # embedding/Hungarian column mapping (ported + boosted)
  cleaning.py                              # numeric coercion, rate splitting (ported)
  geocoding.py                             # Google wrapper + LLM cleanup + cache lookup
  validation.py                            # row-level warnings
  pipeline.py                              # orchestrator: run_pipeline()
  openai_client.py                         # shared OpenAI client with retry/backoff

learning/
  __init__.py
  protocol.py                              # LearningStore typing.Protocol
  schemas.py                               # SQLAlchemy models for learning tables
  store.py                                 # SupabaseLearningStore + SqliteLearningStore
  fakes.py                                 # FakeLearningStore + EmptyLearningStore for tests
  corrections.py                           # persist_with_learning() + helpers
  fingerprints.py                          # hashing + Jaccard helpers
  geocode_cache.py                         # alias/override lookups
  seed_io.py                               # load/dump JSON seed files

learning_data/                             # committed baseline (created empty; filled in Phase 8)
  seed_fingerprints.json
  seed_corrections.json
  seed_geocode_aliases.json
  seed_brokers.json
  labeled_samples/                         # per-segment pending/approved JSONs

tools/
  seed_from_samples.py                     # one-shot baseline run on sample comp files
  rebuild_learning_from_seed.py            # wipe + reload learning tables from JSON
  rebuild_embedding_fixture.py             # regenerate deterministic embedding cache
  sync_learning.py                         # pull Supabase live → refresh seed JSON (Phase 9)

tests/
  __init__.py
  conftest.py                              # pytest fixtures
  fixtures/
    mini_lease.csv
    mini_sale.xlsx
    mini_broker_jll.xlsx
    mini_broker_jll_q2.xlsx
    mini_pdf.pdf
    mini_fuzzy.xlsx
    embeddings_cache.pkl                   # deterministic embedding fixture for Phase 2
  test_loaders.py
  test_vision_pdf.py
  test_fingerprint.py
  test_mapping.py
  test_cleaning.py
  test_geocoding.py
  test_validation.py
  test_pipeline.py
  test_learning_store.py
  test_corrections.py
  test_accuracy_regression.py              # @pytest.mark.integration gate

docs/superpowers/plans/2026-04-15-self-learning-scraper-redesign.md   # this file
```

### Files Modified

```
comp_engine.py                             # reduced to thin facade (re-exports from engine/*)
app.py                                     # save handler calls persist_with_learning()
database.py                                # import learning schemas so they auto-create
requirements.txt                           # +pdf2image, +pillow, +pytest, +responses, +pytest-mock
Dockerfile                                 # +poppler-utils apt-get line
```

### Responsibility Boundaries

- `engine/` — pure functions, no UI coupling, no DB writes. Each stage is independently testable.
- `learning/` — all persistence of learned state. `corrections.persist_with_learning()` is the ONLY write path; stage code never writes to the store directly (only reads).
- `comp_engine.py` — backward-compatibility facade. `app.py` imports remain valid; internally routes to new engine.
- `tools/` — one-shot scripts. Not imported from the app.
- `tests/` — mirror source structure. One test file per source module.

---

## Chunk 1: Phase 0 — Scaffolding and Dependencies

Zero behavior change. Gets the new directory structure, deps, and empty stubs in place so later phases can compile.

### Task 0.1: Add runtime dependencies

**Files:**
- Modify: `requirements.txt`

- [ ] **Step 1:** Append the following lines to `requirements.txt` (end of file, no blank line before):

```
pdf2image>=1.17.0
pillow>=10.0.0
pytest>=8.0.0
pytest-mock>=3.12.0
responses>=0.25.0
rapidfuzz>=3.0.0
```

- [ ] **Step 2:** Run `pip install -r requirements.txt` in the local venv to verify installs resolve cleanly.

Expected: `Successfully installed pdf2image-... pillow-... pytest-... ...`

- [ ] **Step 3:** Commit.

```bash
git add requirements.txt
git commit -m "deps: add pdf2image, pillow, pytest, responses for self-learning engine"
```

### Task 0.2: Add poppler to Dockerfile

**Files:**
- Modify: `Dockerfile`

- [ ] **Step 1:** Edit `Dockerfile` to install `poppler-utils` before the pip install step:

```dockerfile
FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1

WORKDIR /app
COPY . ./

RUN apt-get update && apt-get install -y --no-install-recommends poppler-utils && rm -rf /var/lib/apt/lists/*
RUN pip install --no-cache-dir -r requirements.txt
RUN sed -i 's/\r$//' start.sh && chmod +x start.sh

EXPOSE 8501
CMD ["./start.sh"]
```

- [ ] **Step 2:** Commit.

```bash
git add Dockerfile
git commit -m "docker: install poppler-utils for PDF vision pipeline"
```

### Task 0.3: Create empty package directories and module stubs

**Files:**
- Create: `engine/__init__.py`
- Create: `engine/types.py`
- Create: `engine/loaders.py`
- Create: `engine/vision_pdf.py`
- Create: `engine/fingerprint.py`
- Create: `engine/mapping.py`
- Create: `engine/cleaning.py`
- Create: `engine/geocoding.py`
- Create: `engine/validation.py`
- Create: `engine/pipeline.py`
- Create: `engine/openai_client.py`
- Create: `learning/__init__.py`
- Create: `learning/protocol.py`
- Create: `learning/schemas.py`
- Create: `learning/store.py`
- Create: `learning/fakes.py`
- Create: `learning/corrections.py`
- Create: `learning/fingerprints.py`
- Create: `learning/geocode_cache.py`
- Create: `learning/seed_io.py`
- Create: `learning_data/.gitkeep`
- Create: `learning_data/labeled_samples/.gitkeep`
- Create: `tools/.gitkeep`
- Create: `tests/__init__.py`
- Create: `tests/conftest.py`
- Create: `tests/fixtures/.gitkeep`

- [ ] **Step 1:** Create each file. Every `.py` stub contains only a module docstring:

```python
"""<module-path> — stub, see plan chunk <N>."""
```

`__init__.py` files are empty.

- [ ] **Step 2:** Run `python -c "import engine, learning"` to verify packages import.

Expected: silent exit (no errors).

- [ ] **Step 3:** Run `pytest --collect-only` to verify pytest discovers the empty `tests/` package.

Expected: `0 tests collected` with no errors.

- [ ] **Step 4:** Commit.

```bash
git add engine/ learning/ learning_data/ tools/ tests/
git commit -m "scaffold: create engine/, learning/, tests/, tools/ package skeletons"
```

### Task 0.4: Add dataclass type definitions

**Files:**
- Modify: `engine/types.py`

- [ ] **Step 1:** Write the file contents:

```python
"""Pipeline stage data contracts — typed dataclasses used by all engine stages."""
from __future__ import annotations
from dataclasses import dataclass, field
from typing import Optional
import pandas as pd


@dataclass
class LoadedSegment:
    df: pd.DataFrame
    source_path: str
    sheet_name: Optional[str]
    segment_title: Optional[str]
    segment_index: int
    loader: str  # "xlsx" | "csv" | "pdf_vision"
    raw_headers: list[str]


@dataclass
class Fingerprint:
    """Content-addressable identifier for a (file_type, header-layout) pair.

    - raw_hash: order-sensitive hash, used for Tier 1 exact match.
    - header_set_hash: order-agnostic hash for debugging / inspection.
    - headers: original raw headers as strings, preserved for UI display.
    - normalized_headers: clean_header()-processed form, used for Jaccard.
    """
    raw_hash: str
    header_set_hash: str
    headers: list[str]
    normalized_headers: list[str]
    file_type: str  # "lease" | "sale"
    filename: str
    sheet_name: Optional[str]


@dataclass
class FingerprintMatch:
    """Result of a tiered lookup against a LearningStore."""
    source: str  # "exact" | "fuzzy" | "broker"
    similarity: float  # 1.0 for exact, Jaccard score otherwise
    fingerprint: Fingerprint
    mappings: dict[str, str]
    confidence: float
    hit_count: int


@dataclass
class MappingResult:
    """Output of the mapping stage for one segment."""
    fingerprint: Fingerprint
    mappings: dict[str, str]  # raw_header -> target_column
    confidence: dict[str, float]  # raw_header -> similarity score
    source: str  # "exact" | "fuzzy" | "broker" | "embedding" | "embedding+corrections" | "vision_pdf"
    similarity: float  # Jaccard score when source in {fuzzy, broker}, else 0
    cleaned_df: pd.DataFrame


@dataclass
class CleanedRows:
    df: pd.DataFrame
    rate_basis: Optional[str]
    warnings: list[str] = field(default_factory=list)


@dataclass
class GeocodedRows:
    df: pd.DataFrame
    geocode_sources: list[str]
    warnings: list[str] = field(default_factory=list)


@dataclass
class SegmentResult:
    """One processed segment. segment_key format: '<sheet_name_or_root>::<segment_index>'."""
    segment_key: str
    fingerprint: Fingerprint
    mapping_result: MappingResult
    cleaned_df: pd.DataFrame


@dataclass
class PipelineResult:
    segments: list[SegmentResult]
    combined_df: pd.DataFrame
    confidence_by_segment: dict[str, dict[str, float]]
    mappings_by_segment: dict[str, dict[str, str]]
    warnings: list[str] = field(default_factory=list)
```

- [ ] **Step 2:** Verify import.

```bash
python -c "from engine.types import LoadedSegment, Fingerprint, MappingResult, PipelineResult; print('ok')"
```

Expected: `ok`

- [ ] **Step 3:** Commit.

```bash
git add engine/types.py
git commit -m "engine: add typed dataclass stage contracts"
```

### Task 0.5: Configure pytest

**Files:**
- Create: `pytest.ini`
- Modify: `tests/conftest.py`

- [ ] **Step 1:** Create `pytest.ini` in repo root:

```ini
[pytest]
testpaths = tests
python_files = test_*.py
python_classes = Test*
python_functions = test_*
markers =
    integration: marks tests that hit real external APIs (deselect with -m "not integration")
filterwarnings =
    ignore::DeprecationWarning
```

- [ ] **Step 2:** Populate `tests/conftest.py` with shared fixtures:

```python
"""Shared pytest fixtures for the self-learning engine tests."""
import os
import pytest
from pathlib import Path


FIXTURES_DIR = Path(__file__).parent / "fixtures"


@pytest.fixture
def fixtures_dir():
    return FIXTURES_DIR


@pytest.fixture
def sample_comp_files_dir():
    """Path to the 16 real sample files used for Phase 8 seeding + regression."""
    return Path(__file__).parent.parent / "sample comp files"
```

- [ ] **Step 3:** Run `pytest -v` — should report 0 tests collected, no errors.

- [ ] **Step 4:** Commit.

```bash
git add pytest.ini tests/conftest.py
git commit -m "test: configure pytest with integration marker"
```

---

## Chunk 2: Phase 1 — Learning Store Backend

Builds `learning/` module: Protocol, SQLAlchemy models, concrete Supabase/Sqlite stores, fakes, seed I/O. All tests use `FakeLearningStore` — no DB required.

Zero user impact — store is built but nothing queries it yet.

> **⚠️ REVIEWER FIXES APPLIED — IMPORTANT FOR IMPLEMENTER:**
>
> The Protocol in Task 1.1 below is the source of truth. It was corrected during review to match the kwargs used by Chunks 3-9. Some code samples in Tasks 1.4 (FakeLearningStore) and 1.6 (SqliteLearningStore) were written before this correction and still use older parameter names. When you implement them, **match the Protocol signatures exactly**. Specifically, translate as you copy:
>
> | Old (in sample code below) | New (match this) |
> |----------------------------|------------------|
> | `record_accepted_mapping(fp, final_mapping, user)` | `record_accepted_mapping(fingerprint, mappings, confirmed_by, broker_id=None)` |
> | `find_fuzzy_fingerprints(file_type, header_set, min_jaccard)` → `list[tuple]` | `find_fuzzy_fingerprints(file_type)` → `list[dict]` (caller computes Jaccard) |
> | `find_broker_fingerprints(broker, header_set, min_jaccard)` | `find_broker_fingerprints(broker_name, file_type)` |
> | `get_corrections_for_context(file_type, broker)` → `list[dict]` | `get_corrections_for_context(file_type, raw_header)` → `dict[str, int]` |
> | `upsert_correction(clean_header, target, file_type, broker, user)` | `upsert_correction(file_type, raw_header, target_column, confirmed_by)` |
> | `insert_geocode_alias(key, raw_text, answer, source)` | `insert_geocode_alias(raw_text, canonical_address, lat, lng)` |
> | `record_geocode_override(raw_text, corrected_addr, lat, lng, city, zip_code, user)` | `record_geocode_override(raw_text, override_address, lat, lng, confirmed_by)` |
> | `upsert_broker(canonical_name, user, alias=None)` → `None` | `upsert_broker(name, confirmed_by)` → `int` (broker_id) |
> | `record_broker_correction(fingerprint_hash, llm_guess, confirmed, user)` | `record_broker_correction(alias, canonical_name, confirmed_by)` |
> | `fp.hash` / `fp.clean_headers` / `fp.broker` | `fp.raw_hash` / `fp.normalized_headers` / (broker is a separate kwarg) |
>
> Also: the SQLAlchemy models in Task 1.2 use `fingerprint_hash` as the column name — rename the column to `raw_hash` (matching Fingerprint.raw_hash) and keep a SQLAlchemy index on it. All queries that filter by `TemplateFingerprint.fingerprint_hash` become `.raw_hash`.
>
> `Task 1.6 record_broker_correction` in the earlier draft was a `pass` stub. Implement it properly: find-or-create the canonical broker, append the alias to its aliases JSON column, commit.
>
> The `ColumnMappingCorrection` table's `broker` column should be dropped entirely — the corrected Protocol does not pass broker into `upsert_correction`. Unique constraint becomes `(file_type, raw_header, target_column)` — this is NOT NULL safe and SQLite/Postgres handle it identically.
>
> Use atomic upserts via `sqlalchemy.dialects.sqlite.insert(...).on_conflict_do_update(...)` (and the `postgresql` variant for production). A SELECT-then-INSERT is NOT safe under concurrent Streamlit sessions.

### Task 1.1: Define LearningStore Protocol

**Files:**
- Modify: `learning/protocol.py`
- Create: `tests/test_learning_protocol.py`

- [ ] **Step 1:** Write the Protocol:

```python
"""LearningStore protocol — structural type all implementations must conform to.

All concrete stores (Supabase, SQLite, Fake, Empty) must implement every method
with EXACTLY the signatures below. Downstream engine code (Chunks 3-9) calls
these methods with these exact keyword arguments — do not deviate.
"""
from __future__ import annotations
from typing import Protocol, Optional
from engine.types import Fingerprint


class LearningStore(Protocol):
    # ---- Fingerprints / templates ----
    def get_fingerprint_by_hash(self, fp_hash: str) -> Optional[dict]:
        """Return {"mappings": dict, "confidence": float, "hit_count": int,
        "normalized_headers": list[str], "broker_id": Optional[int]} or None."""
        ...

    def find_fuzzy_fingerprints(self, file_type: str) -> list[dict]:
        """Return all stored fingerprint records for a given file_type. Caller
        computes Jaccard against their own target set. Each dict has
        normalized_headers, mappings, confidence, hit_count."""
        ...

    def find_broker_fingerprints(self, broker_name: str, file_type: str) -> list[dict]:
        """Return fingerprint records for a given broker_name + file_type.
        Same shape as find_fuzzy_fingerprints."""
        ...

    def record_accepted_mapping(
        self,
        fingerprint: Fingerprint,
        mappings: dict[str, str],
        confirmed_by: str,
        broker_id: Optional[int] = None,
    ) -> None:
        """Upsert the fingerprint. Atomic: INSERT ... ON CONFLICT (raw_hash)
        DO UPDATE SET mappings = ..., hit_count = hit_count + 1."""
        ...

    # ---- Correction votes ----
    def get_corrections_for_context(
        self, file_type: str, raw_header: str
    ) -> dict[str, int]:
        """Return {target_column: hit_count} for all corrections recorded
        against (file_type, normalized raw_header). Empty dict when none."""
        ...

    def upsert_correction(
        self,
        file_type: str,
        raw_header: str,
        target_column: str,
        confirmed_by: str,
    ) -> None:
        """Atomic upsert: increment hit_count on (file_type, raw_header, target_column)
        composite key. raw_header is already clean_header()-normalized by caller."""
        ...

    # ---- Geocoding ----
    def get_geocode_override(self, raw_text: str) -> Optional[dict]:
        """Return {"formatted_address": str, "latitude": float, "longitude": float}
        or None. Matches on normalized raw_text (lowercased, stripped, ', TX' bias applied)."""
        ...

    def get_geocode_alias(self, raw_text: str) -> Optional[dict]:
        """Return cached geocode result or None. Same normalization as override."""
        ...

    def insert_geocode_alias(
        self,
        raw_text: str,
        canonical_address: str,
        lat: float,
        lng: float,
    ) -> None:
        """Atomic upsert: on conflict (raw_text) update canonical/lat/lng and bump hit_count."""
        ...

    def bump_hit_count(self, raw_text: str) -> None:
        """Increment hit counter on an existing alias row."""
        ...

    def record_geocode_override(
        self,
        raw_text: str,
        override_address: str,
        lat: float,
        lng: float,
        confirmed_by: str,
    ) -> None:
        """Write a user-confirmed override that shadows the alias cache forever."""
        ...

    # ---- Brokers ----
    def upsert_broker(
        self, name: str, confirmed_by: str
    ) -> int:
        """Return broker_id. Atomic: insert if name is new, else return existing id."""
        ...

    def find_broker_by_alias(self, name: str) -> Optional[dict]:
        """Return {"id": int, "canonical_name": str, "aliases": list[str]} or None.
        Matches case-insensitively on canonical name OR any stored alias."""
        ...

    def record_broker_correction(
        self, alias: str, canonical_name: str, confirmed_by: str
    ) -> None:
        """Record that `alias` should resolve to `canonical_name`. Creates the
        canonical broker if missing, then appends `alias` to its alias list."""
        ...

    # ---- PDF corrections ----
    def get_pdf_corrections(self, pdf_hash: str) -> list[dict]:
        """Return list of {page_num, row_index, field, corrected_value} for a PDF content hash."""
        ...

    def record_pdf_correction(
        self,
        pdf_hash: str,
        page_num: int,
        row_index: int,
        field: str,
        original: str,
        corrected: str,
        confirmed_by: str,
    ) -> None: ...

    # ---- Seed bootstrap ----
    def load_seed(self, seed_dir: str) -> None:
        """Load JSON seed files from seed_dir into the store. Idempotent."""
        ...
```

- [ ] **Step 2:** Write a minimal import test at `tests/test_learning_protocol.py`:

```python
"""Protocol import sanity check."""
from learning.protocol import LearningStore


def test_protocol_imports():
    assert LearningStore is not None
```

- [ ] **Step 3:** Run `pytest tests/test_learning_protocol.py -v`

Expected: 1 passed.

- [ ] **Step 4:** Commit.

```bash
git add learning/protocol.py tests/test_learning_protocol.py
git commit -m "learning: define LearningStore typing.Protocol"
```

### Task 1.2: Add SQLAlchemy models for learning tables

**Files:**
- Modify: `learning/schemas.py`

- [ ] **Step 1:** Write the models:

```python
"""SQLAlchemy models for the learning store tables."""
from __future__ import annotations
from sqlalchemy import Column, Integer, String, Float, Text, Boolean, DateTime, func, UniqueConstraint, Index
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.types import JSON as GenericJSON
from sqlalchemy.orm import declarative_base
import os

LearningBase = declarative_base()


def _json_column():
    """Use JSONB on Postgres, generic JSON on SQLite."""
    db_url = os.environ.get("SUPABASE_DB_URL", "")
    return JSONB if db_url.startswith("postgresql") else GenericJSON


_JSON = _json_column()


class TemplateFingerprint(LearningBase):
    __tablename__ = "template_fingerprints"
    id = Column(Integer, primary_key=True)
    fingerprint_hash = Column(String, unique=True, nullable=False, index=True)
    broker = Column(String, nullable=True, index=True)
    file_type = Column(String, nullable=False, index=True)
    header_set = Column(_JSON, nullable=False)
    mapping_blueprint = Column(_JSON, nullable=False)
    rate_unit_hint = Column(String, nullable=True)
    sample_count = Column(Integer, nullable=False, default=1)
    last_seen_at = Column(DateTime, server_default=func.now(), onupdate=func.now())
    confidence = Column(Float, nullable=False, default=0.5)
    created_by = Column(String, nullable=False, default="seed")


class ColumnMappingCorrection(LearningBase):
    __tablename__ = "column_mapping_corrections"
    id = Column(Integer, primary_key=True)
    clean_header = Column(String, nullable=False)
    target_field = Column(String, nullable=False)
    file_type = Column(String, nullable=False)
    broker = Column(String, nullable=True)
    vote_count = Column(Integer, nullable=False, default=1)
    last_confirmed = Column(DateTime, server_default=func.now(), onupdate=func.now())

    __table_args__ = (
        UniqueConstraint(
            "clean_header", "target_field", "file_type", "broker",
            name="uq_correction_context",
        ),
        Index("ix_correction_lookup", "file_type", "broker"),
    )


class GeocodeAlias(LearningBase):
    __tablename__ = "geocode_aliases"
    raw_text_hash = Column(String, primary_key=True)
    raw_text = Column(Text, nullable=False)
    formatted_addr = Column(Text, nullable=False)
    latitude = Column(Float, nullable=True)
    longitude = Column(Float, nullable=True)
    city = Column(String, nullable=True)
    zip_code = Column(String, nullable=True)
    source = Column(String, nullable=False)  # "google" | "llm_cleanup_then_google"
    created_at = Column(DateTime, server_default=func.now())
    hit_count = Column(Integer, nullable=False, default=1)


class GeocodeOverride(LearningBase):
    __tablename__ = "geocode_overrides"
    raw_text_hash = Column(String, primary_key=True)
    raw_text = Column(Text, nullable=False)
    corrected_addr = Column(Text, nullable=False)
    latitude = Column(Float, nullable=True)
    longitude = Column(Float, nullable=True)
    city = Column(String, nullable=True)
    zip_code = Column(String, nullable=True)
    corrected_by = Column(String, nullable=False)
    corrected_at = Column(DateTime, server_default=func.now(), onupdate=func.now())


class Broker(LearningBase):
    __tablename__ = "brokers"
    id = Column(Integer, primary_key=True)
    canonical_name = Column(String, unique=True, nullable=False, index=True)
    aliases = Column(_JSON, nullable=False, default=list)
    first_seen_at = Column(DateTime, server_default=func.now())
    upload_count = Column(Integer, nullable=False, default=1)
    confidence = Column(Float, nullable=False, default=0.5)
    is_brokerage = Column(Boolean, nullable=False, default=True)
    created_by = Column(String, nullable=False, default="llm_auto")


class PdfExtractionCorrection(LearningBase):
    __tablename__ = "pdf_extraction_corrections"
    id = Column(Integer, primary_key=True)
    pdf_hash = Column(String, nullable=False, index=True)
    page_num = Column(Integer, nullable=False)
    row_index = Column(Integer, nullable=False)
    field = Column(String, nullable=False)
    original_value = Column(Text, nullable=True)
    corrected_value = Column(Text, nullable=True)
    corrected_by = Column(String, nullable=False)
    corrected_at = Column(DateTime, server_default=func.now())
```

- [ ] **Step 2:** Verify import.

```bash
python -c "from learning.schemas import LearningBase, TemplateFingerprint, Broker; print('ok')"
```

Expected: `ok`

- [ ] **Step 3:** Commit.

```bash
git add learning/schemas.py
git commit -m "learning: add SQLAlchemy models for learning tables"
```

### Task 1.3: Hook learning table creation into database.py

**Files:**
- Modify: `database.py`

- [ ] **Step 1:** Edit `database.py` to import `LearningBase` and create its tables alongside the existing ones. Add after the existing `Base = declarative_base()`:

```python
# Import after Base is defined — learning/schemas.py uses its own LearningBase
from learning.schemas import LearningBase  # noqa: E402
```

And in the `ensure_tables()` function, add `LearningBase.metadata.create_all(engine)` right after `Base.metadata.create_all(engine)`:

```python
def ensure_tables():
    global _tables_created
    if not _tables_created:
        try:
            Base.metadata.create_all(engine)
            LearningBase.metadata.create_all(engine)
            _tables_created = True
        except Exception as e:
            print(f"Warning: Could not create tables: {e}")
```

- [ ] **Step 2:** Verify it still imports without connecting to a real DB.

```bash
SUPABASE_DB_URL="sqlite:///:memory:" python -c "import database; database.ensure_tables(); print('ok')"
```

Expected: `ok`

- [ ] **Step 3:** Commit.

```bash
git add database.py
git commit -m "db: auto-create learning tables alongside comp tables"
```

### Task 1.4: Implement FakeLearningStore + EmptyLearningStore

**Files:**
- Modify: `learning/fakes.py`
- Create: `tests/test_fakes.py`

- [ ] **Step 1:** Write `tests/test_fakes.py` first (TDD):

```python
"""FakeLearningStore behavior tests."""
import pytest
from learning.fakes import FakeLearningStore, EmptyLearningStore
from engine.types import Fingerprint


@pytest.fixture
def fake():
    return FakeLearningStore()


def test_fake_get_fingerprint_returns_none_initially(fake):
    assert fake.get_fingerprint_by_hash("nope") is None


def test_fake_record_accepted_mapping_creates_fingerprint(fake):
    fp = Fingerprint(hash="abc", broker="JLL", file_type="SALE", clean_headers=["a", "b"])
    fake.record_accepted_mapping(fp, {"address": "Property", "sale_price": "Price"}, user="t@x.com")
    got = fake.get_fingerprint_by_hash("abc")
    assert got is not None
    assert got["mapping_blueprint"] == {"address": "Property", "sale_price": "Price"}
    assert got["sample_count"] == 1


def test_fake_record_accepted_mapping_increments_on_duplicate(fake):
    fp = Fingerprint(hash="abc", broker="JLL", file_type="SALE", clean_headers=["a"])
    fake.record_accepted_mapping(fp, {"address": "Property"}, user="t@x.com")
    fake.record_accepted_mapping(fp, {"address": "Property"}, user="t@x.com")
    got = fake.get_fingerprint_by_hash("abc")
    assert got["sample_count"] == 2
    assert got["confidence"] == pytest.approx(0.55, abs=1e-6)  # 0.5 + 0.05


def test_fake_upsert_correction_increments_vote(fake):
    fake.upsert_correction("sizesf", "leased_sf", "LEASE", "JLL", "u@x.com")
    fake.upsert_correction("sizesf", "leased_sf", "LEASE", "JLL", "u@x.com")
    corrections = fake.get_corrections_for_context("LEASE", "JLL")
    assert len(corrections) == 1
    assert corrections[0]["vote_count"] == 2


def test_fake_geocode_override_beats_alias(fake):
    fake.insert_geocode_alias(
        "k1", "raw", {"formatted_addr": "cached", "latitude": 29.0, "longitude": -95.0, "city": "Houston", "zip_code": "77001"}, source="google"
    )
    fake.record_geocode_override(
        raw_text="raw", corrected_addr="fixed", lat=29.1, lng=-95.1, city="Houston", zip_code="77002", user="u@x.com"
    )
    # The test doesn't assert lookup order here — lookup order is the caller's job.
    # It only verifies both records exist.
    assert fake.get_geocode_alias("k1") is not None
    assert fake.get_geocode_override(fake._hash_raw("raw")) is not None


def test_fake_upsert_broker_merges_aliases(fake):
    fake.upsert_broker("JLL", user="u@x.com", alias="Jones Lang LaSalle")
    fake.upsert_broker("JLL", user="u@x.com", alias="jll capital markets")
    b = fake.find_broker_by_alias("jones lang lasalle")
    assert b is not None
    assert b["canonical_name"] == "JLL"
    assert "jll capital markets" in b["aliases"]
    assert b["upload_count"] == 2


def test_fuzzy_fingerprint_returns_best_by_jaccard(fake):
    fp1 = Fingerprint(hash="h1", broker=None, file_type="SALE", clean_headers=["a", "b", "c", "d"])
    fp2 = Fingerprint(hash="h2", broker=None, file_type="SALE", clean_headers=["a", "b", "x", "y"])
    fake.record_accepted_mapping(fp1, {"address": "a"}, user="u")
    fake.record_accepted_mapping(fp2, {"address": "a"}, user="u")
    hits = fake.find_fuzzy_fingerprints("SALE", ["a", "b", "c", "d"], min_jaccard=0.5)
    assert len(hits) >= 1
    # Exact match should score 1.0
    best = max(hits, key=lambda t: t[1])
    assert best[1] == pytest.approx(1.0, abs=1e-6)
    assert best[0]["fingerprint_hash"] == "h1"


def test_empty_store_always_returns_none(fake):
    empty = EmptyLearningStore()
    fp = Fingerprint(hash="x", broker=None, file_type="SALE", clean_headers=["a"])
    empty.record_accepted_mapping(fp, {"a": "b"}, user="u")  # no-op
    assert empty.get_fingerprint_by_hash("x") is None
    assert empty.get_geocode_alias("k") is None
    assert empty.get_corrections_for_context("SALE", None) == []
```

- [ ] **Step 2:** Run the tests — expect all to FAIL (no implementation yet).

```bash
pytest tests/test_fakes.py -v
```

Expected: FAIL (module not implemented).

- [ ] **Step 3:** Write `learning/fakes.py`:

```python
"""In-memory implementations of LearningStore for testing and baseline runs."""
from __future__ import annotations
import hashlib
import re
from datetime import datetime
from typing import Optional
from engine.types import Fingerprint


def _hash_raw(text: str) -> str:
    normalized = re.sub(r"\s+", " ", text.lower().strip())
    return hashlib.sha256(normalized.encode()).hexdigest()


def _jaccard(a: list[str], b: list[str]) -> float:
    sa, sb = set(a), set(b)
    if not sa and not sb:
        return 1.0
    if not sa or not sb:
        return 0.0
    return len(sa & sb) / len(sa | sb)


class FakeLearningStore:
    """Dict-backed in-memory store. Conforms to LearningStore Protocol."""

    def __init__(self):
        self._fingerprints: dict[str, dict] = {}
        self._corrections: dict[tuple, dict] = {}
        self._geocode_aliases: dict[str, dict] = {}
        self._geocode_overrides: dict[str, dict] = {}
        self._brokers: dict[str, dict] = {}
        self._broker_corrections: list[dict] = []
        self._pdf_corrections: list[dict] = []

    _hash_raw = staticmethod(_hash_raw)

    # ---- Fingerprints ----
    def get_fingerprint_by_hash(self, fp_hash: str) -> Optional[dict]:
        return self._fingerprints.get(fp_hash)

    def find_fuzzy_fingerprints(self, file_type, header_set, min_jaccard):
        hits = []
        for fp in self._fingerprints.values():
            if fp["file_type"] != file_type:
                continue
            score = _jaccard(header_set, fp["header_set"])
            if score >= min_jaccard:
                hits.append((fp, score))
        return sorted(hits, key=lambda t: t[1], reverse=True)

    def find_broker_fingerprints(self, broker, header_set, min_jaccard):
        hits = []
        for fp in self._fingerprints.values():
            if fp.get("broker") != broker:
                continue
            score = _jaccard(header_set, fp["header_set"])
            if score >= min_jaccard:
                hits.append((fp, score))
        return sorted(hits, key=lambda t: t[1], reverse=True)

    def record_accepted_mapping(self, fp: Fingerprint, final_mapping, user):
        existing = self._fingerprints.get(fp.hash)
        if existing:
            existing["sample_count"] += 1
            existing["confidence"] = min(1.0, existing["confidence"] + 0.05)
            existing["last_seen_at"] = datetime.utcnow()
            existing["mapping_blueprint"] = {**existing["mapping_blueprint"], **final_mapping}
        else:
            self._fingerprints[fp.hash] = {
                "fingerprint_hash": fp.hash,
                "broker": fp.broker,
                "file_type": fp.file_type,
                "header_set": list(fp.clean_headers),
                "mapping_blueprint": dict(final_mapping),
                "sample_count": 1,
                "confidence": 0.5,
                "last_seen_at": datetime.utcnow(),
                "created_by": user,
            }
        # Also record per-header votes
        for target, src_hdr in final_mapping.items():
            clean = src_hdr.strip().lower()
            self.upsert_correction(clean, target, fp.file_type, fp.broker, user)

    # ---- Corrections ----
    def get_corrections_for_context(self, file_type, broker):
        return [
            c for c in self._corrections.values()
            if c["file_type"] == file_type and c["broker"] == broker
        ]

    def upsert_correction(self, clean_header, target, file_type, broker, user):
        key = (clean_header, target, file_type, broker)
        existing = self._corrections.get(key)
        if existing:
            existing["vote_count"] += 1
            existing["last_confirmed"] = datetime.utcnow()
        else:
            self._corrections[key] = {
                "clean_header": clean_header,
                "target_field": target,
                "file_type": file_type,
                "broker": broker,
                "vote_count": 1,
                "last_confirmed": datetime.utcnow(),
            }

    # ---- Geocoding ----
    def get_geocode_override(self, key):
        return self._geocode_overrides.get(key)

    def get_geocode_alias(self, key):
        return self._geocode_aliases.get(key)

    def insert_geocode_alias(self, key, raw_text, answer, source):
        self._geocode_aliases[key] = {
            "raw_text_hash": key,
            "raw_text": raw_text,
            "formatted_addr": answer.get("formatted_addr"),
            "latitude": answer.get("latitude"),
            "longitude": answer.get("longitude"),
            "city": answer.get("city"),
            "zip_code": answer.get("zip_code"),
            "source": source,
            "hit_count": 1,
        }

    def bump_hit_count(self, key):
        if key in self._geocode_aliases:
            self._geocode_aliases[key]["hit_count"] += 1

    def record_geocode_override(self, raw_text, corrected_addr, lat, lng, city, zip_code, user):
        key = _hash_raw(raw_text)
        self._geocode_overrides[key] = {
            "raw_text_hash": key,
            "raw_text": raw_text,
            "corrected_addr": corrected_addr,
            "latitude": lat,
            "longitude": lng,
            "city": city,
            "zip_code": zip_code,
            "corrected_by": user,
        }

    # ---- Brokers ----
    def upsert_broker(self, canonical_name, user, alias=None):
        existing = self._brokers.get(canonical_name)
        if existing:
            existing["upload_count"] += 1
            if alias and alias.lower() not in [a.lower() for a in existing["aliases"]]:
                existing["aliases"].append(alias.lower())
        else:
            self._brokers[canonical_name] = {
                "canonical_name": canonical_name,
                "aliases": [alias.lower()] if alias else [],
                "upload_count": 1,
                "confidence": 0.5,
                "is_brokerage": canonical_name != "INTERNAL",
                "created_by": user,
            }

    def find_broker_by_alias(self, candidate):
        c = candidate.lower()
        for b in self._brokers.values():
            if c == b["canonical_name"].lower() or c in b["aliases"]:
                return b
        return None

    def record_broker_correction(self, fingerprint_hash, llm_guess, confirmed, user):
        self._broker_corrections.append({
            "fingerprint_hash": fingerprint_hash,
            "llm_guess": llm_guess,
            "confirmed": confirmed,
            "corrected_by": user,
        })

    # ---- PDF corrections ----
    def get_pdf_corrections(self, pdf_hash):
        return [c for c in self._pdf_corrections if c["pdf_hash"] == pdf_hash]

    def record_pdf_correction(self, pdf_hash, page_num, row_index, field, original, corrected, user):
        self._pdf_corrections.append({
            "pdf_hash": pdf_hash,
            "page_num": page_num,
            "row_index": row_index,
            "field": field,
            "original_value": original,
            "corrected_value": corrected,
            "corrected_by": user,
        })

    # ---- Seed ----
    def load_seed(self, seed_dir: str) -> None:
        # FakeLearningStore doesn't load seed files (tests provide data directly)
        pass


class EmptyLearningStore:
    """All reads return None/empty, all writes are no-ops. Used for baseline pipeline runs."""

    def get_fingerprint_by_hash(self, fp_hash): return None
    def find_fuzzy_fingerprints(self, file_type, header_set, min_jaccard): return []
    def find_broker_fingerprints(self, broker, header_set, min_jaccard): return []
    def record_accepted_mapping(self, fp, final_mapping, user): pass
    def get_corrections_for_context(self, file_type, broker): return []
    def upsert_correction(self, *args, **kwargs): pass
    def get_geocode_override(self, key): return None
    def get_geocode_alias(self, key): return None
    def insert_geocode_alias(self, *args, **kwargs): pass
    def bump_hit_count(self, key): pass
    def record_geocode_override(self, *args, **kwargs): pass
    def upsert_broker(self, *args, **kwargs): pass
    def find_broker_by_alias(self, candidate): return None
    def record_broker_correction(self, *args, **kwargs): pass
    def get_pdf_corrections(self, pdf_hash): return []
    def record_pdf_correction(self, *args, **kwargs): pass
    def load_seed(self, seed_dir): pass
```

- [ ] **Step 4:** Run the tests.

```bash
pytest tests/test_fakes.py -v
```

Expected: 8 passed.

- [ ] **Step 5:** Commit.

```bash
git add learning/fakes.py tests/test_fakes.py
git commit -m "learning: add FakeLearningStore and EmptyLearningStore with unit tests"
```

### Task 1.5: Implement seed I/O (load/dump JSON files)

**Files:**
- Modify: `learning/seed_io.py`
- Create: `tests/test_seed_io.py`

- [ ] **Step 1:** Write `tests/test_seed_io.py`:

```python
"""Seed JSON serialization round-trip tests."""
import json
import tempfile
from pathlib import Path
import pytest
from learning.fakes import FakeLearningStore
from learning.seed_io import dump_store_to_seed, load_seed_into_store
from engine.types import Fingerprint


def test_roundtrip_fingerprint():
    src = FakeLearningStore()
    fp = Fingerprint(hash="h1", broker="JLL", file_type="SALE", clean_headers=["a", "b"])
    src.record_accepted_mapping(fp, {"address": "Property", "sale_price": "Price"}, user="seed")

    with tempfile.TemporaryDirectory() as tmp:
        dump_store_to_seed(src, tmp)
        # Verify the JSON file exists and is valid
        fp_file = Path(tmp) / "seed_fingerprints.json"
        assert fp_file.exists()
        data = json.loads(fp_file.read_text())
        assert isinstance(data, list)
        assert len(data) == 1

        # Load into fresh store
        dst = FakeLearningStore()
        load_seed_into_store(dst, tmp)
        got = dst.get_fingerprint_by_hash("h1")
        assert got is not None
        assert got["mapping_blueprint"]["address"] == "Property"


def test_load_seed_handles_missing_dir():
    dst = FakeLearningStore()
    # Should not raise; silently does nothing
    load_seed_into_store(dst, "/nonexistent/path")
    assert dst.get_fingerprint_by_hash("anything") is None
```

- [ ] **Step 2:** Run — expect FAIL.

```bash
pytest tests/test_seed_io.py -v
```

- [ ] **Step 3:** Write `learning/seed_io.py`:

```python
"""JSON serialization for seeding and persisting the learning store baseline."""
from __future__ import annotations
import json
from datetime import datetime
from pathlib import Path
from typing import Any
from engine.types import Fingerprint


def _to_jsonable(obj: Any) -> Any:
    if isinstance(obj, datetime):
        return obj.isoformat()
    if isinstance(obj, dict):
        return {k: _to_jsonable(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_to_jsonable(x) for x in obj]
    return obj


def dump_store_to_seed(store, seed_dir: str) -> None:
    """Write the store's current state to JSON files in seed_dir.

    Only works on FakeLearningStore (uses internal dicts). Used by
    tools/rebuild_learning_from_seed.py to write aggregated seeds.
    """
    path = Path(seed_dir)
    path.mkdir(parents=True, exist_ok=True)

    fingerprints = list(getattr(store, "_fingerprints", {}).values())
    corrections = list(getattr(store, "_corrections", {}).values())
    aliases = list(getattr(store, "_geocode_aliases", {}).values())
    overrides = list(getattr(store, "_geocode_overrides", {}).values())
    brokers = list(getattr(store, "_brokers", {}).values())

    (path / "seed_fingerprints.json").write_text(json.dumps(_to_jsonable(fingerprints), indent=2))
    (path / "seed_corrections.json").write_text(json.dumps(_to_jsonable(corrections), indent=2))
    (path / "seed_geocode_aliases.json").write_text(json.dumps(_to_jsonable(aliases), indent=2))
    (path / "seed_geocode_overrides.json").write_text(json.dumps(_to_jsonable(overrides), indent=2))
    (path / "seed_brokers.json").write_text(json.dumps(_to_jsonable(brokers), indent=2))


def load_seed_into_store(store, seed_dir: str) -> None:
    """Read seed JSON files from seed_dir and insert rows into the store."""
    path = Path(seed_dir)
    if not path.exists():
        return

    fp_file = path / "seed_fingerprints.json"
    if fp_file.exists():
        for row in json.loads(fp_file.read_text()):
            fp = Fingerprint(
                hash=row["fingerprint_hash"],
                broker=row.get("broker"),
                file_type=row["file_type"],
                clean_headers=row["header_set"],
            )
            store.record_accepted_mapping(fp, row["mapping_blueprint"], user=row.get("created_by", "seed"))

    corr_file = path / "seed_corrections.json"
    if corr_file.exists():
        for row in json.loads(corr_file.read_text()):
            # Reinsert votes directly — don't use upsert_correction because
            # record_accepted_mapping above already inserted these once.
            # Instead, bump vote_count by (vote_count - 1) more times.
            key = (row["clean_header"], row["target_field"], row["file_type"], row.get("broker"))
            if hasattr(store, "_corrections") and key in store._corrections:
                # top up to the saved vote_count
                current = store._corrections[key]["vote_count"]
                target = row["vote_count"]
                for _ in range(max(0, target - current)):
                    store.upsert_correction(
                        row["clean_header"], row["target_field"], row["file_type"],
                        row.get("broker"), user="seed",
                    )

    alias_file = path / "seed_geocode_aliases.json"
    if alias_file.exists():
        for row in json.loads(alias_file.read_text()):
            store.insert_geocode_alias(
                key=row["raw_text_hash"],
                raw_text=row["raw_text"],
                answer={
                    "formatted_addr": row["formatted_addr"],
                    "latitude": row["latitude"],
                    "longitude": row["longitude"],
                    "city": row["city"],
                    "zip_code": row["zip_code"],
                },
                source=row["source"],
            )

    override_file = path / "seed_geocode_overrides.json"
    if override_file.exists():
        for row in json.loads(override_file.read_text()):
            store.record_geocode_override(
                raw_text=row["raw_text"],
                corrected_addr=row["corrected_addr"],
                lat=row["latitude"],
                lng=row["longitude"],
                city=row["city"],
                zip_code=row["zip_code"],
                user=row.get("corrected_by", "seed"),
            )

    broker_file = path / "seed_brokers.json"
    if broker_file.exists():
        for row in json.loads(broker_file.read_text()):
            store.upsert_broker(
                canonical_name=row["canonical_name"],
                user=row.get("created_by", "seed"),
                alias=None,
            )
            if hasattr(store, "_brokers") and row["canonical_name"] in store._brokers:
                store._brokers[row["canonical_name"]]["aliases"] = list(row.get("aliases", []))
                store._brokers[row["canonical_name"]]["upload_count"] = row.get("upload_count", 1)
```

- [ ] **Step 4:** Run the test.

```bash
pytest tests/test_seed_io.py -v
```

Expected: 2 passed.

- [ ] **Step 5:** Commit.

```bash
git add learning/seed_io.py tests/test_seed_io.py
git commit -m "learning: add seed I/O JSON round-trip helpers with tests"
```

### Task 1.6: Implement SupabaseLearningStore / SqliteLearningStore

**Files:**
- Modify: `learning/store.py`
- Create: `tests/test_store_sqlite.py`

- [ ] **Step 1:** Write `tests/test_store_sqlite.py`:

```python
"""SqliteLearningStore integration test — uses a real in-memory SQLite DB."""
import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from learning.schemas import LearningBase
from learning.store import SqliteLearningStore
from engine.types import Fingerprint


@pytest.fixture
def store():
    engine = create_engine("sqlite:///:memory:")
    LearningBase.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    return SqliteLearningStore(session_factory=Session)


def test_sqlite_fingerprint_roundtrip(store):
    fp = Fingerprint(hash="h1", broker="JLL", file_type="SALE", clean_headers=["a", "b"])
    store.record_accepted_mapping(fp, {"address": "Property"}, user="t@x.com")
    got = store.get_fingerprint_by_hash("h1")
    assert got is not None
    assert got["mapping_blueprint"] == {"address": "Property"}
    assert got["sample_count"] == 1


def test_sqlite_fingerprint_idempotent_increments(store):
    fp = Fingerprint(hash="h1", broker=None, file_type="SALE", clean_headers=["a"])
    store.record_accepted_mapping(fp, {"a": "b"}, user="t@x.com")
    store.record_accepted_mapping(fp, {"a": "b"}, user="t@x.com")
    store.record_accepted_mapping(fp, {"a": "b"}, user="t@x.com")
    got = store.get_fingerprint_by_hash("h1")
    assert got["sample_count"] == 3


def test_sqlite_broker_alias_merging(store):
    store.upsert_broker("JLL", user="u", alias="Jones Lang LaSalle")
    store.upsert_broker("JLL", user="u", alias="jll capital markets")
    b = store.find_broker_by_alias("jones lang lasalle")
    assert b is not None
    assert b["canonical_name"] == "JLL"
    assert b["upload_count"] == 2


def test_sqlite_corrections_vote_counting(store):
    store.upsert_correction("sizesf", "leased_sf", "LEASE", "JLL", user="u")
    store.upsert_correction("sizesf", "leased_sf", "LEASE", "JLL", user="u")
    rows = store.get_corrections_for_context("LEASE", "JLL")
    assert len(rows) == 1
    assert rows[0]["vote_count"] == 2


def test_sqlite_geocode_override_insert_and_lookup(store):
    store.record_geocode_override(
        raw_text="1234 Main", corrected_addr="1234 Main St, Houston, TX",
        lat=29.7, lng=-95.3, city="Houston", zip_code="77001", user="u",
    )
    import hashlib, re
    key = hashlib.sha256(re.sub(r"\s+", " ", "1234 main".strip()).encode()).hexdigest()
    got = store.get_geocode_override(key)
    assert got is not None
    assert got["city"] == "Houston"
```

- [ ] **Step 2:** Run — expect fail.

- [ ] **Step 3:** Write `learning/store.py`:

```python
"""Concrete LearningStore implementations backed by SQLAlchemy sessions."""
from __future__ import annotations
import hashlib
import re
from datetime import datetime
from typing import Optional, Callable
from sqlalchemy.orm import Session
from sqlalchemy import select, and_
from learning.schemas import (
    TemplateFingerprint, ColumnMappingCorrection, GeocodeAlias, GeocodeOverride,
    Broker, PdfExtractionCorrection,
)
from engine.types import Fingerprint


def _hash_raw(text: str) -> str:
    normalized = re.sub(r"\s+", " ", text.lower().strip())
    return hashlib.sha256(normalized.encode()).hexdigest()


def _jaccard(a: list[str], b: list[str]) -> float:
    sa, sb = set(a), set(b)
    if not sa and not sb:
        return 1.0
    if not sa or not sb:
        return 0.0
    return len(sa & sb) / len(sa | sb)


class SqliteLearningStore:
    """LearningStore backed by a SQLAlchemy session. Works with SQLite or Postgres."""

    def __init__(self, session_factory: Callable[[], Session]):
        self._Session = session_factory

    def _session(self) -> Session:
        return self._Session()

    @staticmethod
    def _hash_raw(text: str) -> str:
        return _hash_raw(text)

    # ---- Fingerprints ----
    def get_fingerprint_by_hash(self, fp_hash: str) -> Optional[dict]:
        with self._session() as s:
            row = s.scalar(select(TemplateFingerprint).where(TemplateFingerprint.fingerprint_hash == fp_hash))
            return self._fingerprint_to_dict(row) if row else None

    def find_fuzzy_fingerprints(self, file_type, header_set, min_jaccard):
        with self._session() as s:
            rows = s.scalars(select(TemplateFingerprint).where(TemplateFingerprint.file_type == file_type)).all()
        hits = []
        for row in rows:
            score = _jaccard(header_set, row.header_set or [])
            if score >= min_jaccard:
                hits.append((self._fingerprint_to_dict(row), score))
        return sorted(hits, key=lambda t: t[1], reverse=True)

    def find_broker_fingerprints(self, broker, header_set, min_jaccard):
        with self._session() as s:
            rows = s.scalars(select(TemplateFingerprint).where(TemplateFingerprint.broker == broker)).all()
        hits = []
        for row in rows:
            score = _jaccard(header_set, row.header_set or [])
            if score >= min_jaccard:
                hits.append((self._fingerprint_to_dict(row), score))
        return sorted(hits, key=lambda t: t[1], reverse=True)

    def record_accepted_mapping(self, fp: Fingerprint, final_mapping, user):
        with self._session() as s:
            existing = s.scalar(select(TemplateFingerprint).where(TemplateFingerprint.fingerprint_hash == fp.hash))
            if existing:
                existing.sample_count += 1
                existing.confidence = min(1.0, existing.confidence + 0.05)
                existing.last_seen_at = datetime.utcnow()
                merged = {**(existing.mapping_blueprint or {}), **final_mapping}
                existing.mapping_blueprint = merged
            else:
                s.add(TemplateFingerprint(
                    fingerprint_hash=fp.hash,
                    broker=fp.broker,
                    file_type=fp.file_type,
                    header_set=list(fp.clean_headers),
                    mapping_blueprint=dict(final_mapping),
                    sample_count=1,
                    confidence=0.5,
                    created_by=user,
                ))
            s.commit()
        for target, src_hdr in final_mapping.items():
            clean = src_hdr.strip().lower()
            self.upsert_correction(clean, target, fp.file_type, fp.broker, user)

    def _fingerprint_to_dict(self, row) -> dict:
        return {
            "id": row.id,
            "fingerprint_hash": row.fingerprint_hash,
            "broker": row.broker,
            "file_type": row.file_type,
            "header_set": row.header_set or [],
            "mapping_blueprint": row.mapping_blueprint or {},
            "sample_count": row.sample_count,
            "confidence": row.confidence,
            "created_by": row.created_by,
        }

    # ---- Corrections ----
    def get_corrections_for_context(self, file_type, broker):
        with self._session() as s:
            rows = s.scalars(
                select(ColumnMappingCorrection).where(
                    and_(ColumnMappingCorrection.file_type == file_type,
                         ColumnMappingCorrection.broker == broker)
                )
            ).all()
            return [
                {
                    "clean_header": r.clean_header,
                    "target_field": r.target_field,
                    "file_type": r.file_type,
                    "broker": r.broker,
                    "vote_count": r.vote_count,
                }
                for r in rows
            ]

    def upsert_correction(self, clean_header, target, file_type, broker, user):
        with self._session() as s:
            existing = s.scalar(
                select(ColumnMappingCorrection).where(and_(
                    ColumnMappingCorrection.clean_header == clean_header,
                    ColumnMappingCorrection.target_field == target,
                    ColumnMappingCorrection.file_type == file_type,
                    ColumnMappingCorrection.broker == broker,
                ))
            )
            if existing:
                existing.vote_count += 1
                existing.last_confirmed = datetime.utcnow()
            else:
                s.add(ColumnMappingCorrection(
                    clean_header=clean_header,
                    target_field=target,
                    file_type=file_type,
                    broker=broker,
                    vote_count=1,
                ))
            s.commit()

    # ---- Geocoding ----
    def get_geocode_override(self, key):
        with self._session() as s:
            row = s.scalar(select(GeocodeOverride).where(GeocodeOverride.raw_text_hash == key))
            if not row:
                return None
            return {
                "raw_text_hash": row.raw_text_hash,
                "raw_text": row.raw_text,
                "corrected_addr": row.corrected_addr,
                "latitude": row.latitude,
                "longitude": row.longitude,
                "city": row.city,
                "zip_code": row.zip_code,
                "corrected_by": row.corrected_by,
            }

    def get_geocode_alias(self, key):
        with self._session() as s:
            row = s.scalar(select(GeocodeAlias).where(GeocodeAlias.raw_text_hash == key))
            if not row:
                return None
            return {
                "raw_text_hash": row.raw_text_hash,
                "raw_text": row.raw_text,
                "formatted_addr": row.formatted_addr,
                "latitude": row.latitude,
                "longitude": row.longitude,
                "city": row.city,
                "zip_code": row.zip_code,
                "source": row.source,
                "hit_count": row.hit_count,
            }

    def insert_geocode_alias(self, key, raw_text, answer, source):
        with self._session() as s:
            existing = s.scalar(select(GeocodeAlias).where(GeocodeAlias.raw_text_hash == key))
            if existing:
                existing.hit_count += 1
            else:
                s.add(GeocodeAlias(
                    raw_text_hash=key,
                    raw_text=raw_text,
                    formatted_addr=answer.get("formatted_addr"),
                    latitude=answer.get("latitude"),
                    longitude=answer.get("longitude"),
                    city=answer.get("city"),
                    zip_code=answer.get("zip_code"),
                    source=source,
                ))
            s.commit()

    def bump_hit_count(self, key):
        with self._session() as s:
            row = s.scalar(select(GeocodeAlias).where(GeocodeAlias.raw_text_hash == key))
            if row:
                row.hit_count += 1
                s.commit()

    def record_geocode_override(self, raw_text, corrected_addr, lat, lng, city, zip_code, user):
        key = _hash_raw(raw_text)
        with self._session() as s:
            existing = s.scalar(select(GeocodeOverride).where(GeocodeOverride.raw_text_hash == key))
            if existing:
                existing.corrected_addr = corrected_addr
                existing.latitude = lat
                existing.longitude = lng
                existing.city = city
                existing.zip_code = zip_code
                existing.corrected_by = user
                existing.corrected_at = datetime.utcnow()
            else:
                s.add(GeocodeOverride(
                    raw_text_hash=key, raw_text=raw_text, corrected_addr=corrected_addr,
                    latitude=lat, longitude=lng, city=city, zip_code=zip_code,
                    corrected_by=user,
                ))
            s.commit()

    # ---- Brokers ----
    def upsert_broker(self, canonical_name, user, alias=None):
        with self._session() as s:
            existing = s.scalar(select(Broker).where(Broker.canonical_name == canonical_name))
            if existing:
                existing.upload_count += 1
                if alias:
                    a_lower = alias.lower()
                    current_aliases = list(existing.aliases or [])
                    if a_lower not in current_aliases:
                        current_aliases.append(a_lower)
                        existing.aliases = current_aliases
            else:
                s.add(Broker(
                    canonical_name=canonical_name,
                    aliases=[alias.lower()] if alias else [],
                    upload_count=1,
                    confidence=0.5,
                    is_brokerage=(canonical_name != "INTERNAL"),
                    created_by=user,
                ))
            s.commit()

    def find_broker_by_alias(self, candidate):
        c = candidate.lower()
        with self._session() as s:
            rows = s.scalars(select(Broker)).all()
            for b in rows:
                if c == b.canonical_name.lower():
                    return self._broker_to_dict(b)
                for a in (b.aliases or []):
                    if a == c:
                        return self._broker_to_dict(b)
        return None

    def _broker_to_dict(self, row) -> dict:
        return {
            "id": row.id,
            "canonical_name": row.canonical_name,
            "aliases": list(row.aliases or []),
            "upload_count": row.upload_count,
            "confidence": row.confidence,
            "is_brokerage": row.is_brokerage,
        }

    def record_broker_correction(self, fingerprint_hash, llm_guess, confirmed, user):
        # Simple insert — we don't have a dedicated corrections table; logged via brokers.upload_count bump
        # + we rely on the subsequent record_accepted_mapping to update the fingerprint.broker if needed
        pass  # reserved for Phase 6 expansion

    # ---- PDF ----
    def get_pdf_corrections(self, pdf_hash):
        with self._session() as s:
            rows = s.scalars(
                select(PdfExtractionCorrection).where(PdfExtractionCorrection.pdf_hash == pdf_hash)
            ).all()
            return [
                {
                    "pdf_hash": r.pdf_hash, "page_num": r.page_num, "row_index": r.row_index,
                    "field": r.field, "original_value": r.original_value,
                    "corrected_value": r.corrected_value, "corrected_by": r.corrected_by,
                }
                for r in rows
            ]

    def record_pdf_correction(self, pdf_hash, page_num, row_index, field, original, corrected, user):
        with self._session() as s:
            s.add(PdfExtractionCorrection(
                pdf_hash=pdf_hash, page_num=page_num, row_index=row_index,
                field=field, original_value=original, corrected_value=corrected,
                corrected_by=user,
            ))
            s.commit()

    # ---- Seed ----
    def load_seed(self, seed_dir: str) -> None:
        from learning.seed_io import load_seed_into_store
        load_seed_into_store(self, seed_dir)


# SupabaseLearningStore is functionally identical to SqliteLearningStore —
# both use SQLAlchemy, both handle the same dialects. We alias it for clarity
# at the call site (`app.py` picks one based on DB_URL).
SupabaseLearningStore = SqliteLearningStore
```

- [ ] **Step 4:** Run tests.

```bash
pytest tests/test_store_sqlite.py -v
```

Expected: 5 passed.

- [ ] **Step 5:** Commit.

```bash
git add learning/store.py tests/test_store_sqlite.py
git commit -m "learning: add SqliteLearningStore with SQLAlchemy backend"
```

---

## Chunk 3: Phase 2 — Extract Existing Logic Into Stages

**Goal:** Port every function in `comp_engine.py` into focused `engine/` modules with ZERO behavior change, verified by a 16-file equivalence test using a cached embedding fixture. Current `comp_engine.py` becomes a thin re-export facade so `app.py` imports remain valid.

**Critical rule:** DO NOT refactor logic in this chunk. Copy functions verbatim into new modules. Change only import paths. Any cleanups, fixes, or new features come in later chunks where we can attribute behavior differences.

**Embedding determinism strategy:** `openai.embeddings.create` returns different vectors across runs/model updates. We cache every embedding call made against the 16 sample files once, store the cache as `tests/fixtures/embeddings_cache.pkl`, and monkeypatch `get_embeddings` in tests to read from it. With deterministic embeddings, the new split engine must produce mappings, confidence scores, rate_basis, and cleaned numeric columns identical to the current engine.

---

### Task 2.1: Engine loaders module

**Files:**
- Create: `engine/loaders.py`
- Reference: `comp_engine.py:84-361` (get_sheet_names, detect_table_segments, robust_load_file_segmented, robust_load_file, _is_data_row, _merge_split_headers, _trim_leading_empty_columns)

- [ ] **Step 1:** Write failing test.

```python
# tests/test_loaders.py
import pandas as pd
from engine.loaders import get_sheet_names, robust_load_file, robust_load_file_segmented

SAMPLE = "sample comp files/Arlington Class B Comps.xlsx"

def test_get_sheet_names_returns_list():
    names = get_sheet_names(SAMPLE)
    assert isinstance(names, list) and len(names) >= 1

def test_robust_load_file_returns_dataframe():
    df = robust_load_file(SAMPLE)
    assert isinstance(df, pd.DataFrame)
    assert len(df.columns) > 0
    assert len(df) > 0

def test_robust_load_file_segmented_returns_list_of_segments():
    segments = robust_load_file_segmented(SAMPLE)
    assert isinstance(segments, list)
    assert all(isinstance(s, pd.DataFrame) for s in segments)
```

- [ ] **Step 2:** Run test — expect ImportError.

```bash
pytest tests/test_loaders.py -v
```

- [ ] **Step 3:** Create `engine/loaders.py`.

Copy these functions VERBATIM from `comp_engine.py`:
- `get_sheet_names` (line 84)
- `detect_table_segments` (line 96)
- `robust_load_file_segmented` (line 217)
- `robust_load_file` (line 229)
- `_is_data_row` (line 300)
- `_merge_split_headers` (line 312)
- `_trim_leading_empty_columns` (line 348)

Include any imports each function needs: `pandas`, `openpyxl`, `os`, `re`, `pathlib.Path`.

Do NOT change any logic. Only change module-internal references if one helper calls another (they all stay in the same file, so no changes needed).

- [ ] **Step 4:** Run tests — expect pass.

```bash
pytest tests/test_loaders.py -v
```

Expected: 3 passed.

- [ ] **Step 5:** Commit.

```bash
git add engine/loaders.py tests/test_loaders.py
git commit -m "engine: extract loaders from comp_engine"
```

---

### Task 2.2: Engine cleaning module

**Files:**
- Create: `engine/cleaning.py`
- Reference: `comp_engine.py:362-417, 687-814` (clean_header, get_column_profile, _detect_rate_unit_from_header, apply_rate_logic, _to_float, HOUSTON_RATE_THRESHOLD constant)

- [ ] **Step 1:** Write failing test.

```python
# tests/test_cleaning.py
import pandas as pd
from engine.cleaning import clean_header, get_column_profile, apply_rate_logic, _to_float

def test_clean_header_lowercases_and_strips():
    assert clean_header("  Rent PSF  ") == "rent psf"
    assert clean_header(None) == ""

def test_column_profile_detects_numeric():
    s = pd.Series([1.0, 2.5, 3.7, None])
    profile = get_column_profile(s)
    assert profile["numeric_ratio"] > 0.5

def test_to_float_parses_currency():
    assert _to_float("$1,234.56") == 1234.56
    assert _to_float("—") is None
    assert _to_float(None) is None

def test_apply_rate_logic_splits_monthly_annual():
    df = pd.DataFrame({"Rate": [1.50, 18.00, 1.25, 24.00]})
    result = apply_rate_logic(df, rate_header="Rate")
    assert "rate_monthly" in result.columns or "rate_basis" in result.columns
```

- [ ] **Step 2:** Run — expect ImportError.

```bash
pytest tests/test_cleaning.py -v
```

- [ ] **Step 3:** Create `engine/cleaning.py` by copying verbatim from `comp_engine.py`:
- `HOUSTON_RATE_THRESHOLD` module-level constant
- `clean_header` (line 362)
- `get_column_profile` (line 372)
- `_detect_rate_unit_from_header` (line 687)
- `apply_rate_logic` (line 711)
- `_to_float` (line 766)

Imports needed: `pandas as pd`, `numpy as np`, `re`.

- [ ] **Step 4:** Run tests.

```bash
pytest tests/test_cleaning.py -v
```

Expected: 4 passed.

- [ ] **Step 5:** Commit.

```bash
git add engine/cleaning.py tests/test_cleaning.py
git commit -m "engine: extract cleaning and rate logic from comp_engine"
```

---

### Task 2.3: Engine mapping module

**Files:**
- Create: `engine/mapping.py`
- Reference: `comp_engine.py:32-83, 418-686` (get_embeddings, classify_file_type, BASE_OVERRIDES, LEASE_OVERRIDES, SALE_OVERRIDES, LEASE_SCHEMA, SALE_SCHEMA, _find_override, _get_schema_embeddings, generate_standardized_df)

- [ ] **Step 1:** Write failing test.

```python
# tests/test_mapping.py
import pandas as pd
from engine.mapping import (
    classify_file_type,
    generate_standardized_df,
    LEASE_SCHEMA,
    SALE_SCHEMA,
)

def test_classify_detects_lease():
    headers = ["Property", "Tenant", "Rent PSF", "Lease Date", "SF"]
    result = classify_file_type(headers, filename="lease_comps.xlsx")
    assert result == "lease"

def test_classify_detects_sale():
    headers = ["Property", "Sale Price", "Sale Date", "SF", "PSF"]
    result = classify_file_type(headers, filename="sale_comps.xlsx")
    assert result == "sale"

def test_generate_standardized_df_returns_mapping(monkeypatch):
    # Monkeypatched embeddings fixture is loaded by conftest.py
    df = pd.DataFrame({
        "Property Name": ["Foo"],
        "Tenant Name": ["Bar"],
        "Rent PSF": [18.5],
        "Lease Date": ["2024-01-15"],
        "SF": [10000],
    })
    out_df, mappings, confidence = generate_standardized_df(df, LEASE_SCHEMA, "lease")
    assert isinstance(mappings, dict)
    assert len(mappings) > 0
```

- [ ] **Step 2:** Run — expect ImportError.

- [ ] **Step 3:** Create `engine/mapping.py` by copying verbatim. ALSO create `engine/openai_client.py` now (not later) with the `_get_openai_client` helper so `get_embeddings` and the future broker extractor share one client. Copy:
- `_get_openai_client` (line 22) → put in `engine/openai_client.py` as module-level `_client()` function
- `get_embeddings` (line 32) → put in `engine/mapping.py`, updating its internal call to `from engine.openai_client import _client` then `_client().embeddings.create(...)`
- `BASE_OVERRIDES`, `LEASE_OVERRIDES`, `SALE_OVERRIDES` module-level dicts — copy entire dict literals
- `LEASE_SCHEMA`, `SALE_SCHEMA` module-level dicts — copy entire dict literals
- `classify_file_type` (line 418)
- `_find_override` (line 529)
- `_get_schema_embeddings` (line 558)
- `generate_standardized_df` (line 567)

Imports needed: `openai`, `numpy as np`, `pandas as pd`, `scipy.optimize.linear_sum_assignment`, `os`, `re`, `from engine.cleaning import clean_header, get_column_profile`.

**Important:** This test cannot run until Task 2.5 creates the embedding fixture. Mark test with `@pytest.mark.skip(reason="requires embedding fixture from Task 2.5")` for now and remove the skip in Task 2.5.

- [ ] **Step 4:** Verify file imports cleanly.

```bash
python -c "from engine.mapping import LEASE_SCHEMA, SALE_SCHEMA, classify_file_type; print('ok')"
```

Expected: `ok`

- [ ] **Step 5:** Commit.

```bash
git add engine/mapping.py tests/test_mapping.py
git commit -m "engine: extract mapping and schema embeddings from comp_engine"
```

---

### Task 2.4: Engine geocoding module

**Files:**
- Create: `engine/geocoding.py`
- Reference: `comp_engine.py:816-892` (_is_in_texas, _extract_address_components, fetch_google_data)

- [ ] **Step 1:** Write failing test using `responses` library to stub Google Maps HTTP.

```python
# tests/test_geocoding.py
import responses
from engine.geocoding import fetch_google_data

GOOGLE_URL = "https://maps.googleapis.com/maps/api/geocode/json"

@responses.activate
def test_fetch_google_data_parses_response():
    responses.add(
        responses.GET,
        GOOGLE_URL,
        json={
            "status": "OK",
            "results": [{
                "formatted_address": "123 Main St, Houston, TX 77002, USA",
                "geometry": {"location": {"lat": 29.7604, "lng": -95.3698}},
                "address_components": [
                    {"long_name": "123", "types": ["street_number"]},
                    {"long_name": "Main St", "types": ["route"]},
                    {"long_name": "Houston", "types": ["locality"]},
                    {"long_name": "TX", "short_name": "TX", "types": ["administrative_area_level_1"]},
                    {"long_name": "77002", "types": ["postal_code"]},
                ],
            }],
        },
    )
    result = fetch_google_data("123 Main, Houston", api_key="fake-key")
    assert result["latitude"] == 29.7604
    assert result["longitude"] == -95.3698
    assert result["state"] == "TX"
```

- [ ] **Step 2:** Run — expect ImportError.

- [ ] **Step 3:** Create `engine/geocoding.py` by copying verbatim:
- `_is_in_texas` (line 816)
- `_extract_address_components` (line 820)
- `fetch_google_data` (line 832)

Imports needed: `requests`, `re`.

- [ ] **Step 4:** Run test.

```bash
pytest tests/test_geocoding.py -v
```

Expected: 1 passed.

- [ ] **Step 5:** Commit.

```bash
git add engine/geocoding.py tests/test_geocoding.py
git commit -m "engine: extract google geocoding from comp_engine"
```

---

### Task 2.5: Build embedding fixture cache

**Files:**
- Create: `tools/rebuild_embedding_fixture.py`
- Create: `tests/fixtures/embeddings_cache.pkl` (generated artifact, committed to repo)

**Why this exists:** `openai.embeddings.create` is non-deterministic across runs and model versions. To make equivalence tests reproducible, we cache every embedding request made while running the CURRENT engine against all 16 sample files, then replay from that cache during tests.

- [ ] **Step 1:** Write the rebuild tool.

```python
# tools/rebuild_embedding_fixture.py
"""
Build tests/fixtures/embeddings_cache.pkl by running comp_engine against the
16 sample files and intercepting every call to get_embeddings().

Run whenever the sample file set changes or schema overrides change.
Requires OPENAI_API_KEY in environment.
"""
import os
import pickle
import pathlib
import hashlib
import sys

import comp_engine

SAMPLE_DIR = pathlib.Path("sample comp files")
OUT = pathlib.Path("tests/fixtures/embeddings_cache.pkl")

cache: dict[str, list[list[float]]] = {}
_original = comp_engine.get_embeddings


def _key(texts):
    joined = "\x00".join(texts)
    return hashlib.sha256(joined.encode("utf-8")).hexdigest()


def _wrapped(texts):
    k = _key(list(texts))
    if k not in cache:
        cache[k] = _original(list(texts))
    return cache[k]


def main():
    if not os.environ.get("OPENAI_API_KEY"):
        print("OPENAI_API_KEY required", file=sys.stderr)
        sys.exit(1)

    comp_engine.get_embeddings = _wrapped

    files = sorted(SAMPLE_DIR.iterdir())
    for path in files:
        if path.suffix.lower() not in {".xlsx", ".xls", ".pdf", ".csv"}:
            continue
        print(f"Processing {path.name} ...")
        try:
            if path.suffix.lower() == ".pdf":
                print("  skipped PDF (no embeddings needed)")
                continue
            sheets = comp_engine.get_sheet_names(str(path))
            comp_engine.process_all_sheets(str(path), path.name, selected_sheets=sheets)
        except Exception as e:
            print(f"  warning: {e}")

    OUT.parent.mkdir(parents=True, exist_ok=True)
    with OUT.open("wb") as fh:
        pickle.dump(cache, fh)
    print(f"Wrote {len(cache)} entries to {OUT}")


if __name__ == "__main__":
    main()
```

- [ ] **Step 2:** Run the tool once to materialize the fixture.

```bash
python tools/rebuild_embedding_fixture.py
```

Expected: writes `tests/fixtures/embeddings_cache.pkl` with 50+ entries.

- [ ] **Step 3:** Add fixture loader to `tests/conftest.py`.

```python
# tests/conftest.py — append to existing content
import pickle
import hashlib
import pathlib
import pytest

CACHE_PATH = pathlib.Path(__file__).parent / "fixtures" / "embeddings_cache.pkl"


def _cache_key(texts):
    return hashlib.sha256("\x00".join(texts).encode("utf-8")).hexdigest()


@pytest.fixture(autouse=True)
def deterministic_embeddings(monkeypatch, request):
    """Replace get_embeddings with cache-backed version for all tests.

    Opt out with @pytest.mark.live_embeddings on a test.
    """
    if request.node.get_closest_marker("live_embeddings"):
        return

    if not CACHE_PATH.exists():
        pytest.skip(f"embedding fixture missing: {CACHE_PATH}")

    with CACHE_PATH.open("rb") as fh:
        cache: dict = pickle.load(fh)

    def _fake(texts):
        key = _cache_key(list(texts))
        if key not in cache:
            raise KeyError(
                f"embedding cache miss for {len(texts)} texts — "
                f"rerun tools/rebuild_embedding_fixture.py"
            )
        return cache[key]

    # Patch BOTH the original location AND the new location.
    import comp_engine
    import engine.mapping
    monkeypatch.setattr(comp_engine, "get_embeddings", _fake)
    monkeypatch.setattr(engine.mapping, "get_embeddings", _fake)
```

- [ ] **Step 4:** Register custom marker in `pytest.ini`.

```ini
# Append to existing [pytest] section
markers =
    live_embeddings: test makes real OpenAI embedding calls (opt out of fixture cache)
```

- [ ] **Step 5:** Re-enable the skipped `test_generate_standardized_df_returns_mapping` in `tests/test_mapping.py` (remove the `@pytest.mark.skip`) and run it.

```bash
pytest tests/test_mapping.py -v
```

Expected: 3 passed (now that fixture is available).

- [ ] **Step 6:** Commit.

```bash
git add tools/rebuild_embedding_fixture.py tests/fixtures/embeddings_cache.pkl tests/conftest.py pytest.ini tests/test_mapping.py
git commit -m "test: add embedding fixture cache for deterministic engine tests"
```

---

### Task 2.6: Engine facade + equivalence test against 16 sample files

**Files:**
- Create: `engine/__init__.py` (public re-exports)
- Create: `engine/comp_engine_facade.py` (compatibility surface)
- Modify: `comp_engine.py` (becomes thin facade)
- Create: `tests/test_engine_equivalence.py`

**Why a facade:** `app.py` imports from `comp_engine` at line 14:
```python
from comp_engine import robust_load_file, robust_load_file_segmented, process_file_to_clean_output, fetch_google_data, get_sheet_names, process_all_sheets, apply_manual_mapping, LEASE_SCHEMA, SALE_SCHEMA
```
This import statement MUST continue to work unchanged. `comp_engine.py` becomes a re-export layer.

- [ ] **Step 1:** Write equivalence test BEFORE rewriting the facade.

```python
# tests/test_engine_equivalence.py
"""
Runs the CURRENT comp_engine and the NEW split engine against every sample file
and asserts the outputs are identical. Both sides use the cached embedding fixture
from tests/conftest.py so embeddings are deterministic.

This test guards Phase 2: any behavior drift caused by the extraction fails loud.
"""
import pathlib
import pickle
import pandas as pd
import numpy as np
import pytest

SAMPLE_DIR = pathlib.Path("sample comp files")
XLSX_FILES = sorted(p for p in SAMPLE_DIR.iterdir() if p.suffix.lower() == ".xlsx")

# Stash a reference snapshot of current engine outputs before facade rewrite.
SNAPSHOT_PATH = pathlib.Path("tests/fixtures/engine_output_snapshot.pkl")


def _normalize_df(df: pd.DataFrame) -> pd.DataFrame:
    """Stable column order and dtype coercion for comparison."""
    if df is None or df.empty:
        return df
    df = df.copy()
    df = df.reindex(sorted(df.columns), axis=1)
    for col in df.columns:
        if df[col].dtype == object:
            df[col] = df[col].astype(str)
    return df.reset_index(drop=True)


def _snapshot_key(path: pathlib.Path, sheet: str) -> str:
    return f"{path.name}::{sheet}"


@pytest.fixture(scope="module")
def snapshot():
    if not SNAPSHOT_PATH.exists():
        pytest.skip(f"snapshot missing — run Step 2 to build it")
    with SNAPSHOT_PATH.open("rb") as fh:
        return pickle.load(fh)


@pytest.mark.parametrize("path", XLSX_FILES, ids=lambda p: p.name)
def test_new_engine_matches_snapshot(path, snapshot):
    import comp_engine  # This is the NEW facade after Task 2.6

    sheets = comp_engine.get_sheet_names(str(path))
    for sheet in sheets:
        key = _snapshot_key(path, sheet)
        if key not in snapshot:
            continue
        expected = snapshot[key]

        result = comp_engine.process_all_sheets(str(path), path.name, selected_sheets=[sheet])
        actual_df = result[sheet]["clean_df"] if isinstance(result, dict) and sheet in result else None
        actual_mappings = result[sheet]["mappings"] if isinstance(result, dict) and sheet in result else {}

        assert _normalize_df(actual_df).equals(_normalize_df(expected["clean_df"])), (
            f"{key}: cleaned df differs"
        )
        assert actual_mappings == expected["mappings"], f"{key}: mappings differ"

        for col in actual_df.select_dtypes(include=[np.number]).columns:
            if col in expected["clean_df"].columns:
                a = actual_df[col].to_numpy(dtype=float, na_value=np.nan)
                e = expected["clean_df"][col].to_numpy(dtype=float, na_value=np.nan)
                np.testing.assert_allclose(a, e, atol=1e-9, equal_nan=True)
```

- [ ] **Step 2:** Build the reference snapshot using the CURRENT `comp_engine.py` (BEFORE the facade rewrite).

```python
# tools/snapshot_current_engine.py
"""Run BEFORE Task 2.6 facade rewrite. Captures current outputs for equivalence."""
import pathlib
import pickle
import sys
sys.path.insert(0, ".")

import comp_engine  # current, pre-rewrite
from tests.conftest import _cache_key  # reuse fixture key

SAMPLE_DIR = pathlib.Path("sample comp files")
OUT = pathlib.Path("tests/fixtures/engine_output_snapshot.pkl")

# Replay embeddings from cache so the snapshot itself is deterministic
import pickle as _pkl
with open("tests/fixtures/embeddings_cache.pkl", "rb") as fh:
    _cache = _pkl.load(fh)


def _fake(texts):
    return _cache[_cache_key(list(texts))]


comp_engine.get_embeddings = _fake

snapshot = {}
for path in sorted(SAMPLE_DIR.iterdir()):
    if path.suffix.lower() != ".xlsx":
        continue
    try:
        sheets = comp_engine.get_sheet_names(str(path))
        result = comp_engine.process_all_sheets(str(path), path.name, selected_sheets=sheets)
        for sheet, data in (result or {}).items():
            snapshot[f"{path.name}::{sheet}"] = {
                "clean_df": data.get("clean_df"),
                "mappings": data.get("mappings", {}),
            }
    except Exception as e:
        print(f"warn {path.name}: {e}")

OUT.parent.mkdir(parents=True, exist_ok=True)
with OUT.open("wb") as fh:
    pickle.dump(snapshot, fh)
print(f"Captured {len(snapshot)} snapshot entries -> {OUT}")
```

```bash
python tools/snapshot_current_engine.py
```

Expected: writes `tests/fixtures/engine_output_snapshot.pkl`.

- [ ] **Step 3:** Now rewrite `comp_engine.py` as a pure re-export facade.

```python
# comp_engine.py — REPLACES existing file entirely
"""Backward-compat facade. New code should import from engine.* modules directly.

This file exists so app.py's existing imports remain valid after the Phase 2
extraction. Every name here is re-exported from an engine/ submodule.
"""
from engine.loaders import (
    get_sheet_names,
    detect_table_segments,
    robust_load_file,
    robust_load_file_segmented,
)
from engine.cleaning import (
    clean_header,
    get_column_profile,
    apply_rate_logic,
    HOUSTON_RATE_THRESHOLD,
)
from engine.mapping import (
    get_embeddings,
    classify_file_type,
    generate_standardized_df,
    LEASE_SCHEMA,
    SALE_SCHEMA,
    BASE_OVERRIDES,
    LEASE_OVERRIDES,
    SALE_OVERRIDES,
)
from engine.geocoding import fetch_google_data

# The orchestration functions (process_file_to_clean_output, process_all_sheets,
# apply_manual_mapping) temporarily live here until Phase 4 moves them into
# engine/pipeline.py. Copy them VERBATIM from the pre-rewrite comp_engine.py.

import pandas as pd
from engine.cleaning import apply_rate_logic
from engine.mapping import classify_file_type, generate_standardized_df, LEASE_SCHEMA, SALE_SCHEMA

# -------------------------------------------------------------------------
# IMPLEMENTER: paste the exact bodies of these three functions from the
# PRE-REWRITE comp_engine.py below. Do NOT write `...` — paste real code.
# Line ranges in the pre-rewrite file:
#   process_file_to_clean_output : lines 893-926
#   process_all_sheets           : lines 927-974
#   apply_manual_mapping         : lines 975-1023
# Before deleting the old file, copy these three function bodies verbatim
# into the stubs below. The equivalence test in Step 5 will fail loudly if
# you forget.
# -------------------------------------------------------------------------


def process_file_to_clean_output(df, filename, sheet_name=None):
    raise NotImplementedError("paste body from pre-rewrite comp_engine.py:893-926")


def process_all_sheets(file_path, filename, selected_sheets=None):
    raise NotImplementedError("paste body from pre-rewrite comp_engine.py:927-974")


def apply_manual_mapping(input_df, mapping_dict, schema_dict, file_type, filename):
    raise NotImplementedError("paste body from pre-rewrite comp_engine.py:975-1023")


__all__ = [
    "get_sheet_names", "detect_table_segments", "robust_load_file", "robust_load_file_segmented",
    "clean_header", "get_column_profile", "apply_rate_logic", "HOUSTON_RATE_THRESHOLD",
    "get_embeddings", "classify_file_type", "generate_standardized_df",
    "LEASE_SCHEMA", "SALE_SCHEMA", "BASE_OVERRIDES", "LEASE_OVERRIDES", "SALE_OVERRIDES",
    "fetch_google_data",
    "process_file_to_clean_output", "process_all_sheets", "apply_manual_mapping",
]
```

**Note to implementer:** For the three orchestration functions marked `...`, do a literal copy-paste from the original `comp_engine.py` (lines 893-1023). Do not rewrite them. They will be further refactored in Chunk 5 when we add writeback hooks.

- [ ] **Step 4:** Verify `app.py` still imports cleanly.

```bash
python -c "import app" 2>&1 | head -20
```

Expected: no ImportError from the `from comp_engine import ...` line. (Streamlit-specific errors are fine if the script runs top-level Streamlit calls; we only care about import resolution.)

- [ ] **Step 5:** Run the equivalence test across all 16 files.

```bash
pytest tests/test_engine_equivalence.py -v
```

Expected: ALL parametrized cases pass. If any fail, diff the output and find where the extraction drifted — the most common cause is a forgotten helper or a stale override dict.

- [ ] **Step 6:** Run the full test suite.

```bash
pytest tests/ -v
```

Expected: all previous tests still pass (loaders, cleaning, mapping, geocoding, fakes, seed_io, store_sqlite, equivalence).

- [ ] **Step 7:** Commit.

```bash
git add comp_engine.py engine/__init__.py tests/test_engine_equivalence.py tools/snapshot_current_engine.py tests/fixtures/engine_output_snapshot.pkl
git commit -m "engine: split comp_engine into focused modules with equivalence test

Extracts loaders, cleaning, mapping, and geocoding into engine/ submodules.
comp_engine.py becomes a re-export facade so app.py imports are unchanged.
A 16-file equivalence test locks in byte-identical behavior using the
cached embedding fixture for determinism."
```

---

**End of Chunk 3.** After this chunk, the engine is fully factored but behaviorally identical. Chunks 4+ can now safely add fingerprinting, writeback, and learning on top of a clean foundation.

---

## Chunk 4: Phase 3 — Fingerprinting and Tiered Mapping Lookup

**Goal:** When a file arrives, fingerprint its header layout, look up prior exact/fuzzy/broker matches from the learning store, and skip or bias the embedding mapping step based on what we've learned. If no match, fall back to the current embedding pipeline but apply any correction-weighted hints.

**Tier order (from spec):**
1. **Exact hash match** → skip embeddings, return stored mapping with `source="exact"`.
2. **Fuzzy Jaccard ≥ 0.80** on header set → return stored mapping with `source="fuzzy"`.
3. **Broker Jaccard ≥ 0.60** AND same detected broker → return stored mapping with `source="broker"`.
4. **Correction-weighted embedding fallback** → run embeddings, but if `(file_type, raw_header)` has recorded corrections in the learning store, boost the corrected target column's score by +0.10 before Hungarian assignment. Source: `"embedding"` or `"embedding+corrections"`.

---

### Task 3.1: Fingerprint primitives

**Files:**
- Create: `engine/fingerprint.py`
- Create: `tests/test_fingerprint.py`

- [ ] **Step 1:** Write failing tests.

```python
# tests/test_fingerprint.py
import pandas as pd
from engine.fingerprint import (
    compute_fingerprint,
    header_set,
    jaccard_similarity,
    tier1_exact_lookup,
    tier2_fuzzy_lookup,
)
from learning.fakes import FakeLearningStore


def test_compute_fingerprint_is_stable():
    headers = ["Property", "Rent PSF", "Tenant", "SF"]
    fp1 = compute_fingerprint(headers, filename="a.xlsx", sheet_name="Sheet1", file_type="lease")
    fp2 = compute_fingerprint(headers, filename="a.xlsx", sheet_name="Sheet1", file_type="lease")
    assert fp1.raw_hash == fp2.raw_hash
    assert fp1.file_type == "lease"


def test_compute_fingerprint_header_order_agnostic_for_set_hash():
    fp1 = compute_fingerprint(["A", "B", "C"], "f.xlsx", None, "lease")
    fp2 = compute_fingerprint(["C", "A", "B"], "f.xlsx", None, "lease")
    # raw_hash DIFFERS (order matters), but header_set_hash MATCHES
    assert fp1.raw_hash != fp2.raw_hash
    assert fp1.header_set_hash == fp2.header_set_hash


def test_jaccard_identical_sets():
    assert jaccard_similarity({"a", "b", "c"}, {"a", "b", "c"}) == 1.0


def test_jaccard_disjoint():
    assert jaccard_similarity({"a", "b"}, {"c", "d"}) == 0.0


def test_jaccard_partial():
    score = jaccard_similarity({"a", "b", "c", "d"}, {"a", "b", "e", "f"})
    assert abs(score - (2 / 6)) < 1e-9


def test_tier1_exact_lookup_hits():
    store = FakeLearningStore()
    headers = ["Property", "Rent PSF", "Tenant"]
    fp = compute_fingerprint(headers, "x.xlsx", None, "lease")
    store.record_accepted_mapping(
        fp, {"Property": "property_name", "Rent PSF": "rent_psf", "Tenant": "tenant"},
        confirmed_by="user@test"
    )
    hit = tier1_exact_lookup(store, fp)
    assert hit is not None
    assert hit.mappings["Rent PSF"] == "rent_psf"
    assert hit.source == "exact"


def test_tier2_fuzzy_lookup_hits_on_80_percent_overlap():
    store = FakeLearningStore()
    trained = compute_fingerprint(
        ["Property", "Rent PSF", "Tenant", "SF", "Date"],
        "trained.xlsx", None, "lease"
    )
    store.record_accepted_mapping(
        trained,
        {"Property": "property_name", "Rent PSF": "rent_psf", "Tenant": "tenant",
         "SF": "sf", "Date": "lease_date"},
        confirmed_by="user@test"
    )
    new_fp = compute_fingerprint(
        ["Property", "Rent PSF", "Tenant", "SF", "Expiration"],  # 4/6 overlap = 0.67 — miss
        "new.xlsx", None, "lease"
    )
    assert tier2_fuzzy_lookup(store, new_fp, threshold=0.80) is None

    close_fp = compute_fingerprint(
        ["Property", "Rent PSF", "Tenant", "SF", "Date", "Broker"],  # 5/6 = 0.83 — hit
        "close.xlsx", None, "lease"
    )
    hit = tier2_fuzzy_lookup(store, close_fp, threshold=0.80)
    assert hit is not None
    assert hit.source == "fuzzy"
    assert hit.similarity >= 0.80


def test_tier3_broker_lookup_hits_on_60_percent_overlap():
    from engine.fingerprint import tier3_broker_lookup

    store = FakeLearningStore()
    broker_id = store.upsert_broker(name="JLL", confirmed_by="u@test")
    trained = compute_fingerprint(
        ["Property", "Rent PSF", "Tenant", "SF", "Date"],
        "jll_file.xlsx", None, "lease"
    )
    store.record_accepted_mapping(
        fingerprint=trained,
        mappings={"Property": "property_name", "Rent PSF": "rent_psf",
                  "Tenant": "tenant", "SF": "sf", "Date": "lease_date"},
        confirmed_by="u@test",
        broker_id=broker_id,
    )

    # Different JLL template with ~3/6 = 0.50 overlap — below threshold, miss
    far_fp = compute_fingerprint(
        ["Property", "Rent PSF", "Tenant", "Expiration", "Suite", "Floor"],
        "jll_other.xlsx", None, "lease"
    )
    assert tier3_broker_lookup(store, far_fp, broker_name="JLL", threshold=0.60) is None

    # Close JLL template with 4/6 = 0.67 overlap — hit
    close_fp = compute_fingerprint(
        ["Property", "Rent PSF", "Tenant", "SF", "Term", "Concessions"],
        "jll_close.xlsx", None, "lease"
    )
    hit = tier3_broker_lookup(store, close_fp, broker_name="JLL", threshold=0.60)
    assert hit is not None
    assert hit.source == "broker"
    assert hit.similarity >= 0.60
```

- [ ] **Step 2:** Run — expect ImportError.

- [ ] **Step 3:** Implement `engine/fingerprint.py`.

```python
# engine/fingerprint.py
"""Fingerprinting and tiered lookup against a LearningStore.

A fingerprint is a content-addressable identifier for a (file_type, header-layout)
pair. Exact match = raw_hash; fuzzy match = Jaccard over header_set_hash-component
headers.
"""
from __future__ import annotations

import hashlib
from dataclasses import dataclass
from typing import Optional

from engine.types import Fingerprint, FingerprintMatch
from engine.cleaning import clean_header
from learning.protocol import LearningStore


def _normalize_header(h: str) -> str:
    return clean_header(h)


def header_set(headers: list[str]) -> set[str]:
    return {_normalize_header(h) for h in headers if _normalize_header(h)}


def _hash(items: list[str]) -> str:
    joined = "\x00".join(items)
    return hashlib.sha256(joined.encode("utf-8")).hexdigest()


def compute_fingerprint(
    headers: list[str],
    filename: str,
    sheet_name: Optional[str],
    file_type: str,
) -> Fingerprint:
    normalized_ordered = [_normalize_header(h) for h in headers]
    normalized_set_sorted = sorted(header_set(headers))
    return Fingerprint(
        raw_hash=_hash([file_type] + normalized_ordered),
        header_set_hash=_hash([file_type] + normalized_set_sorted),
        headers=list(headers),
        normalized_headers=normalized_ordered,
        file_type=file_type,
        filename=filename,
        sheet_name=sheet_name,
    )


def jaccard_similarity(a: set[str], b: set[str]) -> float:
    if not a and not b:
        return 1.0
    union = a | b
    if not union:
        return 0.0
    return len(a & b) / len(union)


def tier1_exact_lookup(store: LearningStore, fp: Fingerprint) -> Optional[FingerprintMatch]:
    record = store.get_fingerprint_by_hash(fp.raw_hash)
    if record is None:
        return None
    return FingerprintMatch(
        source="exact",
        similarity=1.0,
        fingerprint=fp,
        mappings=record["mappings"],
        confidence=record.get("confidence", 1.0),
        hit_count=record.get("hit_count", 0),
    )


def tier2_fuzzy_lookup(
    store: LearningStore, fp: Fingerprint, threshold: float = 0.80
) -> Optional[FingerprintMatch]:
    candidates = store.find_fuzzy_fingerprints(file_type=fp.file_type)
    if not candidates:
        return None
    target = header_set(fp.headers)
    best = None
    best_score = 0.0
    for cand in candidates:
        cand_set = set(cand.get("normalized_headers", []))
        score = jaccard_similarity(target, cand_set)
        if score > best_score:
            best_score = score
            best = cand
    if best is None or best_score < threshold:
        return None
    return FingerprintMatch(
        source="fuzzy",
        similarity=best_score,
        fingerprint=fp,
        mappings=best["mappings"],
        confidence=best.get("confidence", best_score),
        hit_count=best.get("hit_count", 0),
    )


def tier3_broker_lookup(
    store: LearningStore,
    fp: Fingerprint,
    broker_name: Optional[str],
    threshold: float = 0.60,
) -> Optional[FingerprintMatch]:
    if not broker_name:
        return None
    candidates = store.find_broker_fingerprints(broker_name=broker_name, file_type=fp.file_type)
    if not candidates:
        return None
    target = header_set(fp.headers)
    best = None
    best_score = 0.0
    for cand in candidates:
        cand_set = set(cand.get("normalized_headers", []))
        score = jaccard_similarity(target, cand_set)
        if score > best_score:
            best_score = score
            best = cand
    if best is None or best_score < threshold:
        return None
    return FingerprintMatch(
        source="broker",
        similarity=best_score,
        fingerprint=fp,
        mappings=best["mappings"],
        confidence=best.get("confidence", best_score),
        hit_count=best.get("hit_count", 0),
    )
```

- [ ] **Step 4:** Run tests.

```bash
pytest tests/test_fingerprint.py -v
```

Expected: 8 passed.

- [ ] **Step 5:** Extend `FakeLearningStore` if tests reveal missing stubs. The methods `find_fuzzy_fingerprints` and `find_broker_fingerprints` must return a list of dicts with `normalized_headers`, `mappings`, `confidence`, and `hit_count` keys. If you implemented them correctly in Chunk 2, this step is a no-op verification.

- [ ] **Step 6:** Commit.

```bash
git add engine/fingerprint.py tests/test_fingerprint.py
git commit -m "engine: add fingerprinting and tiered lookup primitives"
```

---

### Task 3.2: Correction-weighted embedding fallback

**Files:**
- Modify: `engine/mapping.py` — add `generate_standardized_df_with_hints()`
- Create: `tests/test_mapping_hints.py`

**What this does:** The existing `generate_standardized_df` builds a cost matrix from embedding cosine similarities and runs Hungarian assignment. When corrections exist for `(file_type, raw_header)` → `target_column`, we bump the relevant cell in the cost matrix by +0.10 (i.e., lower the Hungarian cost by 0.10, since Hungarian minimizes). This biases assignment toward human-confirmed answers without overriding strong embedding matches.

- [ ] **Step 1:** Write failing test.

```python
# tests/test_mapping_hints.py
import pandas as pd
from engine.mapping import generate_standardized_df_with_hints, LEASE_SCHEMA
from learning.fakes import FakeLearningStore


def test_hint_biases_toward_corrected_column():
    """
    If the user has previously corrected 'Asking Rate' → 'rent_psf' in lease files,
    a new file with that ambiguous header should pick rent_psf over any other close
    candidate from the lease schema.
    """
    store = FakeLearningStore()
    store.upsert_correction(
        file_type="lease",
        raw_header="asking rate",
        target_column="rent_psf",
        confirmed_by="user@test",
    )

    df = pd.DataFrame({
        "Property": ["P1"],
        "Tenant": ["T1"],
        "Asking Rate": [18.5],
        "Lease Date": ["2024-01-15"],
        "SF": [10000],
    })

    out_df, mappings, confidence = generate_standardized_df_with_hints(
        df, LEASE_SCHEMA, file_type="lease", store=store
    )
    assert mappings.get("Asking Rate") == "rent_psf"
```

- [ ] **Step 2:** Run — expect ImportError for `generate_standardized_df_with_hints`.

- [ ] **Step 3:** Add the hinted wrapper to `engine/mapping.py`.

```python
# engine/mapping.py — append
from typing import Optional
from learning.protocol import LearningStore
from engine.cleaning import clean_header


HINT_BIAS = 0.10  # cost reduction applied when a correction matches


def generate_standardized_df_with_hints(
    df: "pd.DataFrame",
    schema_dict: dict,
    file_type: str,
    store: Optional[LearningStore] = None,
    threshold: float = 0.55,
):
    """Same as generate_standardized_df, but biases the cost matrix using stored
    corrections for (file_type, raw_header) -> target_column pairs.

    Drop-in substitute — if store is None or has no corrections, behavior is
    identical to generate_standardized_df.
    """
    import numpy as np
    from scipy.optimize import linear_sum_assignment

    raw_headers = [str(c) for c in df.columns]
    normalized = [clean_header(h) for h in raw_headers]

    schema_cols = list(schema_dict.keys())
    schema_labels = list(schema_dict.values())

    header_embeds = np.array(get_embeddings(normalized))
    schema_embeds = np.array(_get_schema_embeddings(schema_dict))

    # cosine similarity -> cost matrix (1 - sim)
    sim = header_embeds @ schema_embeds.T / (
        np.linalg.norm(header_embeds, axis=1, keepdims=True)
        * np.linalg.norm(schema_embeds, axis=1, keepdims=True).T
        + 1e-12
    )
    cost = 1.0 - sim

    # Apply hint bias.
    if store is not None:
        for i, raw in enumerate(normalized):
            corrections = store.get_corrections_for_context(
                file_type=file_type, raw_header=raw
            )
            for target_col, weight in corrections.items():
                if target_col in schema_cols:
                    j = schema_cols.index(target_col)
                    cost[i, j] -= HINT_BIAS * min(weight, 5) / 5.0  # cap at 5 hits

    row_ind, col_ind = linear_sum_assignment(cost)

    mappings: dict[str, str] = {}
    confidence: dict[str, float] = {}
    for i, j in zip(row_ind, col_ind):
        score = sim[i, j]
        if score >= threshold:
            mappings[raw_headers[i]] = schema_cols[j]
            confidence[raw_headers[i]] = float(score)

    # Apply hard overrides on top — the existing generate_standardized_df has
    # a block that walks BASE_OVERRIDES / LEASE_OVERRIDES / SALE_OVERRIDES via
    # _find_override(). Rather than duplicating that logic, refactor it into a
    # shared helper first:
    _apply_hard_overrides(
        raw_headers=raw_headers,
        normalized=normalized,
        file_type=file_type,
        schema_cols=schema_cols,
        mappings=mappings,
        confidence=confidence,
    )

    out_df = pd.DataFrame()
    for raw, target in mappings.items():
        out_df[target] = df[raw]

    return out_df, mappings, confidence


def _apply_hard_overrides(raw_headers, normalized, file_type, schema_cols, mappings, confidence):
    """Shared override logic, also called by generate_standardized_df.

    REFACTOR NOTE: In the same edit pass, extract the existing override block
    from generate_standardized_df (currently inline at the end of that function)
    into this helper, then call _apply_hard_overrides from BOTH
    generate_standardized_df and generate_standardized_df_with_hints. Do not
    duplicate the logic — a single helper keeps both code paths in sync.

    The override block does:
      1. Pick the right override dict: BASE_OVERRIDES + (LEASE_OVERRIDES if lease else SALE_OVERRIDES).
      2. For each normalized header, call _find_override(cleaned_header, overrides, target_col).
      3. If it matches and the target_col is in schema_cols, set mappings[raw_header] = target_col with confidence 1.0.
    """
    overrides = dict(BASE_OVERRIDES)
    overrides.update(LEASE_OVERRIDES if file_type == "lease" else SALE_OVERRIDES)
    for i, raw in enumerate(raw_headers):
        cleaned = normalized[i]
        for target_col in schema_cols:
            if _find_override(cleaned, overrides, target_col):
                mappings[raw] = target_col
                confidence[raw] = 1.0
                break
```

**Note:** The `FakeLearningStore.get_corrections_for_context(file_type, raw_header)` must return a dict of `{target_column: hit_count}` per the corrected Protocol in Chunk 2. Verify your implementation matches this shape.

- [ ] **Step 4:** Run test.

```bash
pytest tests/test_mapping_hints.py -v
```

Expected: 1 passed.

- [ ] **Step 5:** Run equivalence test to confirm no regression.

```bash
pytest tests/test_engine_equivalence.py -v
```

Expected: still all passing (new function is additive; existing `generate_standardized_df` unchanged).

- [ ] **Step 6:** Commit.

```bash
git add engine/mapping.py tests/test_mapping_hints.py
git commit -m "engine: add correction-weighted embedding fallback"
```

---

### Task 3.3: Wire tiered lookup into the pipeline

**Files:**
- Create: `engine/pipeline.py`
- Modify: `comp_engine.py` (facade updates)
- Create: `tests/test_pipeline_tiers.py`

**What this does:** Introduces a single `run_mapping_stage(df, filename, sheet_name, store)` that performs classify → fingerprint → tier1 → tier2 → tier3 → fallback. This is the function higher layers will call instead of the current monolithic `process_file_to_clean_output`.

- [ ] **Step 1:** Write failing test.

```python
# tests/test_pipeline_tiers.py
import pandas as pd
from engine.pipeline import run_mapping_stage
from engine.fingerprint import compute_fingerprint
from learning.fakes import FakeLearningStore


def test_exact_tier_skips_embeddings():
    store = FakeLearningStore()
    headers = ["Property Name", "Tenant Name", "Rent PSF", "Date", "SF"]
    df = pd.DataFrame({h: ["x"] for h in headers})

    fp = compute_fingerprint(headers, "f.xlsx", None, "lease")
    store.record_accepted_mapping(
        fp,
        {"Property Name": "property_name", "Tenant Name": "tenant",
         "Rent PSF": "rent_psf", "Date": "lease_date", "SF": "sf"},
        confirmed_by="user@test",
    )

    result = run_mapping_stage(df, filename="f.xlsx", sheet_name=None, store=store)
    assert result.source == "exact"
    assert result.mappings["Rent PSF"] == "rent_psf"


def test_no_match_falls_back_to_embeddings():
    store = FakeLearningStore()
    df = pd.DataFrame({
        "Property": ["p"], "Tenant": ["t"], "Rent PSF": [1],
        "Lease Date": ["2024-01-01"], "SF": [100],
    })
    result = run_mapping_stage(df, filename="f.xlsx", sheet_name=None, store=store)
    assert result.source in {"embedding", "embedding+corrections"}
    assert "Rent PSF" in result.mappings
```

- [ ] **Step 2:** Run — expect ImportError.

- [ ] **Step 3:** Implement `engine/pipeline.py`.

```python
# engine/pipeline.py
"""High-level stage orchestrator. Callers use run_mapping_stage; the legacy
process_all_sheets in comp_engine.py gets rewritten to delegate here in Chunk 5.
"""
from __future__ import annotations

from typing import Optional
import pandas as pd

from engine.types import MappingResult
from engine.cleaning import clean_header
from engine.mapping import (
    classify_file_type,
    generate_standardized_df,
    generate_standardized_df_with_hints,
    LEASE_SCHEMA,
    SALE_SCHEMA,
)
from engine.fingerprint import (
    compute_fingerprint,
    tier1_exact_lookup,
    tier2_fuzzy_lookup,
    tier3_broker_lookup,
)
from learning.protocol import LearningStore


def _schema_for(file_type: str) -> dict:
    return LEASE_SCHEMA if file_type == "lease" else SALE_SCHEMA


def run_mapping_stage(
    df: pd.DataFrame,
    filename: str,
    sheet_name: Optional[str],
    store: LearningStore,
    broker_name: Optional[str] = None,
) -> MappingResult:
    raw_headers = [str(c) for c in df.columns]
    file_type = classify_file_type(raw_headers, filename=filename, sheet_name=sheet_name)
    schema = _schema_for(file_type)
    fp = compute_fingerprint(raw_headers, filename, sheet_name, file_type)

    # Tier 1: exact
    hit = tier1_exact_lookup(store, fp)
    if hit is not None:
        out_df = _apply_mappings(df, hit.mappings)
        return MappingResult(
            fingerprint=fp,
            mappings=hit.mappings,
            confidence={h: hit.confidence for h in hit.mappings},
            source="exact",
            similarity=1.0,
            cleaned_df=out_df,
        )

    # Tier 2: fuzzy
    hit = tier2_fuzzy_lookup(store, fp, threshold=0.80)
    if hit is not None:
        mappings = _filter_mappings_to_present_headers(hit.mappings, raw_headers)
        out_df = _apply_mappings(df, mappings)
        return MappingResult(
            fingerprint=fp,
            mappings=mappings,
            confidence={h: hit.similarity for h in mappings},
            source="fuzzy",
            similarity=hit.similarity,
            cleaned_df=out_df,
        )

    # Tier 3: broker
    hit = tier3_broker_lookup(store, fp, broker_name=broker_name, threshold=0.60)
    if hit is not None:
        mappings = _filter_mappings_to_present_headers(hit.mappings, raw_headers)
        out_df = _apply_mappings(df, mappings)
        return MappingResult(
            fingerprint=fp,
            mappings=mappings,
            confidence={h: hit.similarity for h in mappings},
            source="broker",
            similarity=hit.similarity,
            cleaned_df=out_df,
        )

    # Fallback: correction-weighted embedding
    out_df, mappings, confidence = generate_standardized_df_with_hints(
        df, schema, file_type=file_type, store=store
    )
    source = "embedding+corrections" if _has_any_corrections(store, file_type, raw_headers) else "embedding"
    return MappingResult(
        fingerprint=fp,
        mappings=mappings,
        confidence=confidence,
        source=source,
        similarity=0.0,
        cleaned_df=out_df,
    )


def _apply_mappings(df: pd.DataFrame, mappings: dict[str, str]) -> pd.DataFrame:
    out = pd.DataFrame()
    for raw, target in mappings.items():
        if raw in df.columns:
            out[target] = df[raw]
    return out


def _filter_mappings_to_present_headers(
    mappings: dict[str, str], raw_headers: list[str]
) -> dict[str, str]:
    present = set(raw_headers)
    return {r: t for r, t in mappings.items() if r in present}


def _has_any_corrections(store: LearningStore, file_type: str, headers: list[str]) -> bool:
    for h in headers:
        if store.get_corrections_for_context(file_type=file_type, raw_header=clean_header(h)):
            return True
    return False
```

- [ ] **Step 4:** Run tests.

```bash
pytest tests/test_pipeline_tiers.py -v
```

Expected: 2 passed.

- [ ] **Step 5:** Run full suite to verify no cross-test regression.

```bash
pytest tests/ -v
```

Expected: all green.

- [ ] **Step 6:** Commit.

```bash
git add engine/pipeline.py tests/test_pipeline_tiers.py
git commit -m "engine: add tiered mapping stage orchestrator"
```

---

**End of Chunk 4.** The engine now knows how to consult prior knowledge before spending embedding tokens, and it degrades gracefully through fuzzy → broker → hinted-embedding tiers. Chunk 5 hooks the save path so user corrections flow back into the store.

---

## Chunk 5: Phase 4 — Writeback Hooks (Learn From Manual Corrections)

**Goal:** When the user clicks **Save** in the manual mapping UI, every segment's confirmed mapping flows into the learning store atomically with the database insert. If the DB save fails, no learning is recorded. If the learning write fails, we log but still commit the DB rows.

**Data contract from the UI (stable across parallel UI changes):**
- `final_mappings: dict[str, dict[str, str]]` — keyed by `segment_key = "<sheet_or_root>::<segment_index>"`, value is `{raw_header → target_column}`.
- `edited_dfs: dict[str, pd.DataFrame]` — keyed by `segment_key`, the final edited dataframe per segment (headers may have been renamed by the user).
- `confirmed_broker: Optional[str]` — broker name the user confirmed (or None).
- `geocode_overrides: dict[int, dict]` — keyed by row index within the concatenated final df, value is `{"raw_text": str, "override_address": str, "lat": float, "lng": float}`.

---

### Task 4.1: `persist_with_learning` core function

**Files:**
- Create: `learning/corrections.py`
- Create: `tests/test_corrections.py`

- [ ] **Step 1:** Write failing test.

```python
# tests/test_corrections.py
import pandas as pd
import pytest
from engine.types import SegmentResult, Fingerprint, MappingResult
from engine.fingerprint import compute_fingerprint
from learning.fakes import FakeLearningStore
from learning.corrections import persist_with_learning


def _make_segment(segment_key, headers, mappings, source="embedding"):
    fp = compute_fingerprint(headers, "f.xlsx", segment_key.split("::")[0], "lease")
    df = pd.DataFrame({h: ["v"] for h in headers})
    return SegmentResult(
        segment_key=segment_key,
        fingerprint=fp,
        mapping_result=MappingResult(
            fingerprint=fp,
            mappings=mappings,
            confidence={h: 0.7 for h in mappings},
            source=source,
            similarity=0.0,
            cleaned_df=df,
        ),
        cleaned_df=df,
    )


def test_persist_records_exact_mapping_and_saves_db():
    store = FakeLearningStore()
    saved_rows = []

    def fake_saver(df):
        saved_rows.append(df.copy())
        return list(range(len(df)))

    seg = _make_segment(
        "Sheet1::0",
        headers=["Property", "Rent PSF"],
        mappings={"Property": "property_name", "Rent PSF": "rent_psf"},
    )
    final_mappings = {"Sheet1::0": seg.mapping_result.mappings}
    edited = {"Sheet1::0": seg.cleaned_df}

    persist_with_learning(
        segments=[seg],
        final_mappings=final_mappings,
        edited_dfs=edited,
        confirmed_broker=None,
        geocode_overrides={},
        store=store,
        db_saver=fake_saver,
        user="u@test",
    )

    # DB got the rows
    assert len(saved_rows) == 1
    # Store now has the fingerprint
    rec = store.get_fingerprint_by_hash(seg.fingerprint.raw_hash)
    assert rec is not None
    assert rec["mappings"]["Rent PSF"] == "rent_psf"


def test_persist_records_corrections_when_user_renames_mapping():
    store = FakeLearningStore()

    def fake_saver(df):
        return list(range(len(df)))

    seg = _make_segment(
        "Sheet1::0",
        headers=["Property", "Asking Rate"],
        mappings={"Property": "property_name", "Asking Rate": "sf"},  # wrong guess
    )
    corrected = {"Sheet1::0": {"Property": "property_name", "Asking Rate": "rent_psf"}}

    persist_with_learning(
        segments=[seg],
        final_mappings=corrected,
        edited_dfs={"Sheet1::0": seg.cleaned_df},
        confirmed_broker=None,
        geocode_overrides={},
        store=store,
        db_saver=fake_saver,
        user="u@test",
    )

    corrections = store.get_corrections_for_context(file_type="lease", raw_header="asking rate")
    assert "rent_psf" in corrections


def test_db_save_failure_skips_learning_writes():
    store = FakeLearningStore()

    def failing_saver(df):
        raise RuntimeError("db exploded")

    seg = _make_segment(
        "Sheet1::0",
        headers=["Property"],
        mappings={"Property": "property_name"},
    )

    with pytest.raises(RuntimeError, match="db exploded"):
        persist_with_learning(
            segments=[seg],
            final_mappings={"Sheet1::0": seg.mapping_result.mappings},
            edited_dfs={"Sheet1::0": seg.cleaned_df},
            confirmed_broker=None,
            geocode_overrides={},
            store=store,
            db_saver=failing_saver,
            user="u@test",
        )

    assert store.get_fingerprint_by_hash(seg.fingerprint.raw_hash) is None


def test_rederives_fingerprint_from_edited_headers():
    """If user renamed a header in the editor, we must fingerprint the EDITED
    headers, not the original raw headers."""
    store = FakeLearningStore()

    def fake_saver(df):
        return [1]

    seg = _make_segment(
        "Sheet1::0",
        headers=["prop", "rate"],
        mappings={"prop": "property_name", "rate": "rent_psf"},
    )
    # User renamed columns in the editor
    edited_df = pd.DataFrame({"Property Name": ["x"], "Rent PSF": [1.0]})
    final_mappings = {
        "Sheet1::0": {"Property Name": "property_name", "Rent PSF": "rent_psf"}
    }

    persist_with_learning(
        segments=[seg],
        final_mappings=final_mappings,
        edited_dfs={"Sheet1::0": edited_df},
        confirmed_broker=None,
        geocode_overrides={},
        store=store,
        db_saver=fake_saver,
        user="u@test",
    )

    # Fingerprint stored under edited-header hash, not original
    from engine.fingerprint import compute_fingerprint
    new_fp = compute_fingerprint(
        ["Property Name", "Rent PSF"], "f.xlsx", "Sheet1", "lease"
    )
    assert store.get_fingerprint_by_hash(new_fp.raw_hash) is not None
    # Original (stale) hash was NOT stored
    assert store.get_fingerprint_by_hash(seg.fingerprint.raw_hash) is None


def test_persist_links_broker_id_to_rederived_fingerprint():
    """When a broker is confirmed, the fingerprint stored under the RE-DERIVED
    edited header hash must carry the broker_id. Broker writes must not use
    the stale pre-edit fingerprint."""
    store = FakeLearningStore()

    def fake_saver(df):
        return [1]

    seg = _make_segment(
        "Sheet1::0",
        headers=["prop", "rate"],
        mappings={"prop": "property_name", "rate": "rent_psf"},
    )
    edited_df = pd.DataFrame({"Property Name": ["x"], "Rent PSF": [1.0]})

    persist_with_learning(
        segments=[seg],
        final_mappings={"Sheet1::0": {"Property Name": "property_name", "Rent PSF": "rent_psf"}},
        edited_dfs={"Sheet1::0": edited_df},
        confirmed_broker="JLL",
        geocode_overrides={},
        store=store,
        db_saver=fake_saver,
        user="u@test",
    )

    from engine.fingerprint import compute_fingerprint
    new_fp = compute_fingerprint(
        ["Property Name", "Rent PSF"], "f.xlsx", "Sheet1", "lease"
    )
    record = store.get_fingerprint_by_hash(new_fp.raw_hash)
    assert record is not None
    assert record.get("broker_id") is not None
    # No stale record under the pre-edit hash
    assert store.get_fingerprint_by_hash(seg.fingerprint.raw_hash) is None
```

- [ ] **Step 2:** Run — expect ImportError.

- [ ] **Step 3:** Implement `learning/corrections.py`.

```python
# learning/corrections.py
"""Writeback hook: called by the Streamlit save handler.

This is the ONLY place corrections flow back into the learning store. The DB
save is primary: if it fails, nothing is learned. If a learning write fails
AFTER a successful DB save, we log and swallow so the user's data isn't lost.
"""
from __future__ import annotations

import logging
from typing import Callable, Optional

import pandas as pd

from engine.types import SegmentResult
from engine.fingerprint import compute_fingerprint
from engine.cleaning import clean_header
from engine.mapping import classify_file_type
from learning.protocol import LearningStore

log = logging.getLogger(__name__)


def persist_with_learning(
    segments: list[SegmentResult],
    final_mappings: dict[str, dict[str, str]],
    edited_dfs: dict[str, pd.DataFrame],
    confirmed_broker: Optional[str],
    geocode_overrides: dict[int, dict],
    store: LearningStore,
    db_saver: Callable[[pd.DataFrame], list[int]],
    user: str,
) -> list[int]:
    """
    1. Concatenate all edited segment dataframes.
    2. Save to DB via db_saver. If this raises, bubble up and learn nothing.
    3. On success, walk each segment:
       - Re-derive fingerprint from edited headers.
       - Record accepted mapping under new fingerprint.
       - Diff mappings against the original mapping_result; any change is a correction.
       - Apply geocode overrides.
       - Upsert broker if confirmed.
    4. Learning failures are logged but never re-raised.
    """
    concat = pd.concat(
        [edited_dfs[seg.segment_key] for seg in segments if seg.segment_key in edited_dfs],
        ignore_index=True,
    )

    inserted_ids = db_saver(concat)  # may raise; intentionally propagates

    # --- Now learn. All failures below are logged and swallowed. ---
    broker_id = None
    if confirmed_broker:
        try:
            broker_id = store.upsert_broker(name=confirmed_broker, confirmed_by=user)
        except Exception:
            log.exception("broker upsert failed")

    try:
        _record_mapping_learning(
            segments, final_mappings, edited_dfs, user, store, broker_id=broker_id
        )
    except Exception:
        log.exception("mapping learning writeback failed")

    try:
        _record_geocode_learning(geocode_overrides, user, store)
    except Exception:
        log.exception("geocode learning writeback failed")

    return inserted_ids


def _record_mapping_learning(segments, final_mappings, edited_dfs, user, store, broker_id=None):
    """Re-derive the fingerprint from EDITED headers, then record the mapping
    (linked to broker_id if present) and diff against the original guess for
    correction votes. This is the SINGLE path that writes fingerprints — we do
    NOT have a separate broker-learning pass that would double-write under the
    stale pre-edit fingerprint.
    """
    for seg in segments:
        if seg.segment_key not in final_mappings:
            continue
        new_mappings = final_mappings[seg.segment_key]
        edited_df = edited_dfs.get(seg.segment_key)
        if edited_df is None or edited_df.empty:
            continue

        # Re-derive fingerprint from EDITED headers (Must-Fix #5).
        edited_headers = [str(c) for c in edited_df.columns]
        file_type = seg.fingerprint.file_type
        new_fp = compute_fingerprint(
            edited_headers,
            filename=seg.fingerprint.filename,
            sheet_name=seg.fingerprint.sheet_name,
            file_type=file_type,
        )

        store.record_accepted_mapping(
            fingerprint=new_fp,
            mappings=new_mappings,
            confirmed_by=user,
            broker_id=broker_id,
        )

        # Diff against original guesses → corrections.
        original = seg.mapping_result.mappings
        for raw_header, final_target in new_mappings.items():
            original_target = original.get(raw_header)
            if original_target != final_target:
                store.upsert_correction(
                    file_type=file_type,
                    raw_header=clean_header(raw_header),
                    target_column=final_target,
                    confirmed_by=user,
                )


def _record_geocode_learning(geocode_overrides, user, store):
    for _row_idx, override in geocode_overrides.items():
        store.record_geocode_override(
            raw_text=override["raw_text"],
            override_address=override["override_address"],
            lat=override["lat"],
            lng=override["lng"],
            confirmed_by=user,
        )
```

**Note on `record_accepted_mapping` with `broker_id`:** The `LearningStore` Protocol in `learning/protocol.py` needs `broker_id: Optional[int] = None` as a kwarg on `record_accepted_mapping`. If you didn't include it in Chunk 2, add it now to the Protocol, the `FakeLearningStore`, and `SqliteLearningStore`. All three implementations must accept the kwarg (ignore it for now in the fake if broker isn't tested there).

- [ ] **Step 4:** Run tests.

```bash
pytest tests/test_corrections.py -v
```

Expected: 5 passed. If a failure is about missing `broker_id` kwarg, update the store implementations per the Chunk 2 Protocol corrections, then rerun.

- [ ] **Step 5:** Commit.

```bash
git add learning/corrections.py tests/test_corrections.py learning/protocol.py learning/fakes.py learning/store.py
git commit -m "learning: add persist_with_learning writeback hook"
```

---

### Task 4.2: Modify `app.py` save handler to call `persist_with_learning`

**Files:**
- Modify: `app.py` — around line 716 (current `apply_manual_mapping` call)

**Context:** `app.py` is being modified in parallel by the user's other Claude tab for UI changes. Keep this change minimal and additive: we are ONLY changing what happens on the save click, not the widget layout. The data contract (`final_mappings`, `edited_dfs`, `confirmed_broker`, `geocode_overrides`) is stable — it's what the parallel UI work feeds in.

- [ ] **Step 1:** Read the current save handler.

```bash
grep -n "apply_manual_mapping\|def save\|st.button" app.py | head -40
```

Locate the code block that currently calls `apply_manual_mapping` (~line 716) and the DB insert that follows it.

- [ ] **Step 2:** Write the new save handler logic. Replace the existing block with:

```python
# app.py — save handler (replace existing apply_manual_mapping call site)
from learning.corrections import persist_with_learning
from learning.store import SupabaseLearningStore  # or SqliteLearningStore locally
from database import get_session, SaleComp, LeaseComp, ensure_tables


def _db_saver_factory(file_type: str):
    """Returns a saver closure that inserts rows via SQLAlchemy ORM."""
    def save(final_df):
        ensure_tables()
        session = get_session()
        model = LeaseComp if file_type == "lease" else SaleComp
        ids = []
        try:
            for _, row in final_df.iterrows():
                obj = model(**{k: row[k] for k in row.index if hasattr(model, k)})
                session.add(obj)
                session.flush()
                ids.append(obj.id)
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()
        return ids
    return save


# Inside the save-click branch:
if save_clicked:
    store = SupabaseLearningStore(engine_url=os.environ["DB_URL"])
    file_type = segments[0].fingerprint.file_type  # all segments same type

    try:
        ids = persist_with_learning(
            segments=segments,
            final_mappings=st.session_state["final_mappings"],
            edited_dfs=st.session_state["edited_dfs"],
            confirmed_broker=st.session_state.get("confirmed_broker"),
            geocode_overrides=st.session_state.get("geocode_overrides", {}),
            store=store,
            db_saver=_db_saver_factory(file_type),
            user=st.session_state.get("user_email", "unknown"),
        )
        st.success(f"Saved {len(ids)} rows.")
    except Exception as e:
        st.error(f"Save failed: {e}")
```

**IMPORTANT — coordinate with parallel UI branch:** This block assumes `st.session_state["final_mappings"]`, `["edited_dfs"]`, `["confirmed_broker"]`, and `["geocode_overrides"]` are populated by the UI. If the parallel branch uses different key names, rename these at merge time. Do NOT restructure the UI widgets in this task.

- [ ] **Step 3:** Smoke test by launching Streamlit locally.

```bash
streamlit run app.py
```

Manually upload `sample comp files/Arlington Class B Comps.xlsx`, accept the default mapping, click save. Expected: no errors in the terminal, a success toast, and a new row in the learning store for that fingerprint.

Verify learning was recorded:

```bash
sqlite3 learning_local.db "SELECT raw_hash, file_type, hit_count FROM template_fingerprints;"
```

Expected: at least one row.

- [ ] **Step 4:** Commit.

```bash
git add app.py
git commit -m "app: call persist_with_learning on save to feed the learning store"
```

---

**End of Chunk 5.** Every manual save now teaches the system. Chunk 6 extends the same pattern to geocoding.

---

## Chunk 6: Phase 5 — Geocode Learning

**Goal:** Reduce Google Maps API calls and fix recurring bad geocodes. Flow: **override table → alias cache → LLM normalization → Google → write result back to alias cache**.

---

### Task 5.1: Learned geocoding wrapper

**Files:**
- Modify: `engine/geocoding.py` — add `resolve_geocode(raw_text, api_key, store, openai_client)`
- Create: `tests/test_geocode_learning.py`

- [ ] **Step 1:** Write failing tests (all four tiers).

```python
# tests/test_geocode_learning.py
import responses
from engine.geocoding import resolve_geocode
from learning.fakes import FakeLearningStore


GOOGLE_URL = "https://maps.googleapis.com/maps/api/geocode/json"


def _ok_response(addr, lat, lng):
    return {
        "status": "OK",
        "results": [{
            "formatted_address": addr,
            "geometry": {"location": {"lat": lat, "lng": lng}},
            "address_components": [
                {"long_name": "TX", "short_name": "TX",
                 "types": ["administrative_area_level_1"]},
            ],
        }],
    }


def test_override_table_short_circuits_everything():
    store = FakeLearningStore()
    # Overrides are stored under the Texas-normalized form (matches what
    # resolve_geocode will look up).
    store.record_geocode_override(
        raw_text="123 Fake St, TX",
        override_address="123 Fake St, Houston, TX 77002",
        lat=29.7, lng=-95.3,
        confirmed_by="user",
    )
    # Caller passes any form; resolve_geocode normalizes before lookup.
    result = resolve_geocode("123 Fake St", api_key="k", store=store, openai_client=None)
    assert result["latitude"] == 29.7
    assert result["source"] == "override"


def test_alias_cache_returns_before_calling_google():
    store = FakeLearningStore()
    store.insert_geocode_alias(
        raw_text="456 Main, TX",
        canonical_address="456 Main, Austin, TX 78701",
        lat=30.2, lng=-97.7,
    )
    result = resolve_geocode("456 Main", api_key="k", store=store, openai_client=None)
    assert result["source"] == "alias_cache"
    assert result["latitude"] == 30.2


@responses.activate
def test_miss_calls_google_and_writes_alias():
    responses.add(
        responses.GET, GOOGLE_URL,
        json=_ok_response("789 Pine, Dallas, TX 75201", 32.8, -96.8),
    )
    store = FakeLearningStore()
    result = resolve_geocode("789 Pine", api_key="k", store=store, openai_client=None)
    assert result["source"] == "google"
    assert result["latitude"] == 32.8

    # Second call should now hit the alias cache, no new HTTP request
    result2 = resolve_geocode("789 Pine", api_key="k", store=store, openai_client=None)
    assert result2["source"] == "alias_cache"
    assert len(responses.calls) == 1  # still only one network call


@responses.activate
def test_llm_normalization_used_when_google_fails_first_time():
    from types import SimpleNamespace
    # First call: Google returns ZERO_RESULTS for the raw string
    # Second call: after LLM cleans up the address, Google succeeds
    responses.add(responses.GET, GOOGLE_URL, json={"status": "ZERO_RESULTS", "results": []})
    responses.add(
        responses.GET, GOOGLE_URL,
        json=_ok_response("Cleaned Address, Houston, TX", 29.7, -95.3),
    )

    fake_llm = SimpleNamespace(normalize=lambda raw: "Cleaned Address, Houston, TX")

    store = FakeLearningStore()
    result = resolve_geocode(
        "garbage raw text", api_key="k", store=store, openai_client=fake_llm
    )
    assert result["source"] == "google+llm"
    assert len(responses.calls) == 2
```

- [ ] **Step 2:** Run — expect ImportError.

- [ ] **Step 3:** Implement `resolve_geocode` in `engine/geocoding.py`.

```python
# engine/geocoding.py — append
from typing import Optional
from learning.protocol import LearningStore


def resolve_geocode(
    raw_text: str,
    api_key: str,
    store: LearningStore,
    openai_client,
) -> dict:
    """Learned geocoding flow:
       1. Override table (user-confirmed fixed mapping) — highest precedence.
       2. Alias cache (raw_text → canonical address already resolved before).
       3. Google direct call.
       4. On ZERO_RESULTS, LLM normalizes the raw text → retry Google.
    Every successful resolution gets written back to the alias cache.

    All lookups use the Texas-biased normalized form as the cache key so an
    entry inserted as "456 Main, TX" hits on a subsequent query of "456 Main".
    """
    normalized = _normalize_raw(raw_text)

    override = store.get_geocode_override(normalized)
    if override is not None:
        return {**override, "source": "override"}

    alias = store.get_geocode_alias(normalized)
    if alias is not None:
        store.bump_hit_count(normalized)
        return {**alias, "source": "alias_cache"}

    result = fetch_google_data(normalized, api_key=api_key)
    if result and result.get("status") != "ZERO_RESULTS" and result.get("latitude"):
        store.insert_geocode_alias(
            raw_text=normalized,
            canonical_address=result["formatted_address"],
            lat=result["latitude"],
            lng=result["longitude"],
        )
        return {**result, "source": "google"}

    # LLM fallback.
    if openai_client is not None:
        try:
            cleaned = openai_client.normalize(raw_text)
        except Exception:
            cleaned = None
        if cleaned:
            cleaned_biased = _normalize_raw(cleaned)
            retry = fetch_google_data(cleaned_biased, api_key=api_key)
            if retry and retry.get("latitude"):
                store.insert_geocode_alias(
                    raw_text=normalized,  # key by ORIGINAL normalized, not cleaned
                    canonical_address=retry["formatted_address"],
                    lat=retry["latitude"],
                    lng=retry["longitude"],
                )
                return {**retry, "source": "google+llm"}

    return {"source": "failed", "raw_text": raw_text, "latitude": None, "longitude": None}


def _normalize_raw(raw_text: str) -> str:
    """Texas bias + whitespace trim. Single source of truth for cache keys."""
    s = (raw_text or "").strip()
    if not s:
        return s
    if ", TX" in s.upper() or " TX " in f" {s.upper()} " or s.upper().endswith(" TX"):
        return s
    return f"{s}, TX"
```

**Note:** `fetch_google_data` currently returns its result dict — verify it includes a `status` key or adjust the check to use `result.get("latitude") is None` as the miss signal.

- [ ] **Step 4:** Run tests.

```bash
pytest tests/test_geocode_learning.py -v
```

Expected: 4 passed.

- [ ] **Step 5:** Commit.

```bash
git add engine/geocoding.py tests/test_geocode_learning.py
git commit -m "engine: add learned geocoding with override/alias/llm tiers"
```

---

### Task 5.2: Wire `resolve_geocode` into the pipeline

**Files:**
- Modify: `engine/pipeline.py` — add `run_geocoding_stage`
- Modify: `engine/openai_client.py` — add a thin `normalize(raw_text)` helper

- [ ] **Step 1:** Add the OpenAI client helper.

```python
# engine/openai_client.py
"""Thin OpenAI wrappers used by the learning-aware engine stages."""
from __future__ import annotations
import os
from functools import lru_cache


@lru_cache(maxsize=1)
def _client():
    from openai import OpenAI
    return OpenAI(api_key=os.environ["OPENAI_API_KEY"])


def normalize(raw_text: str) -> str:
    """Ask GPT to clean up a messy address into a Google-friendly form.

    Texas is guaranteed, so we instruct the model to append ', TX' if missing.
    """
    prompt = (
        "Clean this commercial real estate property reference into a Google-Maps-"
        "friendly street address. The property is in Texas. Return ONLY the cleaned "
        "address with no commentary.\n\n"
        f"Raw: {raw_text}"
    )
    resp = _client().chat.completions.create(
        model="gpt-4o-mini",
        messages=[{"role": "user", "content": prompt}],
        temperature=0,
        max_tokens=80,
    )
    return resp.choices[0].message.content.strip()
```

- [ ] **Step 2:** Add `run_geocoding_stage` to `engine/pipeline.py`.

```python
# engine/pipeline.py — append
import pandas as pd
from engine.geocoding import resolve_geocode
from engine import openai_client


def run_geocoding_stage(
    df: pd.DataFrame,
    address_column: str,
    api_key: str,
    store: LearningStore,
) -> pd.DataFrame:
    out = df.copy()
    lats, lngs, sources, canonicals = [], [], [], []
    for raw in out[address_column].astype(str):
        result = resolve_geocode(
            raw_text=raw,
            api_key=api_key,
            store=store,
            openai_client=openai_client,
        )
        lats.append(result.get("latitude"))
        lngs.append(result.get("longitude"))
        sources.append(result.get("source"))
        canonicals.append(result.get("formatted_address"))
    out["latitude"] = lats
    out["longitude"] = lngs
    out["geocode_source"] = sources
    out["canonical_address"] = canonicals
    return out
```

- [ ] **Step 3:** Smoke test via unit test.

```python
# tests/test_geocoding_stage.py
import pandas as pd
from engine.pipeline import run_geocoding_stage
from learning.fakes import FakeLearningStore


def test_stage_uses_override_for_every_row(monkeypatch):
    store = FakeLearningStore()
    store.record_geocode_override(
        raw_text="123 A St, TX",  # Texas-normalized key
        override_address="123 A St, Houston, TX",
        lat=29.7, lng=-95.3,
        confirmed_by="u",
    )
    df = pd.DataFrame({"addr": ["123 A St", "123 A St"]})
    out = run_geocoding_stage(df, "addr", api_key="k", store=store)
    assert list(out["latitude"]) == [29.7, 29.7]
    assert list(out["geocode_source"]) == ["override", "override"]
```

```bash
pytest tests/test_geocoding_stage.py -v
```

Expected: 1 passed.

- [ ] **Step 4:** Commit.

```bash
git add engine/pipeline.py engine/openai_client.py tests/test_geocoding_stage.py
git commit -m "engine: add learning-aware geocoding stage to pipeline"
```

---

**End of Chunk 6.**

---

## Chunk 7: Phase 6 — Broker Detection (LLM + Learned Aliases)

**Goal:** Extract broker name from the first page of each upload using a single GPT-4o-mini call. Match against the learned `brokers` table; merge aliases automatically when similarity is high; surface ambiguous matches in the UI for user confirmation. Broker is OPTIONAL — pipeline works with `broker = NULL`.

---

### Task 6.1: LLM broker extractor

**Files:**
- Modify: `engine/openai_client.py` — add `extract_broker(file_bytes, filename)`
- Create: `tests/test_broker_extractor.py`

- [ ] **Step 1:** Write failing test using a stubbed OpenAI client.

```python
# tests/test_broker_extractor.py
from unittest.mock import MagicMock, patch
from engine.openai_client import extract_broker


def test_extract_broker_returns_name_from_llm():
    fake_resp = MagicMock()
    fake_resp.choices = [MagicMock(message=MagicMock(content='{"broker": "JLL", "confidence": 0.9}'))]

    with patch("engine.openai_client._client") as mock_client:
        mock_client.return_value.chat.completions.create.return_value = fake_resp
        result = extract_broker(sample_text="JLL - DFW Industrial Sales Comps - Rockwall", filename="JLL - DFW.xlsx")

    assert result["broker"] == "JLL"
    assert result["confidence"] >= 0.5


def test_extract_broker_returns_none_on_low_confidence():
    fake_resp = MagicMock()
    fake_resp.choices = [MagicMock(message=MagicMock(content='{"broker": null, "confidence": 0.1}'))]

    with patch("engine.openai_client._client") as mock_client:
        mock_client.return_value.chat.completions.create.return_value = fake_resp
        result = extract_broker(sample_text="generic file", filename="sheet.xlsx")

    assert result["broker"] is None
```

- [ ] **Step 2:** Implement `extract_broker`.

```python
# engine/openai_client.py — append
import json
from typing import Optional


def extract_broker(sample_text: str, filename: str) -> dict:
    """One-shot LLM call: return {"broker": str|None, "confidence": float}.

    Texts provided: filename + first 2000 chars of sample_text (usually a few
    header rows joined). Low confidence (<0.5) returns None.
    """
    prompt = (
        "You are given a commercial real estate comp file. Identify which brokerage "
        "firm produced it (e.g., JLL, CBRE, Colliers, Newmark, Cushman & Wakefield). "
        "Return a JSON object: {\"broker\": \"<name>\" or null, \"confidence\": 0.0-1.0}. "
        "If you're not reasonably sure, return null.\n\n"
        f"Filename: {filename}\n\n"
        f"Sample:\n{sample_text[:2000]}"
    )
    try:
        resp = _client().chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role": "user", "content": prompt}],
            response_format={"type": "json_object"},
            temperature=0,
            max_tokens=100,
        )
        parsed = json.loads(resp.choices[0].message.content)
        if parsed.get("confidence", 0) < 0.5:
            return {"broker": None, "confidence": parsed.get("confidence", 0)}
        return parsed
    except Exception:
        return {"broker": None, "confidence": 0.0}
```

- [ ] **Step 3:** Run tests.

```bash
pytest tests/test_broker_extractor.py -v
```

Expected: 2 passed.

- [ ] **Step 4:** Commit.

```bash
git add engine/openai_client.py tests/test_broker_extractor.py
git commit -m "engine: add LLM broker extractor"
```

---

### Task 6.2: Broker alias matching and auto-merge

**Files:**
- Modify: `learning/fakes.py` and `learning/store.py` — implement `find_broker_by_alias(name)`
- Create: `engine/brokers.py` — `resolve_broker(extracted_name, store) -> BrokerResolution`
- Create: `tests/test_broker_resolve.py`

- [ ] **Step 1:** Write failing tests.

```python
# tests/test_broker_resolve.py
from engine.brokers import resolve_broker
from learning.fakes import FakeLearningStore


def test_exact_match_returns_existing_broker():
    store = FakeLearningStore()
    store.upsert_broker(name="JLL", confirmed_by="u")
    result = resolve_broker("JLL", store)
    assert result.broker_name == "JLL"
    assert result.status == "matched"


def test_alias_match_merges_variant():
    store = FakeLearningStore()
    store.upsert_broker(name="Jones Lang LaSalle", confirmed_by="u")
    store.record_broker_correction(
        alias="JLL", canonical_name="Jones Lang LaSalle", confirmed_by="u"
    )
    result = resolve_broker("JLL", store)
    assert result.broker_name == "Jones Lang LaSalle"
    assert result.status == "alias"


def test_high_similarity_auto_merges():
    """Levenshtein ratio >= 0.85 to an existing canonical name → auto-merge."""
    store = FakeLearningStore()
    store.upsert_broker(name="Cushman & Wakefield", confirmed_by="u")
    result = resolve_broker("Cushman and Wakefield", store)
    assert result.status == "alias"
    assert result.broker_name == "Cushman & Wakefield"


def test_medium_similarity_surfaces_ambiguous():
    """Ratio in [0.60, 0.85) → surface for user confirmation."""
    store = FakeLearningStore()
    store.upsert_broker(name="Colliers International", confirmed_by="u")
    result = resolve_broker("Colliers Retail", store)
    assert result.status == "ambiguous"
    # candidate_name carries the best existing match for UI display
    assert result.candidate_name == "Colliers International"
    assert result.broker_name == "Colliers Retail"


def test_low_similarity_returns_new():
    store = FakeLearningStore()
    store.upsert_broker(name="CBRE", confirmed_by="u")
    result = resolve_broker("Marcus & Millichap", store)
    assert result.status == "new"
    assert result.broker_name == "Marcus & Millichap"


def test_none_input_returns_missing():
    store = FakeLearningStore()
    result = resolve_broker(None, store)
    assert result.status == "missing"
    assert result.broker_name is None
```

- [ ] **Step 2:** Implement `engine/brokers.py`.

```python
# engine/brokers.py
from dataclasses import dataclass
from typing import Optional, Literal

from rapidfuzz import fuzz

from learning.protocol import LearningStore


AUTO_MERGE_THRESHOLD = 85  # rapidfuzz.ratio 0-100
AMBIGUOUS_THRESHOLD = 60


@dataclass
class BrokerResolution:
    status: Literal["matched", "alias", "ambiguous", "new", "missing"]
    broker_name: Optional[str]  # the name to use for downstream linking
    broker_id: Optional[int]
    candidate_name: Optional[str] = None  # best match when ambiguous, for UI


def resolve_broker(extracted_name: Optional[str], store: LearningStore) -> BrokerResolution:
    if not extracted_name or not extracted_name.strip():
        return BrokerResolution(status="missing", broker_name=None, broker_id=None)

    candidate = extracted_name.strip()

    # 1) Exact/alias lookup via store.
    record = store.find_broker_by_alias(candidate)
    if record is not None:
        if record["canonical_name"].lower() == candidate.lower():
            return BrokerResolution(
                status="matched",
                broker_name=record["canonical_name"],
                broker_id=record["id"],
            )
        return BrokerResolution(
            status="alias",
            broker_name=record["canonical_name"],
            broker_id=record["id"],
        )

    # 2) Similarity scan over all known brokers.
    best = None
    best_score = 0
    for known in store.find_all_brokers():
        score = fuzz.ratio(candidate.lower(), known["canonical_name"].lower())
        if score > best_score:
            best_score = score
            best = known

    if best is not None and best_score >= AUTO_MERGE_THRESHOLD:
        # Auto-merge: record the alias and return matched.
        return BrokerResolution(
            status="alias",
            broker_name=best["canonical_name"],
            broker_id=best["id"],
            candidate_name=best["canonical_name"],
        )

    if best is not None and best_score >= AMBIGUOUS_THRESHOLD:
        return BrokerResolution(
            status="ambiguous",
            broker_name=candidate,
            broker_id=None,
            candidate_name=best["canonical_name"],
        )

    return BrokerResolution(status="new", broker_name=candidate, broker_id=None)
```

**Note:** `find_all_brokers()` is a new Protocol method — add it to `learning/protocol.py`:

```python
def find_all_brokers(self) -> list[dict]:
    """Return all brokers as [{id, canonical_name, aliases}]. Used for fuzzy scans."""
    ...
```

Implement in `FakeLearningStore` and `SqliteLearningStore` (trivial: iterate `_brokers` / `SELECT *`).

Add `rapidfuzz>=3.0.0` to `requirements.txt` in Task 0.1 — go back and append it to the pip install list now.

- [ ] **Step 3:** If `find_broker_by_alias` isn't implemented on the store backends from Chunk 2, add it now:

```python
# learning/fakes.py — inside FakeLearningStore
def find_broker_by_alias(self, name: str):
    name_lower = (name or "").strip().lower()
    if not name_lower:
        return None
    # direct canonical match
    for bid, rec in self._brokers.items():
        if rec["canonical_name"].lower() == name_lower:
            return {"id": bid, **rec}
    # alias match
    for alias, bid in self._broker_aliases.items():
        if alias.lower() == name_lower:
            return {"id": bid, **self._brokers[bid]}
    return None
```

```python
# learning/store.py — inside SqliteLearningStore
def find_broker_by_alias(self, name: str):
    if not name:
        return None
    with self._session() as s:
        row = s.query(Broker).filter(
            (func.lower(Broker.canonical_name) == name.strip().lower())
        ).first()
        if row:
            return {"id": row.id, "canonical_name": row.canonical_name}
        # alias lookup (aliases stored as a JSON array column on Broker)
        rows = s.query(Broker).all()
        for row in rows:
            aliases = row.aliases or []
            if any(a.lower() == name.strip().lower() for a in aliases):
                return {"id": row.id, "canonical_name": row.canonical_name}
    return None
```

- [ ] **Step 4:** Run tests.

```bash
pytest tests/test_broker_resolve.py -v
```

Expected: 4 passed.

- [ ] **Step 5:** Commit.

```bash
git add engine/brokers.py learning/fakes.py learning/store.py tests/test_broker_resolve.py
git commit -m "engine: add broker resolution with alias merging"
```

---

### Task 6.3: Hook broker detection into upload flow

**Files:**
- Modify: `engine/pipeline.py` — add `detect_broker_stage`
- Modify: `app.py` — call it once per upload, surface `BrokerResolution` in session state for UI confirmation

- [ ] **Step 1:** Add the stage.

```python
# engine/pipeline.py — append
from engine.brokers import resolve_broker, BrokerResolution
from engine.openai_client import extract_broker


def detect_broker_stage(
    sample_text: str, filename: str, store: LearningStore
) -> BrokerResolution:
    extracted = extract_broker(sample_text=sample_text, filename=filename)
    return resolve_broker(extracted.get("broker"), store)
```

- [ ] **Step 2:** Wire into `app.py` upload handler. After the file is loaded but before the mapping UI renders, call:

```python
# app.py — after robust_load_file_segmented, before mapping widgets
from engine.pipeline import detect_broker_stage

if "broker_resolution" not in st.session_state:
    sample_text = "\n".join(
        " ".join(str(c) for c in segment.cleaned_df.columns) for segment in segments[:3]
    )
    st.session_state["broker_resolution"] = detect_broker_stage(
        sample_text=sample_text,
        filename=uploaded_file.name,
        store=learning_store,
    )
```

The parallel UI tab will pick this up from session state and render a confirmation widget. We do NOT render the widget here.

- [ ] **Step 3:** Smoke test — upload `sample comp files/JLL - DFW Industrial Sales Comps - Rockwall.pdf` locally and verify `st.session_state["broker_resolution"].broker_name == "JLL"` (print via `st.write` temporarily if needed, then remove the print).

- [ ] **Step 4:** Commit.

```bash
git add engine/pipeline.py app.py
git commit -m "app: detect broker on upload and surface resolution for UI"
```

---

**End of Chunk 7.**

---

## Chunk 8: Phase 7 — Vision PDF Extraction

**Goal:** PDFs bypass the Excel mapping pipeline entirely. `pdf2image` rasterizes each page, GPT-4o vision extracts tabular rows into our schema directly, and results flow through geocoding and DB save like any other file. Identity mapping (no fingerprinting — the LLM outputs schema-shaped rows).

---

### Task 7.1: PDF rasterizer + vision prompt

**Files:**
- Create: `engine/vision_pdf.py`
- Create: `tests/test_vision_pdf.py`
- Reference PDFs: `sample comp files/BOV - Discovery Hills Commerce Center.pdf`, `JLL - DFW Industrial Sales Comps - Rockwall.pdf`, `BPO - Spring Stuebner Rd. 7901.pdf`

- [ ] **Step 1:** Write failing test with mocked vision response.

```python
# tests/test_vision_pdf.py
from unittest.mock import MagicMock, patch
import pandas as pd
from engine.vision_pdf import extract_pdf_to_rows


def test_extract_pdf_returns_dataframe_from_vision_response():
    fake_resp = MagicMock()
    fake_resp.choices = [MagicMock(message=MagicMock(content=(
        '{"file_type": "sale", "rows": ['
        '{"property_name": "X", "sale_price": 1000000, "sf": 5000, '
        '"sale_date": "2024-01-01", "address": "123 A St, Houston, TX"}'
        ']}'
    )))]

    with patch("engine.vision_pdf._client") as mock_client, \
         patch("engine.vision_pdf.convert_from_path") as mock_convert:
        mock_convert.return_value = [MagicMock()]  # one fake page image
        mock_client.return_value.chat.completions.create.return_value = fake_resp
        df, file_type = extract_pdf_to_rows("fake.pdf")

    assert file_type == "sale"
    assert len(df) == 1
    assert df.iloc[0]["property_name"] == "X"


def test_extract_pdf_handles_multipage_and_concats():
    page_resp_1 = MagicMock()
    page_resp_1.choices = [MagicMock(message=MagicMock(content=(
        '{"file_type": "lease", "rows": ['
        '{"property_name": "A", "rent_psf": 18, "sf": 1000, "lease_date": "2024-01-01"}]}'
    )))]
    page_resp_2 = MagicMock()
    page_resp_2.choices = [MagicMock(message=MagicMock(content=(
        '{"file_type": "lease", "rows": ['
        '{"property_name": "B", "rent_psf": 19, "sf": 2000, "lease_date": "2024-02-01"}]}'
    )))]

    with patch("engine.vision_pdf._client") as mock_client, \
         patch("engine.vision_pdf.convert_from_path") as mock_convert:
        mock_convert.return_value = [MagicMock(), MagicMock()]
        mock_client.return_value.chat.completions.create.side_effect = [page_resp_1, page_resp_2]
        df, file_type = extract_pdf_to_rows("fake.pdf")

    assert file_type == "lease"
    assert len(df) == 2
    assert set(df["property_name"]) == {"A", "B"}
```

- [ ] **Step 2:** Implement `engine/vision_pdf.py`.

```python
# engine/vision_pdf.py
"""PDF → schema rows via GPT-4o vision.

Assumes poppler-utils is installed (Dockerfile already adds it in Task 0.2).
"""
from __future__ import annotations

import base64
import io
import json
import hashlib
from typing import Optional

import pandas as pd
from pdf2image import convert_from_path

from engine.openai_client import _client


VISION_PROMPT = (
    "This is one page of a commercial real estate comp file. Extract every row "
    "of tabular data into JSON. First, determine whether the page shows LEASE "
    "comps or SALE comps. Then, for each row, return these fields (null for "
    "missing):\n"
    "  LEASE: property_name, tenant, rent_psf, rate_basis, sf, lease_date, "
    "address, city, state, zip, lease_type, term_months\n"
    "  SALE: property_name, sale_price, sf, psf, sale_date, address, city, "
    "state, zip, buyer, seller, cap_rate\n"
    "Return strictly: {\"file_type\": \"lease\"|\"sale\", \"rows\": [...]}. "
    "Do not include any other commentary."
)


def _encode_image(pil_image) -> str:
    buf = io.BytesIO()
    pil_image.save(buf, format="PNG")
    return base64.b64encode(buf.getvalue()).decode("utf-8")


def _pdf_content_hash(pdf_path: str) -> str:
    h = hashlib.sha256()
    with open(pdf_path, "rb") as fh:
        for chunk in iter(lambda: fh.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def extract_pdf_to_rows(pdf_path: str, max_pages: Optional[int] = None) -> tuple[pd.DataFrame, str]:
    pages = convert_from_path(pdf_path, dpi=200)
    if max_pages:
        pages = pages[:max_pages]

    all_rows: list[dict] = []
    file_type: Optional[str] = None

    for page in pages:
        b64 = _encode_image(page)
        resp = _client().chat.completions.create(
            model="gpt-4o",
            messages=[{
                "role": "user",
                "content": [
                    {"type": "text", "text": VISION_PROMPT},
                    {"type": "image_url",
                     "image_url": {"url": f"data:image/png;base64,{b64}"}},
                ],
            }],
            response_format={"type": "json_object"},
            temperature=0,
            max_tokens=4000,
        )
        try:
            parsed = json.loads(resp.choices[0].message.content)
        except json.JSONDecodeError:
            continue
        if file_type is None:
            file_type = parsed.get("file_type", "lease")
        all_rows.extend(parsed.get("rows", []))

    df = pd.DataFrame(all_rows)
    return df, (file_type or "lease")
```

- [ ] **Step 3:** Run tests.

```bash
pytest tests/test_vision_pdf.py -v
```

Expected: 2 passed.

- [ ] **Step 4:** Manual smoke test on one real PDF (consumes OpenAI quota — skip if running low).

```bash
python -c "
from engine.vision_pdf import extract_pdf_to_rows
df, ft = extract_pdf_to_rows('sample comp files/JLL - DFW Industrial Sales Comps - Rockwall.pdf', max_pages=1)
print('file_type:', ft)
print(df.head())
"
```

Expected: prints a DataFrame with sale-comp-shaped columns.

- [ ] **Step 5:** Commit.

```bash
git add engine/vision_pdf.py tests/test_vision_pdf.py
git commit -m "engine: add GPT-4o vision PDF extractor"
```

---

### Task 7.2: Integrate vision PDF into upload flow

**Files:**
- Modify: `engine/pipeline.py` — add `run_vision_pdf_stage` that returns a `SegmentResult` shaped like Excel segments (so downstream code is uniform)
- Modify: `app.py` — dispatch by file extension

- [ ] **Step 1:** Add the stage.

```python
# engine/pipeline.py — append
from engine.vision_pdf import extract_pdf_to_rows, _pdf_content_hash
from engine.types import SegmentResult, Fingerprint, MappingResult


def run_vision_pdf_stage(pdf_path: str, filename: str) -> SegmentResult:
    df, file_type = extract_pdf_to_rows(pdf_path)
    pdf_hash = _pdf_content_hash(pdf_path)

    # Identity mapping: every column is already schema-shaped, so raw=target.
    mappings = {c: c for c in df.columns}

    # PDFs don't share header templates the way Excel files do — each PDF is
    # unique, so raw_hash and header_set_hash both collapse to the content hash.
    # This intentionally prevents Tier 2 fuzzy matching on PDFs.
    fp = Fingerprint(
        raw_hash=pdf_hash,
        header_set_hash=pdf_hash,
        headers=list(df.columns),
        normalized_headers=list(df.columns),
        file_type=file_type,
        filename=filename,
        sheet_name=None,
    )
    return SegmentResult(
        segment_key=f"{filename}::pdf",
        fingerprint=fp,
        mapping_result=MappingResult(
            fingerprint=fp,
            mappings=mappings,
            confidence={c: 1.0 for c in mappings},
            source="vision_pdf",
            similarity=1.0,
            cleaned_df=df,
        ),
        cleaned_df=df,
    )
```

- [ ] **Step 2:** In `app.py` upload handler, dispatch on extension:

```python
# app.py — upload handler
from engine.pipeline import run_vision_pdf_stage, run_mapping_stage

if uploaded_file.name.lower().endswith(".pdf"):
    with tempfile.NamedTemporaryFile(delete=False, suffix=".pdf") as tmp:
        tmp.write(uploaded_file.getbuffer())
        tmp_path = tmp.name
    segment = run_vision_pdf_stage(tmp_path, uploaded_file.name)
    segments = [segment]
else:
    # existing Excel path
    ...
```

- [ ] **Step 3:** Commit.

```bash
git add engine/pipeline.py app.py
git commit -m "app: dispatch PDFs through vision extractor"
```

---

**End of Chunk 8.**

---

## Chunk 9: Phase 8/9 — Seed Tooling + Accuracy Regression Test

**Goal:** Ship bootstrap tools that populate the learning store from the 16 committed sample files and from JSON seed files, so a fresh clone starts with useful knowledge. Then add a regression test that runs every sample file through the new pipeline and asserts the mapping F1 score against a labeled ground-truth file does not regress below a threshold.

---

### Task 8.1: Seed from samples

**Files:**
- Create: `tools/seed_from_samples.py`

- [ ] **Step 1:** Write the tool.

```python
# tools/seed_from_samples.py
"""Walk every file in `sample comp files/` and, for those we have ground-truth
mappings for in `learning_data/ground_truth/<filename>.json`, record them
into the learning store.

Idempotent — safe to re-run. Uses SqliteLearningStore at `learning_local.db`
by default; override with LEARNING_DB_URL.
"""
from __future__ import annotations

import json
import os
import pathlib
import sys

sys.path.insert(0, ".")

from engine.loaders import robust_load_file_segmented, get_sheet_names
from engine.mapping import classify_file_type
from engine.fingerprint import compute_fingerprint
from learning.store import SqliteLearningStore


SAMPLE_DIR = pathlib.Path("sample comp files")
GROUND_TRUTH_DIR = pathlib.Path("learning_data/ground_truth")


def main():
    db_url = os.environ.get("LEARNING_DB_URL", "sqlite:///learning_local.db")
    store = SqliteLearningStore(engine_url=db_url)

    count = 0
    for path in sorted(SAMPLE_DIR.iterdir()):
        if path.suffix.lower() not in {".xlsx", ".xls"}:
            continue

        gt_path = GROUND_TRUTH_DIR / f"{path.stem}.json"
        if not gt_path.exists():
            print(f"skip (no ground truth): {path.name}")
            continue

        with gt_path.open() as fh:
            ground_truth = json.load(fh)

        for sheet in get_sheet_names(str(path)):
            segments = robust_load_file_segmented(str(path), sheet_name=sheet)
            for seg_idx, seg_df in enumerate(segments):
                segment_key = f"{sheet}::{seg_idx}"
                if segment_key not in ground_truth:
                    continue
                mappings = ground_truth[segment_key]["mappings"]
                headers = [str(c) for c in seg_df.columns]
                file_type = classify_file_type(headers, filename=path.name, sheet_name=sheet)
                fp = compute_fingerprint(headers, path.name, sheet, file_type)
                store.record_accepted_mapping(
                    fingerprint=fp, mappings=mappings, confirmed_by="seed"
                )
                count += 1
                print(f"seeded {path.name}::{segment_key}")

    print(f"\nTotal fingerprints seeded: {count}")


if __name__ == "__main__":
    main()
```

- [ ] **Step 2:** Create the ground-truth directory with a template README.

```bash
mkdir -p learning_data/ground_truth
```

Create `learning_data/ground_truth/README.md`:

```markdown
# Ground Truth Mappings

One JSON file per sample spreadsheet. Filename = sample file stem (no extension).

Schema:
```json
{
  "<sheet_name>::<segment_index>": {
    "mappings": {
      "<raw header>": "<target_column>",
      ...
    }
  }
}
```

When building new ground-truth files, first run the sample through the CURRENT engine interactively, confirm the mapping, and copy the accepted dict into the JSON file.
```

**Note on scope:** We do NOT prepopulate ground-truth JSON for all 16 files in this plan — that's manual labeling work the user should do incrementally during local testing. The seed tool will gracefully skip files without ground truth. At minimum, before merging to main, label 3-5 high-value files (JLL MarketSphere formats, Harbor Capital format, DFW Comp Set).

- [ ] **Step 3:** Smoke test — run the tool with no ground truth files present.

```bash
python tools/seed_from_samples.py
```

Expected: prints "skip" for every file, ends with "Total fingerprints seeded: 0". Tool is idempotent and non-destructive.

- [ ] **Step 4:** Commit.

```bash
git add tools/seed_from_samples.py learning_data/ground_truth/README.md
git commit -m "tools: add seed_from_samples for bootstrapping learning store"
```

---

### Task 8.2: Rebuild from JSON seed

**Files:**
- Create: `tools/rebuild_learning_from_seed.py`

- [ ] **Step 1:** Write the tool.

```python
# tools/rebuild_learning_from_seed.py
"""Reconstruct the learning store from committed JSON seed files in
learning_data/seed/. Used when spinning up a fresh DB (local or production)
that should start from a known-good state.

Idempotent on upsert-shaped methods.
"""
import os
import sys
sys.path.insert(0, ".")

from learning.store import SqliteLearningStore


def main():
    seed_dir = os.environ.get("LEARNING_SEED_DIR", "learning_data/seed")
    db_url = os.environ.get("LEARNING_DB_URL", "sqlite:///learning_local.db")
    store = SqliteLearningStore(engine_url=db_url)
    store.load_seed(seed_dir)
    print(f"loaded seed from {seed_dir} into {db_url}")


if __name__ == "__main__":
    main()
```

- [ ] **Step 2:** Create seed directory structure.

```bash
mkdir -p learning_data/seed
touch learning_data/seed/.gitkeep
```

- [ ] **Step 3:** Commit.

```bash
git add tools/rebuild_learning_from_seed.py learning_data/seed/.gitkeep
git commit -m "tools: add rebuild_learning_from_seed for fresh-clone bootstrap"
```

---

### Task 8.3: Accuracy regression test

**Files:**
- Create: `tests/test_accuracy_regression.py`
- Create: `learning_data/ground_truth/<sample>.json` for at least 3 sample files (manual labeling step — user does this during local testing)

**What this guards against:** Any future change that degrades mapping accuracy below a threshold. Runs the full `run_mapping_stage` against labeled files and computes F1 per segment. Fails if average F1 drops below 0.85.

- [ ] **Step 1:** Write the test.

```python
# tests/test_accuracy_regression.py
import json
import pathlib
import pytest
import pandas as pd

from engine.pipeline import run_mapping_stage
from engine.loaders import robust_load_file_segmented, get_sheet_names
from learning.fakes import EmptyLearningStore

SAMPLE_DIR = pathlib.Path("sample comp files")
GT_DIR = pathlib.Path("learning_data/ground_truth")

LABELED_FILES = [p for p in SAMPLE_DIR.iterdir() if (GT_DIR / f"{p.stem}.json").exists()]

MIN_AVG_F1 = 0.85


def _f1(expected: dict, actual: dict) -> float:
    exp_pairs = set(expected.items())
    act_pairs = set(actual.items())
    if not exp_pairs and not act_pairs:
        return 1.0
    if not exp_pairs or not act_pairs:
        return 0.0
    tp = len(exp_pairs & act_pairs)
    precision = tp / len(act_pairs) if act_pairs else 0.0
    recall = tp / len(exp_pairs) if exp_pairs else 0.0
    if precision + recall == 0:
        return 0.0
    return 2 * precision * recall / (precision + recall)


@pytest.mark.skipif(not LABELED_FILES, reason="no labeled sample files yet")
def test_mapping_accuracy_regression():
    store = EmptyLearningStore()  # cold-start — no learned knowledge
    scores = []
    for path in LABELED_FILES:
        with (GT_DIR / f"{path.stem}.json").open() as fh:
            gt = json.load(fh)
        for sheet in get_sheet_names(str(path)):
            segments = robust_load_file_segmented(str(path), sheet_name=sheet)
            for seg_idx, seg_df in enumerate(segments):
                segment_key = f"{sheet}::{seg_idx}"
                if segment_key not in gt:
                    continue
                expected = gt[segment_key]["mappings"]
                result = run_mapping_stage(
                    seg_df, filename=path.name, sheet_name=sheet, store=store
                )
                scores.append(_f1(expected, result.mappings))

    assert scores, "no labeled segments processed"
    avg = sum(scores) / len(scores)
    print(f"\navg F1 across {len(scores)} labeled segments: {avg:.3f}")
    assert avg >= MIN_AVG_F1, (
        f"mapping accuracy regressed to {avg:.3f} (<{MIN_AVG_F1}). "
        f"Individual scores: {scores}"
    )
```

- [ ] **Step 2:** Run the test with no labels yet.

```bash
pytest tests/test_accuracy_regression.py -v
```

Expected: SKIPPED with "no labeled sample files yet".

- [ ] **Step 3:** Commit.

```bash
git add tests/test_accuracy_regression.py
git commit -m "test: add mapping accuracy regression guard (0.85 F1 floor)"
```

---

### Task 8.4: Final end-to-end local smoke test

- [ ] **Step 1:** Fresh DB.

```bash
rm -f learning_local.db
python -c "from database import ensure_tables; ensure_tables()"
```

- [ ] **Step 2:** Launch Streamlit.

```bash
streamlit run app.py
```

- [ ] **Step 3:** Manual walkthrough with at least four sample files:
  1. Upload `Harbor Capital Sale Comps.xlsx` — accept default mapping, save.
  2. Upload `Arlington Class B Comps.xlsx` — fix any wrong column guesses, save.
  3. Re-upload `Harbor Capital Sale Comps.xlsx` (identical) — confirm fingerprint hits (MappingResult.source == "exact"), no embedding call.
  4. Upload `JLL - DFW Industrial Sales Comps - Rockwall.pdf` — verify vision pipeline runs, rows appear, broker detected as JLL.

Verify learning store state:

```bash
sqlite3 learning_local.db "SELECT COUNT(*), source FROM template_fingerprints GROUP BY source;"
sqlite3 learning_local.db "SELECT raw_header, target_column, hit_count FROM column_mapping_corrections;"
sqlite3 learning_local.db "SELECT canonical_name FROM brokers;"
```

Expected: at least a few fingerprints, at least one correction if you fixed anything manually, JLL in brokers.

- [ ] **Step 4:** Run the full test suite one more time.

```bash
pytest tests/ -v
```

Expected: all green. If anything fails, fix before moving on.

- [ ] **Step 5:** Commit any final fixes.

```bash
git status
# commit whatever remains
```

---

**End of Chunk 9 / End of Plan.**

## Post-Execution Handoff

After Chunk 9 completes:
1. Do NOT push to main.
2. Leave the local Streamlit instance running on `http://localhost:8501`.
3. Notify the user: "Local instance ready at localhost:8501. Upload files, verify behavior, and let me know what to tweak before pushing to main."
4. Iterate on user feedback. Each tweak = new branch commit. Run `pytest tests/` after every change.
5. Only after explicit user sign-off ("ship it" / "push to main"):
   ```bash
   git push origin claude/thirsty-wilbur
   gh pr create --base main --title "Self-learning scraper redesign" ...
   ```

