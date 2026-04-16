"""LearningStore protocol — structural type all implementations must conform to.

All concrete stores (Supabase, SQLite, Fake, Empty) must implement every method
with EXACTLY the signatures below. Downstream engine code calls these methods
with these exact keyword arguments — do not deviate.
"""
from __future__ import annotations
from typing import Protocol, Optional
from engine.types import Fingerprint


class LearningStore(Protocol):
    # ---- Fingerprints / templates ----
    def get_fingerprint_by_hash(self, fp_hash: str) -> Optional[dict]:
        """Return {"mappings": dict, "normalized_headers": list[str], "confidence": float,
        "hit_count": int, "broker_id": Optional[int]} or None."""
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
        """Upsert. ON CONFLICT (raw_hash) DO UPDATE hit_count += 1, mappings merged."""
        ...

    # ---- Correction votes ----
    def get_corrections_for_context(
        self, file_type: str, raw_header: str
    ) -> dict[str, int]:
        """Return {target_column: hit_count}. Empty dict when none."""
        ...

    def upsert_correction(
        self,
        file_type: str,
        raw_header: str,
        target_column: str,
        confirmed_by: str,
    ) -> None:
        """Atomic: increment hit_count on (file_type, raw_header, target_column)."""
        ...

    # ---- Geocoding ----
    def get_geocode_override(self, raw_text: str) -> Optional[dict]:
        """Return {"formatted_address": str, "latitude": float, "longitude": float} or None."""
        ...

    def get_geocode_alias(self, raw_text: str) -> Optional[dict]:
        """Return cached geocode result or None."""
        ...

    def insert_geocode_alias(
        self, raw_text: str, canonical_address: str, lat: float, lng: float
    ) -> None:
        """Atomic upsert: on conflict update canonical/lat/lng and bump hit_count."""
        ...

    def bump_hit_count(self, raw_text: str) -> None: ...

    def record_geocode_override(
        self, raw_text: str, override_address: str, lat: float, lng: float, confirmed_by: str
    ) -> None:
        """User-confirmed override that shadows alias cache forever."""
        ...

    # ---- Brokers ----
    def upsert_broker(self, name: str, confirmed_by: str) -> Optional[int]:
        """Return broker_id, or None for no-op stores. Atomic insert if new, else return existing id."""
        ...

    def find_broker_by_alias(self, name: str) -> Optional[dict]:
        """Return {"id": int, "canonical_name": str, "aliases": list[str]} or None.
        Case-insensitive match on canonical name OR any alias."""
        ...

    def find_all_brokers(self) -> list[dict]:
        """Return all brokers as [{"id": int, "canonical_name": str, "aliases": list[str]}].
        Used for rapidfuzz similarity scans."""
        ...

    def record_broker_correction(self, alias: str, canonical_name: str, confirmed_by: str) -> None:
        """Record alias → canonical_name mapping. Find-or-create canonical broker,
        append alias to its aliases list, commit."""
        ...

    # ---- PDF corrections ----
    def get_pdf_corrections(self, pdf_hash: str) -> list[dict]: ...

    def record_pdf_correction(
        self, pdf_hash: str, page_num: int, row_index: int, field: str,
        original: str, corrected: str, confirmed_by: str
    ) -> None: ...

    # ---- Seed bootstrap ----
    def load_seed(self, seed_dir: str) -> None:
        """Load JSON seed files. Idempotent."""
        ...
