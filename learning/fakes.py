"""In-memory LearningStore implementations for testing."""
from __future__ import annotations
from typing import Optional
from engine.types import Fingerprint


class FakeLearningStore:
    def __init__(self):
        self._id_counter = 0
        self._fingerprints: dict[str, dict] = {}  # raw_hash -> record
        self._corrections: dict[tuple, int] = {}   # (file_type, raw_header, target_column) -> hit_count
        self._geocode_aliases: dict[str, dict] = {}  # raw_text -> record
        self._geocode_overrides: dict[str, dict] = {}  # raw_text -> record
        self._brokers: dict[int, dict] = {}         # id -> record
        self._broker_names: dict[str, int] = {}     # canonical_name.lower() -> id
        self._broker_aliases: dict[str, int] = {}   # alias.lower() -> broker_id
        self._pdf_corrections: list[dict] = []

    def _new_id(self) -> int:
        self._id_counter += 1
        return self._id_counter

    # ---- Fingerprints ----
    def get_fingerprint_by_hash(self, fp_hash: str) -> Optional[dict]:
        return self._fingerprints.get(fp_hash)

    def find_fuzzy_fingerprints(self, file_type: str) -> list[dict]:
        return [r for r in self._fingerprints.values() if r["file_type"] == file_type]

    def find_broker_fingerprints(self, broker_name: str, file_type: str) -> list[dict]:
        bid = self._broker_names.get(broker_name.lower())
        if bid is None:
            return []
        return [r for r in self._fingerprints.values()
                if r["file_type"] == file_type and r.get("broker_id") == bid]

    def record_accepted_mapping(self, fingerprint: Fingerprint, mappings: dict[str, str],
                                confirmed_by: str, broker_id: Optional[int] = None) -> None:
        existing = self._fingerprints.get(fingerprint.raw_hash)
        if existing:
            existing["hit_count"] += 1
            existing["mappings"].update(mappings)
            if broker_id is not None:
                existing["broker_id"] = broker_id
        else:
            self._fingerprints[fingerprint.raw_hash] = {
                "raw_hash": fingerprint.raw_hash,
                "file_type": fingerprint.file_type,
                "normalized_headers": list(fingerprint.normalized_headers),
                "mappings": dict(mappings),
                "confidence": 0.5,
                "hit_count": 1,
                "broker_id": broker_id,
                "confirmed_by": confirmed_by,
            }

    # ---- Corrections ----
    def get_corrections_for_context(self, file_type: str, raw_header: str) -> dict[str, int]:
        result = {}
        for (ft, rh, tc), count in self._corrections.items():
            if ft == file_type and rh == raw_header:
                result[tc] = count
        return result

    def upsert_correction(self, file_type: str, raw_header: str,
                          target_column: str, confirmed_by: str) -> None:
        key = (file_type, raw_header, target_column)
        self._corrections[key] = self._corrections.get(key, 0) + 1

    # ---- Geocoding ----
    def get_geocode_override(self, raw_text: str) -> Optional[dict]:
        return self._geocode_overrides.get(raw_text)

    def get_geocode_alias(self, raw_text: str) -> Optional[dict]:
        return self._geocode_aliases.get(raw_text)

    def insert_geocode_alias(self, raw_text: str, canonical_address: str,
                             lat: float, lng: float) -> None:
        existing = self._geocode_aliases.get(raw_text)
        if existing:
            existing["canonical_address"] = canonical_address
            existing["latitude"] = lat
            existing["longitude"] = lng
            existing["hit_count"] += 1
        else:
            self._geocode_aliases[raw_text] = {
                "raw_text": raw_text,
                "canonical_address": canonical_address,
                "latitude": lat,
                "longitude": lng,
                "hit_count": 1,
            }

    def bump_hit_count(self, raw_text: str) -> None:
        if raw_text in self._geocode_aliases:
            self._geocode_aliases[raw_text]["hit_count"] += 1

    def record_geocode_override(self, raw_text: str, override_address: str,
                                lat: float, lng: float, confirmed_by: str) -> None:
        self._geocode_overrides[raw_text] = {
            "raw_text": raw_text,
            "override_address": override_address,
            "formatted_address": override_address,
            "latitude": lat,
            "longitude": lng,
            "confirmed_by": confirmed_by,
        }

    # ---- Brokers ----
    def upsert_broker(self, name: str, confirmed_by: str) -> int:
        key = name.lower()
        if key in self._broker_names:
            return self._broker_names[key]
        bid = self._new_id()
        self._brokers[bid] = {"id": bid, "canonical_name": name, "aliases": [],
                              "confirmed_by": confirmed_by}
        self._broker_names[key] = bid
        return bid

    def find_broker_by_alias(self, name: str) -> Optional[dict]:
        k = name.lower()
        if k in self._broker_names:
            return dict(self._brokers[self._broker_names[k]])
        if k in self._broker_aliases:
            return dict(self._brokers[self._broker_aliases[k]])
        return None

    def find_all_brokers(self) -> list[dict]:
        return [dict(b) for b in self._brokers.values()]

    def record_broker_correction(self, alias: str, canonical_name: str, confirmed_by: str) -> None:
        bid = self.upsert_broker(canonical_name, confirmed_by)
        a_lower = alias.lower()
        broker = self._brokers[bid]
        if a_lower not in broker["aliases"]:
            broker["aliases"].append(a_lower)
        self._broker_aliases[a_lower] = bid

    # ---- PDF ----
    def get_pdf_corrections(self, pdf_hash: str) -> list[dict]:
        return [c for c in self._pdf_corrections if c["pdf_hash"] == pdf_hash]

    def record_pdf_correction(self, pdf_hash: str, page_num: int, row_index: int,
                              field: str, original: str, corrected: str, confirmed_by: str) -> None:
        self._pdf_corrections.append({
            "pdf_hash": pdf_hash, "page_num": page_num, "row_index": row_index,
            "field": field, "original_value": original, "corrected_value": corrected,
            "confirmed_by": confirmed_by,
        })

    def load_seed(self, seed_dir: str) -> None:
        pass  # tests inject data directly


class EmptyLearningStore:
    """All reads return None/empty, all writes are no-ops. For cold-start accuracy testing."""
    def get_fingerprint_by_hash(self, fp_hash): return None
    def find_fuzzy_fingerprints(self, file_type): return []
    def find_broker_fingerprints(self, broker_name, file_type): return []
    def record_accepted_mapping(self, fingerprint, mappings, confirmed_by, broker_id=None): pass
    def get_corrections_for_context(self, file_type, raw_header): return {}
    def upsert_correction(self, file_type, raw_header, target_column, confirmed_by): pass
    def get_geocode_override(self, raw_text): return None
    def get_geocode_alias(self, raw_text): return None
    def insert_geocode_alias(self, raw_text, canonical_address, lat, lng): pass
    def bump_hit_count(self, raw_text): pass
    def record_geocode_override(self, raw_text, override_address, lat, lng, confirmed_by): pass
    def upsert_broker(self, name, confirmed_by): return None
    def find_broker_by_alias(self, name): return None
    def find_all_brokers(self): return []
    def record_broker_correction(self, alias, canonical_name, confirmed_by): pass
    def get_pdf_corrections(self, pdf_hash): return []
    def record_pdf_correction(self, pdf_hash, page_num, row_index, field, original, corrected, confirmed_by): pass
    def load_seed(self, seed_dir): pass
