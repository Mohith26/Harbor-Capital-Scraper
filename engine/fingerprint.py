"""Fingerprint primitives and tiered lookup for the learning pipeline."""
from __future__ import annotations
import hashlib
from typing import TYPE_CHECKING, Optional

from engine.cleaning import clean_header
from engine.types import Fingerprint, FingerprintMatch

if TYPE_CHECKING:
    from learning.protocol import LearningStore


def _normalize_header(h: str) -> str:
    return clean_header(h)


def _hash(parts: list[str]) -> str:
    joined = "\x00".join(parts)
    return hashlib.sha256(joined.encode("utf-8")).hexdigest()


def header_set(headers: list[str]) -> frozenset[str]:
    """Return frozenset of normalized header strings."""
    return frozenset(_normalize_header(h) for h in headers)


def compute_fingerprint(
    headers: list[str],
    filename: str,
    sheet_name: Optional[str],
    file_type: str,
) -> Fingerprint:
    normalized_ordered = [_normalize_header(h) for h in headers]
    normalized_set_sorted = sorted(set(normalized_ordered))
    return Fingerprint(
        raw_hash=_hash([file_type] + normalized_ordered),
        header_set_hash=_hash([file_type] + normalized_set_sorted),
        headers=list(headers),
        normalized_headers=normalized_ordered,
        file_type=file_type,
        filename=filename,
        sheet_name=sheet_name,
    )


def jaccard_similarity(a: set, b: set) -> float:
    if not a and not b:
        return 1.0
    union = a | b
    if not union:
        return 1.0
    return len(a & b) / len(union)


def tier1_exact_lookup(store: "LearningStore", fp: Fingerprint) -> Optional[FingerprintMatch]:
    record = store.get_fingerprint_by_hash(fp.raw_hash)
    if record is None:
        return None
    return FingerprintMatch(
        source="exact",
        similarity=1.0,
        fingerprint=fp,
        mappings=dict(record.get("mappings", {})),
        confidence=float(record.get("confidence", 1.0)),
        hit_count=int(record.get("hit_count", 1)),
    )


def tier2_fuzzy_lookup(
    store: "LearningStore",
    fp: Fingerprint,
    threshold: float = 0.80,
) -> Optional[FingerprintMatch]:
    candidates = store.find_fuzzy_fingerprints(fp.file_type)
    query_set = set(fp.normalized_headers)
    best_sim = 0.0
    best_record = None
    for record in candidates:
        stored_set = set(record.get("normalized_headers", []))
        sim = jaccard_similarity(query_set, stored_set)
        if sim >= threshold and sim > best_sim:
            best_sim = sim
            best_record = record
    if best_record is None:
        return None
    return FingerprintMatch(
        source="fuzzy",
        similarity=best_sim,
        fingerprint=fp,
        mappings=dict(best_record.get("mappings", {})),
        confidence=float(best_record.get("confidence", best_sim)),
        hit_count=int(best_record.get("hit_count", 1)),
    )


def tier3_broker_lookup(
    store: "LearningStore",
    fp: Fingerprint,
    broker_name: str,
    threshold: float = 0.60,
) -> Optional[FingerprintMatch]:
    candidates = store.find_broker_fingerprints(broker_name, fp.file_type)
    query_set = set(fp.normalized_headers)
    best_sim = 0.0
    best_record = None
    for record in candidates:
        stored_set = set(record.get("normalized_headers", []))
        sim = jaccard_similarity(query_set, stored_set)
        if sim >= threshold and sim > best_sim:
            best_sim = sim
            best_record = record
    if best_record is None:
        return None
    return FingerprintMatch(
        source="broker",
        similarity=best_sim,
        fingerprint=fp,
        mappings=dict(best_record.get("mappings", {})),
        confidence=float(best_record.get("confidence", best_sim)),
        hit_count=int(best_record.get("hit_count", 1)),
    )
