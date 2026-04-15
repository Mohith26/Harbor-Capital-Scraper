"""Broker resolution: exact/alias/fuzzy matching against the learning store."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Literal

from rapidfuzz import fuzz


AUTO_MERGE_THRESHOLD = 85   # rapidfuzz.ratio 0-100
AMBIGUOUS_THRESHOLD = 60


@dataclass
class BrokerResolution:
    status: Literal["matched", "alias", "ambiguous", "new", "missing"]
    broker_name: Optional[str]   # name to use for downstream linking
    broker_id: Optional[int]
    candidate_name: Optional[str] = None  # best match when ambiguous, for UI


def resolve_broker(extracted_name: Optional[str], store) -> BrokerResolution:
    """Match extracted broker name against the learning store.

    Tier 1 — Exact/alias lookup via store.find_broker_by_alias.
    Tier 2 — Fuzzy scan over all canonical names with rapidfuzz.ratio.
              ≥ AUTO_MERGE_THRESHOLD → status=alias (auto-merge).
              ≥ AMBIGUOUS_THRESHOLD  → status=ambiguous (surface for user).
    Tier 3 — No match → status=new.
    None/empty input → status=missing.
    """
    if not extracted_name or not extracted_name.strip():
        return BrokerResolution(status="missing", broker_name=None, broker_id=None)

    candidate = extracted_name.strip()

    # Tier 1: exact / alias lookup
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

    # Tier 2: similarity scan
    best = None
    best_score = 0
    for known in store.find_all_brokers():
        score = fuzz.ratio(candidate.lower(), known["canonical_name"].lower())
        if score > best_score:
            best_score = score
            best = known

    if best is not None and best_score >= AUTO_MERGE_THRESHOLD:
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
