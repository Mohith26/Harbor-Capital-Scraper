"""Post-mapping audit layer — surfaces data-vs-mapping contradictions to the analyst.

Runs after the mapping stage on EVERY segment (all tiers), before the analyst
edits the preview. Advisory only: degrades to an empty list when disabled, when
there is no API key, or on any error.
"""
from __future__ import annotations

import os

from engine.mapping import LEASE_SCHEMA, SALE_SCHEMA
from engine.verify_mapping import verify_mapping

_MAX_SAMPLE_ROWS = 5


def _audit_enabled() -> bool:
    return os.environ.get("COMP_MAPPING_AUDIT", "1") != "0"


def _schema_for(file_type: str) -> dict:
    return LEASE_SCHEMA if (file_type or "").upper() in ("LEASE", "BOTH") else SALE_SCHEMA


def audit_segment(
    mappings: dict[str, str],
    sample_rows: list[dict],
    file_type: str,
) -> list[dict]:
    """Audit one segment's mapping against its sample values.

    Returns a list of {"header", "reason", "suggested_field"} dicts (advisory).
    Empty list when disabled, when there is nothing to check, or on any error.
    """
    if not _audit_enabled() or not mappings:
        return []

    schema = _schema_for(file_type)
    try:
        verdict = verify_mapping(mappings, list(sample_rows or [])[:_MAX_SAMPLE_ROWS], schema)
    except Exception:
        return []

    valid_targets = set(schema.keys())
    flags: list[dict] = []
    for f in verdict.get("flags") or []:
        header = f.get("header", "")
        if header not in mappings:
            continue
        suggested = f.get("suggested_field")
        if suggested not in valid_targets or suggested == mappings.get(header):
            suggested = None
        flags.append({
            "header": header,
            "reason": f.get("reason", ""),
            "suggested_field": suggested,
        })
    return flags
