"""High-level stage orchestrator.

Callers use run_mapping_stage; the legacy process_all_sheets in comp_engine.py
delegates here from Chunk 5 onward.
"""
from __future__ import annotations

from typing import Optional
import pandas as pd

from engine.types import MappingResult
from engine.cleaning import clean_header
from engine.mapping import (
    classify_file_type,
    dedupe_mappings_by_target,
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

import os

from engine.mapping import (
    BASE_OVERRIDES,
    LEASE_OVERRIDES,
    SALE_OVERRIDES,
    _find_override,
)
from engine.llm_mapping import llm_map_columns
from engine.verify_mapping import verify_mapping
from engine.mapping_examples import build_examples

# raw_hash -> (mappings, confidence) — avoids re-paying the LLM for an identical file shape
_LLM_MAPPING_CACHE: dict[str, tuple[dict, dict]] = {}

# Headers verifier-flagged or below this confidence are surfaced for analyst review (not auto-accepted)
_LLM_AUTO_ACCEPT_THRESHOLD = 0.75


def _schema_for(file_type: str) -> dict:
    return LEASE_SCHEMA if file_type.upper() in ("LEASE", "BOTH") else SALE_SCHEMA


def _llm_mapper_enabled() -> bool:
    return os.environ.get("COMP_LLM_MAPPER", "1") != "0"


def _exact_override_mappings(raw_headers: list[str], file_type: str) -> dict[str, str]:
    """Headers that resolve to a target via an EXACT deterministic override (score >= 100)."""
    overrides = dict(BASE_OVERRIDES)
    overrides.update(LEASE_OVERRIDES if file_type in ("LEASE", "BOTH") else SALE_OVERRIDES)
    result: dict[str, str] = {}
    for raw in raw_headers:
        cleaned = clean_header(raw)
        for target in set(overrides.values()):
            if _find_override(cleaned, overrides, target) >= 100.0:
                result[raw] = target
                break
    return result


def _llm_fallback(df, schema, file_type, store, raw_hash):
    """Tier-4 LLM mapping. Returns (out_df, mappings, confidence, source) or None to fall through."""
    if not _llm_mapper_enabled():
        return None

    raw_headers = [str(c) for c in df.columns]

    if raw_hash in _LLM_MAPPING_CACHE:
        mappings, confidence = _LLM_MAPPING_CACHE[raw_hash]
    else:
        # Exact-override prefilter: resolve trivial headers deterministically, send the rest to the LLM
        override_map = _exact_override_mappings(raw_headers, file_type)
        unresolved = [h for h in raw_headers if h not in override_map]
        sample_rows = df[unresolved].head(5).to_dict("records") if unresolved else []
        examples = build_examples(store, file_type)
        try:
            llm = llm_map_columns(unresolved, sample_rows, schema, file_type, examples)
        except Exception:
            return None  # fall through to embeddings/heuristic

        mappings = dict(override_map)
        mappings.update(llm["mappings"])
        confidence = {h: 1.0 for h in override_map}
        confidence.update(llm["confidence"])

        verdict = verify_mapping(llm["mappings"], sample_rows, schema)
        for header, adj in verdict["adjusted_confidence"].items():
            confidence[header] = adj
        _LLM_MAPPING_CACHE[raw_hash] = (mappings, confidence)

    mappings = dedupe_mappings_by_target(mappings, raw_headers, confidence)
    out_df = _apply_mappings(df, mappings)
    source = "llm+corrections" if build_examples(store, file_type) else "llm"
    return out_df, mappings, confidence, source


def run_mapping_stage(
    df: pd.DataFrame,
    filename: str,
    sheet_name: Optional[str],
    store,
    broker_name: Optional[str] = None,
) -> MappingResult:
    """Classify → fingerprint → tier1 → tier2 → tier3 → embedding fallback.

    Returns a MappingResult whose mappings dict is {raw_header: target_schema_col}.
    """
    raw_headers = [str(c) for c in df.columns]
    file_type = classify_file_type(raw_headers, filename=filename, sheet_name=sheet_name)
    schema = _schema_for(file_type)
    fp = compute_fingerprint(raw_headers, filename, sheet_name, file_type)

    # Tier 1: exact hash match
    hit = tier1_exact_lookup(store, fp)
    if hit is not None:
        mappings = dedupe_mappings_by_target(
            _filter_to_present(hit.mappings, raw_headers),
            raw_headers,
        )
        out_df = _apply_mappings(df, mappings)
        return MappingResult(
            fingerprint=fp,
            mappings=mappings,
            confidence={h: hit.confidence for h in mappings},
            source="exact",
            similarity=1.0,
            cleaned_df=out_df,
        )

    # Tier 2: fuzzy Jaccard ≥ 0.80
    hit = tier2_fuzzy_lookup(store, fp, threshold=0.80)
    if hit is not None:
        mappings = dedupe_mappings_by_target(
            _filter_to_present(hit.mappings, raw_headers),
            raw_headers,
        )
        out_df = _apply_mappings(df, mappings)
        return MappingResult(
            fingerprint=fp,
            mappings=mappings,
            confidence={h: hit.similarity for h in mappings},
            source="fuzzy",
            similarity=hit.similarity,
            cleaned_df=out_df,
        )

    # Tier 3: broker Jaccard ≥ 0.60
    if broker_name:
        hit = tier3_broker_lookup(store, fp, broker_name=broker_name, threshold=0.60)
        if hit is not None:
            mappings = dedupe_mappings_by_target(
                _filter_to_present(hit.mappings, raw_headers),
                raw_headers,
            )
            out_df = _apply_mappings(df, mappings)
            return MappingResult(
                fingerprint=fp,
                mappings=mappings,
                confidence={h: hit.similarity for h in mappings},
                source="broker",
                similarity=hit.similarity,
                cleaned_df=out_df,
            )

    # Tier 4: LLM mapper (gpt-4o + few-shot + verifier); falls through on error/kill-switch
    llm_result = _llm_fallback(df, schema, file_type, store, fp.raw_hash)
    if llm_result is not None:
        out_df, mappings, confidence, source = llm_result
        return MappingResult(
            fingerprint=fp,
            mappings=mappings,
            confidence=confidence,
            source=source,
            similarity=0.0,
            cleaned_df=out_df,
        )

    # Fallback: correction-weighted embedding (offline-safe: degrades to heuristic)
    out_df, mappings, confidence = generate_standardized_df_with_hints(
        df, schema, file_type=file_type, store=store
    )
    mapping_source = out_df.attrs.get("mapping_source")
    mappings = dedupe_mappings_by_target(mappings, raw_headers, confidence)
    out_df = _apply_mappings(df, mappings)
    has_corrections = _has_any_corrections(store, file_type, raw_headers)
    source = "embedding+corrections" if has_corrections else "embedding"
    source = mapping_source or source
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
            col = df[raw]
            if isinstance(col, pd.DataFrame):
                col = col.iloc[:, 0]
            out[target] = col
    return out


def _filter_to_present(mappings: dict[str, str], raw_headers: list[str]) -> dict[str, str]:
    present = set(raw_headers)
    return {r: t for r, t in mappings.items() if r in present}


def _has_any_corrections(store, file_type: str, headers: list[str]) -> bool:
    for h in headers:
        if store.get_corrections_for_context(file_type=file_type, raw_header=clean_header(h)):
            return True
    return False


def run_geocoding_stage(
    df: pd.DataFrame,
    address_column: str,
    api_key: str,
    store,
) -> pd.DataFrame:
    """Geocode each row using the learned resolve_geocode pipeline.

    Writes latitude, longitude, geocode_source, and canonical_address columns.
    Uses the openai_client module as the LLM fallback (provides .normalize()).
    """
    from engine.geocoding import resolve_geocode
    from engine import openai_client

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


def run_vision_pdf_stage(
    pdf_path: str,
    filename: str,
    max_pages: int | None = None,
    total_timeout_seconds: int | None = 120,
    raster_timeout_seconds: int | None = 20,
    openai_timeout_seconds: int | None = 45,
    dpi: int = 150,
) -> "SegmentResult":
    """Extract rows from a PDF via GPT-4o vision and return as a SegmentResult.

    Uses identity mapping (every column = schema column, no fingerprinting tier).
    """
    from engine.vision_pdf import extract_pdf_to_rows, _pdf_content_hash
    from engine.types import SegmentResult, Fingerprint, MappingResult

    df, file_type = extract_pdf_to_rows(
        pdf_path,
        max_pages=max_pages,
        total_timeout_seconds=total_timeout_seconds,
        raster_timeout_seconds=raster_timeout_seconds,
        openai_timeout_seconds=openai_timeout_seconds,
        dpi=dpi,
    )
    pdf_hash = _pdf_content_hash(pdf_path)
    mappings = {c: c for c in df.columns}

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


def detect_broker_stage(sample_text: str, filename: str, store) -> "BrokerResolution":
    """Extract broker from file metadata then resolve against the learning store."""
    from engine.brokers import resolve_broker, BrokerResolution
    from engine.openai_client import extract_broker
    extracted = extract_broker(sample_text=sample_text, filename=filename)
    return resolve_broker(extracted.get("broker"), store)
