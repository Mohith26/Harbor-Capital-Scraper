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


def _schema_for(file_type: str) -> dict:
    return LEASE_SCHEMA if file_type.upper() in ("LEASE", "BOTH") else SALE_SCHEMA


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
        out_df = _apply_mappings(df, hit.mappings)
        return MappingResult(
            fingerprint=fp,
            mappings=hit.mappings,
            confidence={h: hit.confidence for h in hit.mappings},
            source="exact",
            similarity=1.0,
            cleaned_df=out_df,
        )

    # Tier 2: fuzzy Jaccard ≥ 0.80
    hit = tier2_fuzzy_lookup(store, fp, threshold=0.80)
    if hit is not None:
        mappings = _filter_to_present(hit.mappings, raw_headers)
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
            mappings = _filter_to_present(hit.mappings, raw_headers)
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
    has_corrections = _has_any_corrections(store, file_type, raw_headers)
    source = "embedding+corrections" if has_corrections else "embedding"
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
