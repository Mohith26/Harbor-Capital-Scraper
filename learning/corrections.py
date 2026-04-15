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

log = logging.getLogger(__name__)


def persist_with_learning(
    segments: list[SegmentResult],
    final_mappings: dict[str, dict[str, str]],
    edited_dfs: dict[str, pd.DataFrame],
    confirmed_broker: Optional[str],
    geocode_overrides: dict[int, dict],
    store,
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
    correction votes.
    """
    for seg in segments:
        if seg.segment_key not in final_mappings:
            continue
        new_mappings = final_mappings[seg.segment_key]
        edited_df = edited_dfs.get(seg.segment_key)
        if edited_df is None or edited_df.empty:
            continue

        # Re-derive fingerprint from EDITED headers.
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
