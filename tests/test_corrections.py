"""Tests for learning/corrections.py — persist_with_learning writeback hook."""
import pandas as pd
import pytest
from engine.types import SegmentResult, Fingerprint, MappingResult
from engine.fingerprint import compute_fingerprint
from learning.fakes import FakeLearningStore
from learning.corrections import persist_with_learning


def test_get_all_corrections_returns_sorted_by_hit_count():
    store = FakeLearningStore()
    store.upsert_correction("LEASE", "asking rate", "rate_psf", "u@test")
    store.upsert_correction("LEASE", "asking rate", "rate_psf", "u@test")  # hit_count=2
    store.upsert_correction("LEASE", "deal sf", "leased_sf", "u@test")     # hit_count=1
    store.upsert_correction("SALE", "pp", "sale_price", "u@test")          # other file_type

    rows = store.get_all_corrections("LEASE")

    assert {"raw_header": "asking rate", "target_column": "rate_psf", "hit_count": 2} in rows
    assert {"raw_header": "deal sf", "target_column": "leased_sf", "hit_count": 1} in rows
    assert all(r["raw_header"] != "pp" for r in rows)  # SALE excluded
    assert [r["hit_count"] for r in rows] == sorted([r["hit_count"] for r in rows], reverse=True)


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

    assert len(saved_rows) == 1
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


def test_persist_uses_preserved_original_mapping_for_corrections():
    store = FakeLearningStore()

    def fake_saver(df):
        return list(range(len(df)))

    seg = _make_segment(
        "Sheet1::0",
        headers=["Property", "Asking Rate"],
        mappings={"Property": "property_name", "Asking Rate": "rent_psf"},
    )
    seg.mapping_result.original_mappings = {"Property": "property_name", "Asking Rate": "sf"}

    persist_with_learning(
        segments=[seg],
        final_mappings={"Sheet1::0": seg.mapping_result.mappings},
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
    """Fingerprint should be based on the edited (renamed) headers, not originals."""
    store = FakeLearningStore()

    def fake_saver(df):
        return [1]

    seg = _make_segment(
        "Sheet1::0",
        headers=["prop", "rate"],
        mappings={"prop": "property_name", "rate": "rent_psf"},
    )
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

    new_fp = compute_fingerprint(
        ["Property Name", "Rent PSF"], "f.xlsx", "Sheet1", "lease"
    )
    assert store.get_fingerprint_by_hash(new_fp.raw_hash) is not None
    # Original (stale) hash was NOT stored
    assert store.get_fingerprint_by_hash(seg.fingerprint.raw_hash) is None


def test_persist_links_broker_id_to_rederived_fingerprint():
    """Confirmed broker must be linked to the re-derived (edited-header) fingerprint."""
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

    new_fp = compute_fingerprint(
        ["Property Name", "Rent PSF"], "f.xlsx", "Sheet1", "lease"
    )
    record = store.get_fingerprint_by_hash(new_fp.raw_hash)
    assert record is not None
    assert record.get("broker_id") is not None
    # No stale record under the pre-edit hash
    assert store.get_fingerprint_by_hash(seg.fingerprint.raw_hash) is None
