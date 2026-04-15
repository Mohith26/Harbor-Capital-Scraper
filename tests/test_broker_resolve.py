"""Tests for resolve_broker in engine/brokers.py."""
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
    """rapidfuzz ratio >= 85 to an existing canonical name → auto-merge."""
    store = FakeLearningStore()
    store.upsert_broker(name="Cushman & Wakefield", confirmed_by="u")
    result = resolve_broker("Cushman and Wakefield", store)
    assert result.status == "alias"
    assert result.broker_name == "Cushman & Wakefield"


def test_medium_similarity_surfaces_ambiguous():
    """Ratio in [60, 85) → surface for user confirmation."""
    store = FakeLearningStore()
    store.upsert_broker(name="Colliers International", confirmed_by="u")
    result = resolve_broker("Colliers Retail", store)
    assert result.status == "ambiguous"
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
