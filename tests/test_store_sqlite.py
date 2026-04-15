"""SqliteLearningStore integration tests — real in-memory SQLite."""
import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from learning.schemas import LearningBase
from learning.store import SqliteLearningStore
from engine.types import Fingerprint


def _fp(raw_hash, headers, file_type="lease"):
    return Fingerprint(
        raw_hash=raw_hash, header_set_hash=raw_hash+"_s",
        headers=headers, normalized_headers=[h.lower() for h in headers],
        file_type=file_type, filename="f.xlsx", sheet_name="S",
    )


@pytest.fixture
def store():
    engine = create_engine("sqlite:///:memory:")
    LearningBase.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    return SqliteLearningStore(Session)


def test_fingerprint_roundtrip(store):
    fp = _fp("h1", ["Property", "Rent PSF"])
    store.record_accepted_mapping(fp, {"Property": "property_name"}, confirmed_by="u")
    got = store.get_fingerprint_by_hash("h1")
    assert got is not None
    assert got["mappings"]["Property"] == "property_name"
    assert got["hit_count"] == 1


def test_fingerprint_idempotent_increments(store):
    fp = _fp("h1", ["A"])
    store.record_accepted_mapping(fp, {"A": "a"}, confirmed_by="u")
    store.record_accepted_mapping(fp, {"A": "a"}, confirmed_by="u")
    assert store.get_fingerprint_by_hash("h1")["hit_count"] == 2


def test_correction_vote_counting(store):
    store.upsert_correction("lease", "size sf", "sf", confirmed_by="u")
    store.upsert_correction("lease", "size sf", "sf", confirmed_by="u")
    ctx = store.get_corrections_for_context("lease", "size sf")
    assert ctx.get("sf") == 2


def test_geocode_override_roundtrip(store):
    store.record_geocode_override("123 Main, TX", "123 Main St, Houston, TX", lat=29.7, lng=-95.3, confirmed_by="u")
    got = store.get_geocode_override("123 Main, TX")
    assert got is not None
    assert got["latitude"] == pytest.approx(29.7)


def test_broker_upsert_returns_same_id(store):
    id1 = store.upsert_broker("JLL", confirmed_by="u")
    id2 = store.upsert_broker("JLL", confirmed_by="u")
    assert id1 == id2

def test_find_all_brokers(store):
    store.upsert_broker("JLL", confirmed_by="u")
    store.upsert_broker("CBRE", confirmed_by="u")
    names = {b["canonical_name"] for b in store.find_all_brokers()}
    assert "JLL" in names and "CBRE" in names
