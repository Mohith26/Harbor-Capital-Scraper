"""FakeLearningStore behavior tests — uses NEW Protocol signatures."""
import pytest
from learning.fakes import FakeLearningStore, EmptyLearningStore
from engine.types import Fingerprint


def _fp(raw_hash, headers, file_type="lease"):
    try:
        from engine.cleaning import clean_header
        normalized = [clean_header(h) for h in headers]
    except Exception:
        normalized = [h.lower().strip() for h in headers]
    return Fingerprint(
        raw_hash=raw_hash,
        header_set_hash=raw_hash + "_set",
        headers=headers,
        normalized_headers=normalized,
        file_type=file_type,
        filename="test.xlsx",
        sheet_name="Sheet1",
    )


@pytest.fixture
def fake():
    return FakeLearningStore()


def test_get_fingerprint_returns_none_initially(fake):
    assert fake.get_fingerprint_by_hash("nope") is None


def test_record_accepted_mapping_creates_fingerprint(fake):
    fp = _fp("abc", ["Property", "Rent PSF"])
    fake.record_accepted_mapping(fp, {"Property": "property_name", "Rent PSF": "rent_psf"}, confirmed_by="u@x.com")
    got = fake.get_fingerprint_by_hash("abc")
    assert got is not None
    assert got["mappings"] == {"Property": "property_name", "Rent PSF": "rent_psf"}
    assert got["hit_count"] == 1


def test_record_accepted_mapping_increments_on_duplicate(fake):
    fp = _fp("abc", ["A", "B"])
    fake.record_accepted_mapping(fp, {"A": "a"}, confirmed_by="u")
    fake.record_accepted_mapping(fp, {"A": "a"}, confirmed_by="u")
    assert fake.get_fingerprint_by_hash("abc")["hit_count"] == 2


def test_upsert_correction_increments_hit_count(fake):
    fake.upsert_correction("lease", "size sf", "sf", confirmed_by="u")
    fake.upsert_correction("lease", "size sf", "sf", confirmed_by="u")
    ctx = fake.get_corrections_for_context("lease", "size sf")
    assert ctx.get("sf") == 2


def test_get_corrections_returns_empty_dict_when_none(fake):
    assert fake.get_corrections_for_context("lease", "unknown") == {}


def test_geocode_override_lookup(fake):
    fake.record_geocode_override("123 Main, TX", "123 Main St, Houston, TX", lat=29.7, lng=-95.3, confirmed_by="u")
    got = fake.get_geocode_override("123 Main, TX")
    assert got is not None
    assert got["latitude"] == 29.7


def test_geocode_alias_lookup_and_bump(fake):
    fake.insert_geocode_alias("456 Oak, TX", "456 Oak St, Dallas, TX", lat=32.8, lng=-96.8)
    got = fake.get_geocode_alias("456 Oak, TX")
    assert got is not None
    assert got["longitude"] == -96.8
    fake.bump_hit_count("456 Oak, TX")
    assert fake.get_geocode_alias("456 Oak, TX")["hit_count"] == 2


def test_upsert_broker_returns_id(fake):
    id1 = fake.upsert_broker("JLL", confirmed_by="u")
    id2 = fake.upsert_broker("JLL", confirmed_by="u")
    assert id1 == id2
    assert isinstance(id1, int)


def test_find_broker_by_alias(fake):
    fake.upsert_broker("JLL", confirmed_by="u")
    fake.record_broker_correction("Jones Lang LaSalle", "JLL", "u")
    b = fake.find_broker_by_alias("jones lang lasalle")
    assert b is not None
    assert b["canonical_name"] == "JLL"


def test_find_all_brokers(fake):
    fake.upsert_broker("CBRE", confirmed_by="u")
    fake.upsert_broker("JLL", confirmed_by="u")
    all_b = fake.find_all_brokers()
    names = {b["canonical_name"] for b in all_b}
    assert "CBRE" in names and "JLL" in names


def test_find_fuzzy_fingerprints_returns_all_for_file_type(fake):
    fake.record_accepted_mapping(_fp("h1", ["A", "B", "C"], "lease"), {"A": "a"}, confirmed_by="u")
    fake.record_accepted_mapping(_fp("h2", ["A", "B", "D"], "lease"), {"A": "a"}, confirmed_by="u")
    fake.record_accepted_mapping(_fp("h3", ["X", "Y"], "sale"), {"X": "x"}, confirmed_by="u")
    results = fake.find_fuzzy_fingerprints("lease")
    assert len(results) == 2
    assert all(r["file_type"] == "lease" for r in results)


def test_empty_store_always_returns_none():
    empty = EmptyLearningStore()
    fp = _fp("x", ["A"])
    empty.record_accepted_mapping(fp, {"A": "a"}, confirmed_by="u")
    assert empty.get_fingerprint_by_hash("x") is None
    assert empty.get_geocode_alias("k") is None
    assert empty.get_corrections_for_context("lease", "anything") == {}
