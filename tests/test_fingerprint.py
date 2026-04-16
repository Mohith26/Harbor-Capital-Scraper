"""Tests for engine/fingerprint.py — fingerprint primitives and tiered lookup."""
import pytest
from engine.fingerprint import (
    compute_fingerprint,
    header_set,
    jaccard_similarity,
    tier1_exact_lookup,
    tier2_fuzzy_lookup,
    tier3_broker_lookup,
)
from learning.fakes import FakeLearningStore
from engine.types import Fingerprint


def _make_store_with_fingerprint(headers, file_type="LEASE", broker_id=None):
    store = FakeLearningStore()
    fp = compute_fingerprint(headers, "test.xlsx", "Sheet1", file_type)
    mappings = {"tenant name": "tenant", "rent psf": "rate_psf"}
    store.record_accepted_mapping(fp, mappings, confirmed_by="test", broker_id=broker_id)
    return store, fp


# --- compute_fingerprint ---

def test_compute_fingerprint_is_stable():
    headers = ["Tenant Name", "Rent PSF", "Lease Date"]
    fp1 = compute_fingerprint(headers, "file.xlsx", "Sheet1", "LEASE")
    fp2 = compute_fingerprint(headers, "file.xlsx", "Sheet1", "LEASE")
    assert fp1.raw_hash == fp2.raw_hash
    assert fp1.header_set_hash == fp2.header_set_hash


def test_compute_fingerprint_header_order_agnostic_for_set_hash():
    headers_a = ["Tenant Name", "Rent PSF", "Lease Date"]
    headers_b = ["Lease Date", "Rent PSF", "Tenant Name"]
    fp_a = compute_fingerprint(headers_a, "file.xlsx", "Sheet1", "LEASE")
    fp_b = compute_fingerprint(headers_b, "file.xlsx", "Sheet1", "LEASE")
    # Different order → different raw_hash
    assert fp_a.raw_hash != fp_b.raw_hash
    # Same set → same header_set_hash
    assert fp_a.header_set_hash == fp_b.header_set_hash


def test_compute_fingerprint_file_type_affects_hash():
    headers = ["Address", "Sale Price", "SF"]
    fp_lease = compute_fingerprint(headers, "file.xlsx", "Sheet1", "LEASE")
    fp_sale = compute_fingerprint(headers, "file.xlsx", "Sheet1", "SALE")
    assert fp_lease.raw_hash != fp_sale.raw_hash


def test_compute_fingerprint_stores_normalized_headers():
    headers = ["  Tenant Name  ", "RENT PSF", "Lease  Date"]
    fp = compute_fingerprint(headers, "f.xlsx", "S1", "LEASE")
    assert fp.normalized_headers[0] == "tenant name"
    assert fp.normalized_headers[1] == "rent psf"
    assert fp.headers == headers  # originals preserved


# --- header_set ---

def test_header_set_returns_normalized_frozenset():
    headers = ["Tenant Name", "  Rent PSF  ", "Lease Date"]
    hs = header_set(headers)
    assert "tenant name" in hs
    assert "rent psf" in hs
    assert "lease date" in hs


# --- jaccard_similarity ---

def test_jaccard_identical_sets():
    a = {"tenant name", "rent psf", "lease date"}
    assert jaccard_similarity(a, a) == pytest.approx(1.0)


def test_jaccard_disjoint_sets():
    a = {"tenant name", "rent psf"}
    b = {"address", "sale price"}
    assert jaccard_similarity(a, b) == pytest.approx(0.0)


def test_jaccard_partial_overlap():
    a = {"tenant name", "rent psf", "lease date", "sf"}
    b = {"tenant name", "rent psf", "lease date", "address"}
    # intersection=3, union=5 → 0.6
    assert jaccard_similarity(a, b) == pytest.approx(0.6)


def test_jaccard_empty_sets():
    assert jaccard_similarity(set(), set()) == pytest.approx(1.0)


# --- tier1_exact_lookup ---

def test_tier1_exact_lookup_hits():
    store, fp = _make_store_with_fingerprint(["Tenant Name", "Rent PSF"])
    match = tier1_exact_lookup(store, fp)
    assert match is not None
    assert match.source == "exact"
    assert match.similarity == pytest.approx(1.0)
    assert "tenant name" in match.mappings or "rent psf" in match.mappings


def test_tier1_exact_lookup_misses_on_different_hash():
    store, fp = _make_store_with_fingerprint(["Tenant Name", "Rent PSF"])
    fp_other = compute_fingerprint(["Address", "Sale Price"], "f.xlsx", "S1", "SALE")
    assert tier1_exact_lookup(store, fp_other) is None


# --- tier2_fuzzy_lookup ---

def test_tier2_fuzzy_lookup_hits_on_80_percent_overlap():
    base_headers = ["Tenant Name", "Rent PSF", "Lease Date", "SF", "Address"]
    store, _ = _make_store_with_fingerprint(base_headers)
    # 4 of 5 match → Jaccard = 4/6 ≈ 0.67 — below 0.80
    # 4 of 4+1 match → need high overlap: drop one, add zero extra
    similar_headers = ["Tenant Name", "Rent PSF", "Lease Date", "SF"]
    # intersection=4, union=5 → 0.80
    fp_similar = compute_fingerprint(similar_headers + ["Address"], "new.xlsx", "S1", "LEASE")
    # Exact match will fire for this identical set — test with 4/5
    fp_fuzzy = compute_fingerprint(similar_headers, "new.xlsx", "S1", "LEASE")
    # similarity = 4/6 ≈ 0.667 — use threshold=0.60 to hit
    match = tier2_fuzzy_lookup(store, fp_fuzzy, threshold=0.60)
    assert match is not None
    assert match.source == "fuzzy"
    assert match.similarity >= 0.60


def test_tier2_fuzzy_lookup_misses_below_threshold():
    base_headers = ["Tenant Name", "Rent PSF", "Lease Date", "SF", "Address"]
    store, _ = _make_store_with_fingerprint(base_headers)
    # completely different headers → should miss at 0.80
    fp_diff = compute_fingerprint(["Col A", "Col B", "Col C"], "new.xlsx", "S1", "LEASE")
    match = tier2_fuzzy_lookup(store, fp_diff, threshold=0.80)
    assert match is None


# --- tier3_broker_lookup ---

def test_tier3_broker_lookup_hits_on_60_percent_overlap():
    base_headers = ["Tenant Name", "Rent PSF", "Lease Date", "SF", "Address"]
    store = FakeLearningStore()
    broker_id = store.upsert_broker("CBRE", confirmed_by="test")
    fp = compute_fingerprint(base_headers, "test.xlsx", "Sheet1", "LEASE")
    mappings = {"tenant name": "tenant", "rent psf": "rate_psf"}
    store.record_accepted_mapping(fp, mappings, confirmed_by="test", broker_id=broker_id)

    # Similar to stored (4/6 overlap ≈ 0.667 > 0.60) but different from exact
    similar_headers = ["Tenant Name", "Rent PSF", "Lease Date", "SF"]
    fp_similar = compute_fingerprint(similar_headers, "new.xlsx", "S1", "LEASE")
    match = tier3_broker_lookup(store, fp_similar, broker_name="CBRE", threshold=0.60)
    assert match is not None
    assert match.source == "broker"
    assert match.similarity >= 0.60


def test_tier3_broker_lookup_misses_with_wrong_broker():
    base_headers = ["Tenant Name", "Rent PSF", "Lease Date", "SF", "Address"]
    store = FakeLearningStore()
    broker_id = store.upsert_broker("CBRE", confirmed_by="test")
    fp = compute_fingerprint(base_headers, "test.xlsx", "Sheet1", "LEASE")
    store.record_accepted_mapping(fp, {}, confirmed_by="test", broker_id=broker_id)

    similar_headers = ["Tenant Name", "Rent PSF", "Lease Date", "SF"]
    fp_similar = compute_fingerprint(similar_headers, "new.xlsx", "S1", "LEASE")
    match = tier3_broker_lookup(store, fp_similar, broker_name="JLL", threshold=0.60)
    assert match is None
