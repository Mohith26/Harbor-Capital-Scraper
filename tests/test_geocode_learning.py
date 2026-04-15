"""Tests for learned geocoding (resolve_geocode) in engine/geocoding.py."""
import responses
from engine.geocoding import resolve_geocode
from learning.fakes import FakeLearningStore


GOOGLE_URL = "https://maps.googleapis.com/maps/api/geocode/json"


def _ok_response(addr, lat, lng):
    return {
        "status": "OK",
        "results": [{
            "formatted_address": addr,
            "geometry": {"location": {"lat": lat, "lng": lng}},
            "address_components": [
                {"long_name": "TX", "short_name": "TX",
                 "types": ["administrative_area_level_1"]},
            ],
        }],
    }


def test_override_table_short_circuits_everything():
    store = FakeLearningStore()
    store.record_geocode_override(
        raw_text="123 Fake St, TX",
        override_address="123 Fake St, Houston, TX 77002",
        lat=29.7, lng=-95.3,
        confirmed_by="user",
    )
    result = resolve_geocode("123 Fake St", api_key="k", store=store, openai_client=None)
    assert result["latitude"] == 29.7
    assert result["source"] == "override"


def test_alias_cache_returns_before_calling_google():
    store = FakeLearningStore()
    store.insert_geocode_alias(
        raw_text="456 Main, TX",
        canonical_address="456 Main, Austin, TX 78701",
        lat=30.2, lng=-97.7,
    )
    result = resolve_geocode("456 Main", api_key="k", store=store, openai_client=None)
    assert result["source"] == "alias_cache"
    assert result["latitude"] == 30.2


@responses.activate
def test_miss_calls_google_and_writes_alias():
    responses.add(
        responses.GET, GOOGLE_URL,
        json=_ok_response("789 Pine, Dallas, TX 75201", 32.8, -96.8),
    )
    store = FakeLearningStore()
    result = resolve_geocode("789 Pine", api_key="k", store=store, openai_client=None)
    assert result["source"] == "google"
    assert result["latitude"] == 32.8

    # Second call should now hit alias cache — no new HTTP request
    result2 = resolve_geocode("789 Pine", api_key="k", store=store, openai_client=None)
    assert result2["source"] == "alias_cache"
    assert len(responses.calls) == 1  # still only one network call


@responses.activate
def test_llm_normalization_used_when_google_fails_first_time():
    from types import SimpleNamespace
    # First call: Google returns ZERO_RESULTS for the raw string
    # Second call: after LLM normalization, Google succeeds
    responses.add(responses.GET, GOOGLE_URL, json={"status": "ZERO_RESULTS", "results": []})
    responses.add(
        responses.GET, GOOGLE_URL,
        json=_ok_response("Cleaned Address, Houston, TX", 29.7, -95.3),
    )

    fake_llm = SimpleNamespace(normalize=lambda raw: "Cleaned Address, Houston, TX")

    store = FakeLearningStore()
    result = resolve_geocode(
        "garbage raw text", api_key="k", store=store, openai_client=fake_llm
    )
    assert result["source"] == "google+llm"
    assert len(responses.calls) == 2
