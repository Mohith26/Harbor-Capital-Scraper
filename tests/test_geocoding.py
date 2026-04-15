"""Tests for engine/geocoding.py."""
import responses
from engine.geocoding import fetch_google_data

GOOGLE_URL = "https://maps.googleapis.com/maps/api/geocode/json"


@responses.activate
def test_fetch_google_data_parses_response():
    responses.add(
        responses.GET,
        GOOGLE_URL,
        json={
            "status": "OK",
            "results": [{
                "formatted_address": "123 Main St, Houston, TX 77002, USA",
                "geometry": {"location": {"lat": 29.7604, "lng": -95.3698}},
                "address_components": [
                    {"long_name": "123", "types": ["street_number"]},
                    {"long_name": "Main St", "types": ["route"]},
                    {"long_name": "Houston", "types": ["locality"]},
                    {"long_name": "TX", "short_name": "TX", "types": ["administrative_area_level_1"]},
                    {"long_name": "77002", "types": ["postal_code"]},
                ],
            }],
        },
    )
    result = fetch_google_data("123 Main, Houston", api_key="fake-key")
    assert result is not None
    assert result["latitude"] == 29.7604
    assert result["longitude"] == -95.3698
    assert result["state"] == "TX"
