import engine.llm_mapping as llm_mapping
from engine.llm_mapping import llm_map_columns
from engine.mapping import LEASE_SCHEMA


def test_llm_map_columns_uses_chat_json(monkeypatch):
    captured = {}

    def fake_chat_json(prompt, model="gpt-4o"):
        captured["prompt"] = prompt
        return {
            "mappings": {"Asking Rate": "rate_psf", "SF": "leased_sf"},
            "confidence": {"Asking Rate": 0.95, "SF": 0.9},
            "unmapped": ["Mystery Col"],
            "reasoning": "values look like $/sf and square footage",
        }

    monkeypatch.setattr(llm_mapping, "_chat_json", fake_chat_json)

    result = llm_map_columns(
        headers=["Asking Rate", "SF", "Mystery Col"],
        sample_rows=[{"Asking Rate": "$8.15", "SF": "20,007", "Mystery Col": "?"}],
        schema=LEASE_SCHEMA,
        file_type="LEASE",
        examples=[{"raw_header": "asking rate", "target_column": "rate_psf"}],
    )

    assert result["mappings"]["Asking Rate"] == "rate_psf"
    assert result["unmapped"] == ["Mystery Col"]
    # Prompt must include the schema, the sample values, and the few-shot example
    assert "rate_psf" in captured["prompt"]
    assert "$8.15" in captured["prompt"]
    assert "asking rate" in captured["prompt"]


def test_llm_map_columns_drops_mappings_to_unknown_targets(monkeypatch):
    monkeypatch.setattr(
        llm_mapping, "_chat_json",
        lambda prompt, model="gpt-4o": {
            "mappings": {"A": "rate_psf", "B": "not_a_real_field"},
            "confidence": {"A": 0.9, "B": 0.9},
            "unmapped": [],
            "reasoning": "",
        },
    )
    result = llm_map_columns(["A", "B"], [{"A": 1, "B": 2}], LEASE_SCHEMA, "LEASE")
    assert "A" in result["mappings"]
    assert "B" not in result["mappings"]  # hallucinated target filtered out
    assert "B" in result["unmapped"]


def test_llm_map_columns_propagates_errors_as_exception(monkeypatch):
    def boom(prompt, model="gpt-4o"):
        raise RuntimeError("no api key")
    monkeypatch.setattr(llm_mapping, "_chat_json", boom)

    import pytest
    with pytest.raises(RuntimeError):
        llm_map_columns(["A"], [{"A": 1}], LEASE_SCHEMA, "LEASE")
