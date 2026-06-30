import engine.verify_mapping as verify_mod
from engine.verify_mapping import verify_mapping
from engine.mapping import SALE_SCHEMA


def test_verify_flags_value_type_mismatch(monkeypatch):
    monkeypatch.setattr(
        verify_mod, "_chat_json",
        lambda prompt, model="gpt-4o": {
            "adjusted_confidence": {"Size": 0.2},
            "flags": [{"header": "Size", "reason": "values look like square footage, not a sale price"}],
        },
    )
    result = verify_mapping(
        mappings={"Size": "sale_price"},
        sample_rows=[{"Size": "19,500"}],
        schema=SALE_SCHEMA,
    )
    assert result["flags"][0]["header"] == "Size"
    assert result["adjusted_confidence"]["Size"] == 0.2


def test_verify_failure_degrades_gracefully(monkeypatch):
    def boom(prompt, model="gpt-4o"):
        raise RuntimeError("api down")
    monkeypatch.setattr(verify_mod, "_chat_json", boom)

    result = verify_mapping({"A": "sale_price"}, [{"A": 1}], SALE_SCHEMA)
    # On verifier failure: no flags, no confidence change (caller keeps mapper confidence)
    assert result["flags"] == []
    assert result["adjusted_confidence"] == {}
