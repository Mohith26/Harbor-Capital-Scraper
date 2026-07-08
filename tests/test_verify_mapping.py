import engine.verify_mapping as vm

_SCHEMA = {
    "tenant_name": {"desc": "tenant", "type": "text"},
    "closing_date": {"desc": "closing date", "type": "date"},
}


def test_verify_mapping_parses_suggested_field(monkeypatch):
    monkeypatch.setattr(
        vm,
        "_chat_json",
        lambda prompt, model=None: {
            "adjusted_confidence": {"CLOSE DATE": 0.1},
            "flags": [
                {"header": "CLOSE DATE", "reason": "values are dates", "suggested_field": "closing_date"}
            ],
        },
    )

    out = vm.verify_mapping({"CLOSE DATE": "tenant_name"}, [{"CLOSE DATE": "5/1/22"}], _SCHEMA)

    assert out["flags"] == [
        {"header": "CLOSE DATE", "reason": "values are dates", "suggested_field": "closing_date"}
    ]
    assert out["adjusted_confidence"] == {"CLOSE DATE": 0.1}


def test_verify_mapping_missing_suggested_field_is_none(monkeypatch):
    monkeypatch.setattr(
        vm,
        "_chat_json",
        lambda prompt, model=None: {"flags": [{"header": "CLOSE DATE", "reason": "dates"}]},
    )

    out = vm.verify_mapping({"CLOSE DATE": "tenant_name"}, [], _SCHEMA)

    assert out["flags"][0]["suggested_field"] is None


def test_verify_mapping_drops_flags_for_unknown_headers(monkeypatch):
    monkeypatch.setattr(
        vm,
        "_chat_json",
        lambda prompt, model=None: {
            "flags": [{"header": "GHOST", "reason": "x", "suggested_field": "closing_date"}]
        },
    )

    out = vm.verify_mapping({"CLOSE DATE": "tenant_name"}, [], _SCHEMA)

    assert out["flags"] == []


def test_verify_mapping_llm_error_is_noop(monkeypatch):
    def _raise(*a, **k):
        raise RuntimeError("api down")

    monkeypatch.setattr(vm, "_chat_json", _raise)

    out = vm.verify_mapping({"CLOSE DATE": "tenant_name"}, [], _SCHEMA)

    assert out == {"adjusted_confidence": {}, "flags": []}


def test_verify_mapping_non_string_suggested_field_is_none(monkeypatch):
    monkeypatch.setattr(
        vm,
        "_chat_json",
        lambda prompt, model=None: {
            "flags": [{"header": "CLOSE DATE", "reason": "dates", "suggested_field": 123}]
        },
    )

    out = vm.verify_mapping({"CLOSE DATE": "tenant_name"}, [], _SCHEMA)

    assert out["flags"][0]["suggested_field"] is None


def test_verify_mapping_whitespace_suggested_field_is_none(monkeypatch):
    monkeypatch.setattr(
        vm,
        "_chat_json",
        lambda prompt, model=None: {
            "flags": [{"header": "CLOSE DATE", "reason": "dates", "suggested_field": "   "}]
        },
    )

    out = vm.verify_mapping({"CLOSE DATE": "tenant_name"}, [], _SCHEMA)

    assert out["flags"][0]["suggested_field"] is None
