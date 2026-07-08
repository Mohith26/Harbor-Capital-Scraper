import engine.mapping_audit as ma


def test_audit_segment_normalizes_suggestions(monkeypatch):
    monkeypatch.setenv("COMP_MAPPING_AUDIT", "1")
    monkeypatch.setattr(
        ma,
        "verify_mapping",
        lambda mappings, rows, schema: {
            "adjusted_confidence": {},
            "flags": [
                {"header": "CLOSE DATE", "reason": "dates", "suggested_field": "closing_date"},
                {"header": "BUYER", "reason": "weird", "suggested_field": "not_a_field"},
                {"header": "SELLER", "reason": "same", "suggested_field": "seller"},
            ],
        },
    )
    mappings = {"CLOSE DATE": "buyer", "BUYER": "buyer", "SELLER": "seller"}

    flags = ma.audit_segment(mappings, [{"CLOSE DATE": "5/1/22"}], "SALE")

    by = {f["header"]: f for f in flags}
    assert by["CLOSE DATE"]["suggested_field"] == "closing_date"  # valid + differs from current
    assert by["BUYER"]["suggested_field"] is None                 # not a schema field
    assert by["SELLER"]["suggested_field"] is None                # equals current mapping


def test_audit_segment_killswitch_off_skips_llm(monkeypatch):
    monkeypatch.setenv("COMP_MAPPING_AUDIT", "0")

    def _boom(*a, **k):
        raise AssertionError("verify_mapping must not be called when disabled")

    monkeypatch.setattr(ma, "verify_mapping", _boom)

    assert ma.audit_segment({"A": "buyer"}, [{"A": "x"}], "SALE") == []


def test_audit_segment_empty_mappings(monkeypatch):
    monkeypatch.setenv("COMP_MAPPING_AUDIT", "1")
    assert ma.audit_segment({}, [{"A": "x"}], "SALE") == []


def test_audit_segment_error_returns_empty(monkeypatch):
    monkeypatch.setenv("COMP_MAPPING_AUDIT", "1")

    def _raise(*a, **k):
        raise RuntimeError("api down")

    monkeypatch.setattr(ma, "verify_mapping", _raise)

    assert ma.audit_segment({"A": "buyer"}, [{"A": "x"}], "SALE") == []


def test_audit_segment_uses_lease_schema(monkeypatch):
    monkeypatch.setenv("COMP_MAPPING_AUDIT", "1")
    monkeypatch.setattr(
        ma,
        "verify_mapping",
        lambda mappings, rows, schema: {
            "flags": [{"header": "H", "reason": "r", "suggested_field": "tenant_name"}]
        },
    )

    flags = ma.audit_segment({"H": "rate_psf"}, [{"H": "Acme"}], "LEASE")

    assert flags[0]["suggested_field"] == "tenant_name"  # valid LEASE field
