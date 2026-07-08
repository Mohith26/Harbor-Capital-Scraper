import pandas as pd
import pytest

from engine.types import Fingerprint, MappingResult, SegmentResult
from web.routes.upload import (
    _active_segments,
    _dedupe_segments_within_sheets,
    _schema_fields_for_type,
)


def _resolve_destination_model(seg):
    """Mirror the SaleComp/LeaseComp routing used by the save path, which keys
    entirely off ``seg.fingerprint.file_type``."""
    from database import LeaseComp, SaleComp

    return LeaseComp if seg.fingerprint.file_type.lower() in ("lease", "both") else SaleComp


def _segment(segment_key, sheet_name, file_type, df):
    fp = Fingerprint(
        raw_hash=segment_key,
        header_set_hash=segment_key,
        headers=list(df.columns),
        normalized_headers=list(df.columns),
        file_type=file_type,
        filename="comps.xlsx",
        sheet_name=sheet_name,
    )
    mapping_result = MappingResult(
        fingerprint=fp,
        mappings={c: c for c in df.columns},
        confidence={c: 1.0 for c in df.columns},
        source="test",
        similarity=1.0,
        cleaned_df=df,
    )
    return SegmentResult(
        segment_key=segment_key,
        fingerprint=fp,
        mapping_result=mapping_result,
        cleaned_df=df.copy(),
    )


def test_schema_fields_follow_segment_file_type():
    assert "tenant_name" in _schema_fields_for_type("LEASE")
    assert "sale_price" not in _schema_fields_for_type("LEASE")

    assert "sale_price" in _schema_fields_for_type("SALE")
    assert "tenant_name" not in _schema_fields_for_type("SALE")


def test_active_segments_excludes_voided_segment_keys():
    sales_seg = _segment("Sales::0", "Sales", "SALE", pd.DataFrame([{"address": "123 Main St"}]))
    lease_seg = _segment("Leases::0", "Leases", "LEASE", pd.DataFrame([{"address": "456 Main St"}]))
    job = {
        "segments": [sales_seg, lease_seg],
        "voided_segment_keys": ["Leases::0"],
    }

    active = _active_segments(job)

    assert [seg.segment_key for seg in active] == ["Sales::0"]


def test_dedupe_keeps_most_complete_sale_entry_within_sheet():
    df = pd.DataFrame([
        {"address": "123 Main Street", "sale_price": None, "buyer": None},
        {"address": "123 Main St", "sale_price": "$1,000,000", "buyer": "Buyer LLC"},
    ])
    seg = _segment("Sales::0", "Sales", "SALE", df)

    deduped = _dedupe_segments_within_sheets([seg])

    assert deduped == 1
    assert len(seg.cleaned_df) == 1
    assert seg.cleaned_df.iloc[0]["buyer"] == "Buyer LLC"


def test_dedupe_preserves_conflicting_sale_entries():
    df = pd.DataFrame([
        {"address": "123 Main St", "sale_price": "$1,000,000", "buyer": "Buyer A"},
        {"address": "123 Main St", "sale_price": "$2,000,000", "buyer": "Buyer B"},
    ])
    seg = _segment("Sales::0", "Sales", "SALE", df)

    deduped = _dedupe_segments_within_sheets([seg])

    assert deduped == 0
    assert len(seg.cleaned_df) == 2


def test_dedupe_preserves_distinct_lease_tenants_at_same_address():
    df = pd.DataFrame([
        {"address": "123 Main St", "tenant_name": "Tenant A", "rate_psf": "$10"},
        {"address": "123 Main St", "tenant_name": "Tenant B", "rate_psf": "$12"},
    ])
    seg = _segment("Leases::0", "Leases", "LEASE", df)

    deduped = _dedupe_segments_within_sheets([seg])

    assert deduped == 0
    assert len(seg.cleaned_df) == 2


@pytest.mark.parametrize("start_type,new_type", [("LEASE", "sale"), ("SALE", "lease")])
def test_apply_mappings_type_override_flips_destination_model(monkeypatch, start_type, new_type):
    """A per-segment type override in the upload payload must re-route the segment
    to the opposite comp table before save.

    Mirrors the real 'IOS Sale Comps' bug: an inner sheet literally named
    'Lease Comps' is auto-tagged LEASE even though its columns are sale columns
    (CLOSE DATE / BUYER / SELLER / SALE PRICE). Toggling the type must flip the
    destination model.
    """
    from fastapi.testclient import TestClient

    import web.main
    from database import LeaseComp, SaleComp
    from web.main import app
    from web.routes.upload import _jobs

    # AuthMiddleware gates /upload — supply a fake authenticated session.
    monkeypatch.setattr(
        web.main,
        "get_auth_session",
        lambda request: {"username": "qa", "name": "QA", "role": "admin"},
    )

    raw_df = pd.DataFrame([{"ADDRESS": "123 Main St", "PRICE": "$10,000,000"}])
    seg = _segment("Sheet::0", "Sheet", start_type, raw_df)
    job_id = "job-type-flip"
    _jobs[job_id] = {
        "segments": [seg],
        "raw_dfs": {seg.segment_key: raw_df.copy()},
        "tmp_path": None,
        "filename": "IOS Sale Comps - 2022-2023.xlsx",
        "status": "mapped",
        "broker": None,
    }

    expected_before = LeaseComp if start_type.lower() == "lease" else SaleComp
    expected_after = LeaseComp if new_type == "lease" else SaleComp

    try:
        assert _resolve_destination_model(seg) is expected_before

        client = TestClient(app)
        resp = client.post(
            "/upload/apply-mappings",
            json={
                "job_id": job_id,
                "final_mappings": {seg.segment_key: {"ADDRESS": "address"}},
                "final_types": {seg.segment_key: new_type},
                "voided_segment_keys": [],
            },
        )

        assert resp.status_code == 200
        assert resp.json() == {"status": "ok"}

        updated = _jobs[job_id]["segments"][0]
        # The override lands on the fingerprint...
        assert updated.fingerprint.file_type == new_type
        # ...which is the sole routing key, so the destination model flips.
        assert _resolve_destination_model(updated) is expected_after
        assert expected_after is not expected_before
    finally:
        _jobs.pop(job_id, None)


def test_apply_mappings_without_final_types_preserves_original_type(monkeypatch):
    """Omitting ``final_types`` (older client payloads) must not alter routing."""
    from fastapi.testclient import TestClient

    import web.main
    from database import SaleComp
    from web.main import app
    from web.routes.upload import _jobs

    monkeypatch.setattr(
        web.main,
        "get_auth_session",
        lambda request: {"username": "qa", "name": "QA", "role": "admin"},
    )

    raw_df = pd.DataFrame([{"ADDRESS": "123 Main St", "SALE PRICE": "$10,000,000"}])
    seg = _segment("Sheet::0", "Sheet", "SALE", raw_df)
    job_id = "job-no-types"
    _jobs[job_id] = {
        "segments": [seg],
        "raw_dfs": {seg.segment_key: raw_df.copy()},
        "tmp_path": None,
        "filename": "comps.xlsx",
        "status": "mapped",
        "broker": None,
    }

    try:
        client = TestClient(app)
        resp = client.post(
            "/upload/apply-mappings",
            json={
                "job_id": job_id,
                "final_mappings": {seg.segment_key: {"ADDRESS": "address"}},
                "voided_segment_keys": [],
            },
        )

        assert resp.status_code == 200
        updated = _jobs[job_id]["segments"][0]
        assert updated.fingerprint.file_type == "SALE"
        assert _resolve_destination_model(updated) is SaleComp
    finally:
        _jobs.pop(job_id, None)


def test_attach_audit_flags_populates_segments_data(monkeypatch):
    import web.routes.upload as up

    raw_df = pd.DataFrame([{"CLOSE DATE": "5/1/22", "PRICE": "$10,000,000"}])
    seg = _segment("Sheet::0", "Sheet", "SALE", raw_df)
    job = {"segments": [seg], "raw_dfs": {seg.segment_key: raw_df.copy()}}
    segments_data = [{"segment_key": seg.segment_key, "voided": False}]

    monkeypatch.setattr(
        up,
        "audit_segment",
        lambda mappings, sample_rows, file_type: [
            {"header": "CLOSE DATE", "reason": "dates", "suggested_field": "closing_date"}
        ],
    )

    up._attach_audit_flags(job, segments_data)

    assert segments_data[0]["audit_flags"] == [
        {"header": "CLOSE DATE", "reason": "dates", "suggested_field": "closing_date"}
    ]


def test_attach_audit_flags_degrades_to_empty_on_error(monkeypatch):
    import web.routes.upload as up

    raw_df = pd.DataFrame([{"A": "x"}])
    seg = _segment("Sheet::0", "Sheet", "SALE", raw_df)
    job = {"segments": [seg], "raw_dfs": {seg.segment_key: raw_df.copy()}}
    segments_data = [{"segment_key": seg.segment_key, "voided": False}]

    def _boom(*a, **k):
        raise RuntimeError("down")

    monkeypatch.setattr(up, "audit_segment", _boom)

    up._attach_audit_flags(job, segments_data)

    assert segments_data[0]["audit_flags"] == []


def test_attach_audit_flags_handles_missing_raw_df(monkeypatch):
    import web.routes.upload as up

    raw_df = pd.DataFrame([{"A": "x"}])
    seg = _segment("Sheet::0", "Sheet", "SALE", raw_df)
    job = {"segments": [seg], "raw_dfs": {}}  # no raw df for this segment
    segments_data = [{"segment_key": seg.segment_key, "voided": False}]

    monkeypatch.setattr(
        up,
        "audit_segment",
        lambda *a, **k: [{"header": "A", "reason": "r", "suggested_field": None}],
    )

    up._attach_audit_flags(job, segments_data)

    assert segments_data[0]["audit_flags"] == []  # skipped: no raw df to sample


def test_attach_audit_flags_multi_segment_and_no_mapped_headers(monkeypatch):
    import web.routes.upload as up

    raw_a = pd.DataFrame([{"CLOSE DATE": "5/1/22"}])
    seg_a = _segment("A::0", "A", "SALE", raw_a)
    raw_b = pd.DataFrame([{"OTHER": "x"}])
    seg_b = _segment("B::0", "B", "SALE", raw_b)
    raw_c = pd.DataFrame([{"REAL": "y"}])
    seg_c = _segment("C::0", "C", "SALE", raw_c)
    seg_c.mapping_result.mappings = {"GHOST": "buyer"}  # header absent from raw_c -> no mapped headers

    job = {
        "segments": [seg_a, seg_b, seg_c],
        "raw_dfs": {
            seg_a.segment_key: raw_a.copy(),
            seg_b.segment_key: raw_b.copy(),
            seg_c.segment_key: raw_c.copy(),
        },
    }
    segments_data = [
        {"segment_key": "A::0", "voided": False},
        {"segment_key": "B::0", "voided": False},
        {"segment_key": "C::0", "voided": False},
    ]

    def _fake_audit(mappings, sample_rows, file_type):
        return [{"header": list(mappings)[0], "reason": "r", "suggested_field": None}]

    monkeypatch.setattr(up, "audit_segment", _fake_audit)

    up._attach_audit_flags(job, segments_data)

    assert segments_data[0]["audit_flags"] == [{"header": "CLOSE DATE", "reason": "r", "suggested_field": None}]
    assert segments_data[1]["audit_flags"] == [{"header": "OTHER", "reason": "r", "suggested_field": None}]
    assert segments_data[2]["audit_flags"] == []
