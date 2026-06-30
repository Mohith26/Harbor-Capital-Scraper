import pandas as pd

from engine.types import Fingerprint, MappingResult, SegmentResult
from web.routes.upload import (
    _active_segments,
    _dedupe_segments_within_sheets,
    _schema_fields_for_type,
)


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
