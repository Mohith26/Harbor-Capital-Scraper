"""Tests: comp rows without an address must NOT be persisted.

Bug: comps with NULL / empty-string / whitespace-only address were being
inserted into the local DB. Address is required — without it the row can't
be geocoded, mapped, or matched in Comp Finder.

These tests cover both layers of defense:
  1. Application-level: the upload save path must skip rows without address.
  2. Schema-level: the SQLAlchemy model must reject rows without address.
"""
import pandas as pd
import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import IntegrityError

import database as db_mod
from database import Base, SaleComp, LeaseComp
from web.routes.upload import _has_address, _save_rows_to_db


# ---------------------------------------------------------------------------
# Layer 1: helper that decides "does this row have an address?"
# ---------------------------------------------------------------------------

class TestHasAddressHelper:
    def test_none_is_missing(self):
        assert _has_address(None) is False

    def test_empty_string_is_missing(self):
        assert _has_address("") is False

    def test_whitespace_only_is_missing(self):
        assert _has_address("   ") is False
        assert _has_address("\t\n") is False

    def test_nan_is_missing(self):
        assert _has_address(float("nan")) is False

    def test_pandas_na_is_missing(self):
        assert _has_address(pd.NA) is False

    def test_real_address_is_present(self):
        assert _has_address("123 Main St, Raleigh, NC") is True

    def test_address_with_surrounding_whitespace_is_present(self):
        assert _has_address("  123 Main St  ") is True


# ---------------------------------------------------------------------------
# Layer 2: application-level — upload save path must skip rows w/o address
# ---------------------------------------------------------------------------

@pytest.fixture
def fresh_sqlite_session(monkeypatch):
    """Spin up an isolated in-memory SQLite with the comp schema."""
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    LocalSession = sessionmaker(bind=engine)
    # Point the upload module's Session at our test session.
    monkeypatch.setattr(db_mod, "Session", LocalSession)
    import web.routes.upload as upload_mod
    monkeypatch.setattr(upload_mod, "Session", LocalSession)
    return LocalSession


class TestSaveRowsSkipsMissingAddress:
    def test_rows_without_address_are_skipped(self, fresh_sqlite_session):
        df = pd.DataFrame([
            {"address": "123 Real St, Raleigh, NC", "sale_price": 1_000_000},
            {"address": None, "sale_price": 2_000_000},
            {"address": "", "sale_price": 3_000_000},
            {"address": "   ", "sale_price": 4_000_000},
            {"address": "456 Other St, Cary, NC", "sale_price": 5_000_000},
        ])
        result = _save_rows_to_db(df, file_type="sale", source_file="x.csv")

        # Only the two real-address rows should be inserted.
        assert len(result["inserted_ids"]) == 2
        assert result["skipped_count"] == 3

        session = fresh_sqlite_session()
        try:
            rows = session.query(SaleComp).order_by(SaleComp.id).all()
            assert len(rows) == 2
            addresses = [r.address for r in rows]
            assert "123 Real St, Raleigh, NC" in addresses
            assert "456 Other St, Cary, NC" in addresses
        finally:
            session.close()

    def test_no_rows_inserted_when_all_missing_address(self, fresh_sqlite_session):
        df = pd.DataFrame([
            {"address": None, "sale_price": 1_000_000},
            {"address": "", "sale_price": 2_000_000},
            {"address": "  \t  ", "sale_price": 3_000_000},
        ])
        result = _save_rows_to_db(df, file_type="sale", source_file="x.csv")

        assert result["inserted_ids"] == []
        assert result["skipped_count"] == 3

        session = fresh_sqlite_session()
        try:
            assert session.query(SaleComp).count() == 0
        finally:
            session.close()

    def test_lease_comps_also_validated(self, fresh_sqlite_session):
        df = pd.DataFrame([
            {"address": "100 Lease Way, Raleigh, NC", "tenant_name": "Tenant A"},
            {"address": "", "tenant_name": "Tenant B"},
        ])
        result = _save_rows_to_db(df, file_type="lease", source_file="x.csv")

        assert len(result["inserted_ids"]) == 1
        assert result["skipped_count"] == 1

        session = fresh_sqlite_session()
        try:
            rows = session.query(LeaseComp).all()
            assert len(rows) == 1
            assert rows[0].address == "100 Lease Way, Raleigh, NC"
        finally:
            session.close()


# ---------------------------------------------------------------------------
# Layer 3: schema-level — even direct ORM use should reject empty addresses
# ---------------------------------------------------------------------------

class TestSchemaRejectsMissingAddress:
    """The ORM @validates hook fires when the attribute is set (incl. ctor).

    We accept the rejection happening at construction OR at commit — what we
    care about is that the row never lands in the DB.
    """

    def test_schema_rejects_null_address(self, fresh_sqlite_session):
        session = fresh_sqlite_session()
        try:
            with pytest.raises((IntegrityError, ValueError)):
                rec = SaleComp(address=None, sale_price=1_000_000)
                session.add(rec)
                session.commit()
            session.rollback()
            assert session.query(SaleComp).count() == 0
        finally:
            session.close()

    def test_schema_rejects_empty_address(self, fresh_sqlite_session):
        session = fresh_sqlite_session()
        try:
            with pytest.raises((IntegrityError, ValueError)):
                rec = SaleComp(address="", sale_price=1_000_000)
                session.add(rec)
                session.commit()
            session.rollback()
            assert session.query(SaleComp).count() == 0
        finally:
            session.close()

    def test_schema_rejects_whitespace_address(self, fresh_sqlite_session):
        session = fresh_sqlite_session()
        try:
            with pytest.raises((IntegrityError, ValueError)):
                rec = SaleComp(address="   ", sale_price=1_000_000)
                session.add(rec)
                session.commit()
            session.rollback()
            assert session.query(SaleComp).count() == 0
        finally:
            session.close()

    def test_schema_accepts_real_address(self, fresh_sqlite_session):
        session = fresh_sqlite_session()
        try:
            session.add(SaleComp(address="123 Real St", sale_price=1_000_000))
            session.commit()
            assert session.query(SaleComp).count() == 1
        finally:
            session.close()
