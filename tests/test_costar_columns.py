from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import database


def test_sale_comp_has_costar_columns():
    cols = {c.name for c in database.SaleComp.__table__.columns}
    assert {"costar_property_id", "costar_url", "costar_specs",
            "costar_status", "costar_candidates", "costar_enriched_at"} <= cols


def test_new_sale_comp_defaults_to_pending(tmp_path):
    eng = create_engine(f"sqlite:///{tmp_path/'t.db'}")
    database.Base.metadata.create_all(eng)
    Session = sessionmaker(bind=eng)
    with Session() as s:
        row = database.SaleComp(address="1 Main St")
        s.add(row)
        s.commit()
        s.refresh(row)
        assert row.costar_status == "pending"


def test_lease_comp_has_costar_columns():
    cols = {c.name for c in database.LeaseComp.__table__.columns}
    assert "costar_status" in cols and "costar_specs" in cols
