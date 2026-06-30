from sqlalchemy import create_engine, text
from web.costar_view import costar_badge, comparison_rows, select_costar_candidate


def test_costar_badge_maps_status():
    assert costar_badge("enriched")["label"].lower().startswith("costar")
    assert costar_badge("ambiguous")["css"]
    assert costar_badge("not_found")["label"]
    assert costar_badge("pending")["label"]


def test_comparison_rows_flags_agreement():
    rows = comparison_rows(
        analyst={"building_size": 100000, "year_built": 1998},
        costar_specs={"rba_sf": 120500, "year_built": 1998},
    )
    by_field = {r["field"]: r for r in rows}
    assert by_field["year_built"]["agree"] is True
    # building_size 100000 vs rba_sf 120500 is ~17% off (> 2% tol) -> not agree
    assert by_field["building_size"]["agree"] is False


def test_comparison_rows_agrees_within_tolerance():
    rows = comparison_rows(
        analyst={"building_size": 120000},
        costar_specs={"rba_sf": 120500},  # 0.4% diff, within 2% tolerance
    )
    by_field = {r["field"]: r for r in rows}
    assert by_field["building_size"]["agree"] is True


def test_select_candidate_requeues_pending(tmp_path):
    url = f"sqlite:///{tmp_path/'c.db'}"
    eng = create_engine(url)
    with eng.begin() as c:
        c.execute(text(
            "CREATE TABLE sale_comps (id INTEGER PRIMARY KEY, costar_property_id TEXT, "
            "costar_status TEXT, costar_candidates TEXT)"
        ))
        c.execute(text(
            "INSERT INTO sale_comps (id, costar_status, costar_candidates) "
            "VALUES (1, 'ambiguous', '[]')"
        ))
    select_costar_candidate(url, "sale", 1, "777")
    with eng.connect() as c:
        row = c.execute(text("SELECT costar_property_id, costar_status FROM sale_comps WHERE id=1")).mappings().one()
    assert row["costar_property_id"] == "777"
    assert row["costar_status"] == "pending"   # re-queued for local enrich
