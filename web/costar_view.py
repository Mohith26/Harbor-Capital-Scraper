"""Pure presentation + action helpers for CoStar enrichment in the comp UI."""
from __future__ import annotations

from sqlalchemy import create_engine, text

_BADGES = {
    "enriched": {"label": "CoStar ✓", "css": "badge-success"},
    "ambiguous": {"label": "CoStar: choose", "css": "badge-warn"},
    "not_found": {"label": "CoStar: none", "css": "badge-muted"},
    "pending": {"label": "CoStar: pending", "css": "badge-pending"},
    "error": {"label": "CoStar: error", "css": "badge-error"},
}

# Overlapping fields: (analyst_column, costar_specs_key)
_OVERLAP = [
    ("building_size", "rba_sf"),
    ("leased_sf", "rba_sf"),
    ("year_built", "year_built"),
    ("clear_height", "clear_height_ft"),
    ("cap_rate", "submkt_cap_rate"),
]

_TOL = 0.02  # 2% tolerance for numeric agreement


def costar_badge(status: str) -> dict:
    return _BADGES.get(status or "pending", _BADGES["pending"])


def _agree(a, b) -> bool:
    if a is None or b is None:
        return False
    try:
        a, b = float(a), float(b)
    except (TypeError, ValueError):
        return str(a).strip().lower() == str(b).strip().lower()
    if a == b:
        return True
    denom = max(abs(a), abs(b)) or 1.0
    return abs(a - b) / denom <= _TOL


def comparison_rows(analyst: dict, costar_specs: dict) -> list[dict]:
    rows = []
    for acol, ckey in _OVERLAP:
        if acol in analyst or ckey in costar_specs:
            av, cv = analyst.get(acol), costar_specs.get(ckey)
            if av is None and cv is None:
                continue
            rows.append({"field": acol, "analyst_value": av,
                         "costar_value": cv, "agree": _agree(av, cv)})
    return rows


def select_costar_candidate(db_url: str, comp_type: str, comp_id: int, pid: str) -> None:
    table = {"sale": "sale_comps", "lease": "lease_comps"}[comp_type]
    eng = create_engine(db_url)
    with eng.begin() as c:
        c.execute(text(
            f"UPDATE {table} SET costar_property_id=:pid, costar_status='pending' WHERE id=:id"
        ), {"pid": pid, "id": comp_id})
