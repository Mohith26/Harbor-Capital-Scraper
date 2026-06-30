"""Idempotent migration: add costar_* columns to sale_comps and lease_comps.

Run against the live DB once: python scripts/add_costar_columns.py
Safe to re-run (ADD COLUMN IF NOT EXISTS).
"""
import os
from sqlalchemy import create_engine, text

DB_URL = os.environ.get("SUPABASE_DB_URL", "sqlite:///comps.db")

_COLS = [
    "costar_property_id VARCHAR",
    "costar_url VARCHAR",
    "costar_specs TEXT",
    "costar_status VARCHAR DEFAULT 'pending'",
    "costar_candidates TEXT",
    "costar_enriched_at TIMESTAMP",
]


def main() -> None:
    eng = create_engine(DB_URL)
    with eng.begin() as c:
        for table in ("sale_comps", "lease_comps"):
            for col in _COLS:
                c.execute(text(f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS {col}"))
    print("costar_* columns ensured on sale_comps and lease_comps.")


if __name__ == "__main__":
    main()
