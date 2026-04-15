"""Load seed data from `learning_data/seed/` into the learning store.

Idempotent — safe to re-run. Uses SqliteLearningStore at `learning_local.db`
by default; override with LEARNING_DB_URL. Override seed directory with
LEARNING_SEED_DIR.
"""
from __future__ import annotations

import os
import sys

sys.path.insert(0, ".")

from learning.store import SqliteLearningStore


def main():
    seed_dir = os.environ.get("LEARNING_SEED_DIR", "learning_data/seed")
    db_url = os.environ.get("LEARNING_DB_URL", "sqlite:///learning_local.db")
    store = SqliteLearningStore(engine_url=db_url)
    store.load_seed(seed_dir)
    print(f"loaded seed from {seed_dir} into {db_url}")


if __name__ == "__main__":
    main()
