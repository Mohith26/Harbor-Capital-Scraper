"""JSON seed I/O helpers for bootstrapping the learning store."""
from __future__ import annotations
import json
from datetime import datetime
from pathlib import Path
from typing import Any


def _to_jsonable(obj: Any) -> Any:
    if isinstance(obj, datetime):
        return obj.isoformat()
    if isinstance(obj, dict):
        return {k: _to_jsonable(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_to_jsonable(x) for x in obj]
    return obj


def dump_store_to_seed(store, seed_dir: str) -> None:
    path = Path(seed_dir)
    path.mkdir(parents=True, exist_ok=True)

    fingerprints = list(getattr(store, "_fingerprints", {}).values())
    corrections = [
        {"file_type": ft, "raw_header": rh, "target_column": tc, "hit_count": cnt}
        for (ft, rh, tc), cnt in getattr(store, "_corrections", {}).items()
    ]
    aliases = list(getattr(store, "_geocode_aliases", {}).values())
    overrides = list(getattr(store, "_geocode_overrides", {}).values())
    brokers = list(getattr(store, "_brokers", {}).values())

    (path / "seed_fingerprints.json").write_text(json.dumps(_to_jsonable(fingerprints), indent=2))
    (path / "seed_corrections.json").write_text(json.dumps(_to_jsonable(corrections), indent=2))
    (path / "seed_geocode_aliases.json").write_text(json.dumps(_to_jsonable(aliases), indent=2))
    (path / "seed_geocode_overrides.json").write_text(json.dumps(_to_jsonable(overrides), indent=2))
    (path / "seed_brokers.json").write_text(json.dumps(_to_jsonable(brokers), indent=2))


def load_seed_into_store(store, seed_dir: str) -> None:
    from engine.types import Fingerprint
    path = Path(seed_dir)
    if not path.exists():
        return

    fp_file = path / "seed_fingerprints.json"
    if fp_file.exists():
        for row in json.loads(fp_file.read_text()):
            fp = Fingerprint(
                raw_hash=row["raw_hash"],
                header_set_hash=row.get("header_set_hash", row["raw_hash"]),
                headers=row.get("normalized_headers", []),
                normalized_headers=row.get("normalized_headers", []),
                file_type=row["file_type"],
                filename=row.get("filename", "seed"),
                sheet_name=row.get("sheet_name"),
            )
            store.record_accepted_mapping(fp, row["mappings"], confirmed_by=row.get("confirmed_by", "seed"))

    corr_file = path / "seed_corrections.json"
    if corr_file.exists():
        for row in json.loads(corr_file.read_text()):
            for _ in range(row.get("hit_count", 1)):
                store.upsert_correction(
                    file_type=row["file_type"],
                    raw_header=row["raw_header"],
                    target_column=row["target_column"],
                    confirmed_by="seed",
                )

    alias_file = path / "seed_geocode_aliases.json"
    if alias_file.exists():
        for row in json.loads(alias_file.read_text()):
            store.insert_geocode_alias(
                raw_text=row["raw_text"],
                canonical_address=row.get("canonical_address", row.get("formatted_addr", "")),
                lat=row.get("latitude", 0.0),
                lng=row.get("longitude", 0.0),
            )

    override_file = path / "seed_geocode_overrides.json"
    if override_file.exists():
        for row in json.loads(override_file.read_text()):
            store.record_geocode_override(
                raw_text=row["raw_text"],
                override_address=row.get("override_address", row.get("corrected_addr", "")),
                lat=row.get("latitude", 0.0),
                lng=row.get("longitude", 0.0),
                confirmed_by=row.get("confirmed_by", "seed"),
            )

    broker_file = path / "seed_brokers.json"
    if broker_file.exists():
        for row in json.loads(broker_file.read_text()):
            bid = store.upsert_broker(row["canonical_name"], confirmed_by=row.get("confirmed_by", "seed"))
            for alias in row.get("aliases", []):
                store.record_broker_correction(alias, row["canonical_name"], "seed")
