"""Seed JSON serialization round-trip tests."""
import json
import tempfile
from pathlib import Path
import pytest
from learning.fakes import FakeLearningStore
from learning.seed_io import dump_store_to_seed, load_seed_into_store
from engine.types import Fingerprint


def _fp(raw_hash, headers, file_type="lease"):
    return Fingerprint(
        raw_hash=raw_hash, header_set_hash=raw_hash+"_set",
        headers=headers, normalized_headers=[h.lower() for h in headers],
        file_type=file_type, filename="f.xlsx", sheet_name="S1",
    )


def test_roundtrip_fingerprint():
    src = FakeLearningStore()
    src.record_accepted_mapping(_fp("h1", ["Property", "Rent"]), {"Property": "property_name"}, confirmed_by="seed")

    with tempfile.TemporaryDirectory() as tmp:
        dump_store_to_seed(src, tmp)
        fp_file = Path(tmp) / "seed_fingerprints.json"
        assert fp_file.exists()
        data = json.loads(fp_file.read_text())
        assert len(data) == 1

        dst = FakeLearningStore()
        load_seed_into_store(dst, tmp)
        got = dst.get_fingerprint_by_hash("h1")
        assert got is not None
        assert got["mappings"]["Property"] == "property_name"


def test_load_seed_handles_missing_dir():
    dst = FakeLearningStore()
    load_seed_into_store(dst, "/nonexistent/path")
    assert dst.get_fingerprint_by_hash("anything") is None
