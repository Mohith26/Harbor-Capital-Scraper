"""Run BEFORE Task 2.6 facade rewrite. Captures current outputs for equivalence."""
import pathlib, pickle, sys, hashlib

_WORKTREE = pathlib.Path(__file__).parent.parent
_REPO_ROOT = _WORKTREE.parent.parent.parent
sys.path.insert(0, str(_WORKTREE))

SAMPLE_DIR = _REPO_ROOT / "sample comp files"
if not SAMPLE_DIR.exists():
    SAMPLE_DIR = _WORKTREE / "sample comp files"
OUT = _WORKTREE / "tests/fixtures/engine_output_snapshot.pkl"

import comp_engine

# Replay from embedding cache so snapshot is deterministic
cache_path = _WORKTREE / "tests/fixtures/embeddings_cache.pkl"
if cache_path.exists():
    with cache_path.open("rb") as fh:
        _cache = pickle.load(fh)
    def _key(texts):
        return hashlib.sha256("\x00".join(texts).encode("utf-8")).hexdigest()
    def _fake(texts):
        k = _key(list(texts))
        return _cache.get(k, comp_engine._get_openai_client().embeddings.create(
            input=texts, model="text-embedding-3-small"
        ).data[0].embedding if k not in _cache else _cache[k])
    import numpy as np
    def _fake2(texts):
        k = _key(list(texts))
        if k in _cache:
            return _cache[k]
        raise KeyError(f"cache miss for {texts}")
    comp_engine.get_embeddings = _fake2

snapshot = {}
for path in sorted(SAMPLE_DIR.iterdir()):
    if path.suffix.lower() != ".xlsx":
        continue
    try:
        sheets = comp_engine.get_sheet_names(str(path))
        result = comp_engine.process_all_sheets(str(path), path.name, selected_sheets=sheets)
        combined, confidences, mappings, errors = result if result else (None, None, {}, [])
        if combined is not None:
            for sheet_label, sheet_mappings in mappings.items():
                key = f"{path.name}::{sheet_label}"
                sheet_rows = combined[combined['source_sheet'] == sheet_label] if 'source_sheet' in combined.columns else combined
                snapshot[key] = {"clean_df": sheet_rows.copy(), "mappings": sheet_mappings}
    except Exception as e:
        print(f"warn {path.name}: {e}")

OUT.parent.mkdir(parents=True, exist_ok=True)
with OUT.open("wb") as fh:
    pickle.dump(snapshot, fh)
print(f"Captured {len(snapshot)} snapshot entries -> {OUT}")
