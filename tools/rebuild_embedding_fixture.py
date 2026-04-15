"""Build tests/fixtures/embeddings_cache.pkl by intercepting get_embeddings calls."""
import os, pickle, pathlib, hashlib, sys

# Ensure the worktree root is on sys.path so `import comp_engine` finds the worktree copy
_WORKTREE = pathlib.Path(__file__).parent.parent
sys.path.insert(0, str(_WORKTREE))
# Resolve sample files relative to repo root (one level above worktree)
_REPO_ROOT = _WORKTREE.parent.parent.parent
SAMPLE_DIR = _REPO_ROOT / "sample comp files"
if not SAMPLE_DIR.exists():
    SAMPLE_DIR = _WORKTREE / "sample comp files"
OUT = _WORKTREE / "tests/fixtures/embeddings_cache.pkl"
cache: dict[str, list] = {}


def _key(texts):
    return hashlib.sha256("\x00".join(texts).encode("utf-8")).hexdigest()


def main():
    if not os.environ.get("OPENAI_API_KEY"):
        print("OPENAI_API_KEY required", file=sys.stderr); sys.exit(1)

    import comp_engine
    _orig = comp_engine.get_embeddings

    def _wrapped(texts):
        k = _key(list(texts))
        if k not in cache:
            cache[k] = _orig(list(texts))
        return cache[k]

    comp_engine.get_embeddings = _wrapped

    for path in sorted(SAMPLE_DIR.iterdir()):
        if path.suffix.lower() not in {".xlsx", ".xls", ".csv"}:
            continue
        print(f"Processing {path.name} ...")
        try:
            sheets = comp_engine.get_sheet_names(str(path))
            comp_engine.process_all_sheets(str(path), path.name, selected_sheets=sheets)
        except Exception as e:
            print(f"  warn: {e}")

    OUT.parent.mkdir(parents=True, exist_ok=True)
    with OUT.open("wb") as fh:
        pickle.dump(cache, fh)
    print(f"Wrote {len(cache)} entries to {OUT}")


if __name__ == "__main__":
    main()
