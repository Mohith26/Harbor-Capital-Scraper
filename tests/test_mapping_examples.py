from engine.mapping_examples import build_examples, format_examples
from learning.fakes import FakeLearningStore


def test_build_examples_returns_top_k_by_hit_count():
    store = FakeLearningStore()
    for _ in range(3):
        store.upsert_correction("LEASE", "asking rate", "rate_psf", "u")     # hit=3
    store.upsert_correction("LEASE", "deal sf", "leased_sf", "u")            # hit=1
    store.upsert_correction("LEASE", "esc %", "escalations", "u")            # hit=1

    examples = build_examples(store, "LEASE", k=2)

    assert len(examples) == 2
    assert examples[0] == {"raw_header": "asking rate", "target_column": "rate_psf"}


def test_build_examples_empty_store_returns_empty():
    assert build_examples(FakeLearningStore(), "SALE") == []


def test_format_examples_renders_arrow_lines():
    text = format_examples([
        {"raw_header": "base rent $/sf", "target_column": "rate_psf"},
        {"raw_header": "pp", "target_column": "sale_price"},
    ])
    assert '"base rent $/sf" -> rate_psf' in text
    assert '"pp" -> sale_price' in text


def test_format_examples_empty_returns_empty_string():
    assert format_examples([]) == ""
