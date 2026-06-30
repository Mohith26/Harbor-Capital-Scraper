"""Mine the corrections corpus into few-shot examples for the LLM mapper."""
from __future__ import annotations


def build_examples(store, file_type: str, k: int = 20) -> list[dict]:
    """Return up to k highest-confidence corrections as {raw_header, target_column}."""
    if store is None:
        return []
    rows = store.get_all_corrections(file_type)
    return [
        {"raw_header": r["raw_header"], "target_column": r["target_column"]}
        for r in rows[:k]
    ]


def format_examples(examples: list[dict]) -> str:
    """Render examples as one '"header" -> target' line each (empty string if none)."""
    if not examples:
        return ""
    return "\n".join(
        f'"{e["raw_header"]}" -> {e["target_column"]}' for e in examples
    )
