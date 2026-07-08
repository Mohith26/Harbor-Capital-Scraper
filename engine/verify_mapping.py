"""Verifier pass — a second gpt-4o call that checks a proposed mapping against sample values."""
from __future__ import annotations

import json

from engine.llm_mapping import _chat_json

_MODEL = "gpt-4o"


def _build_prompt(mappings, sample_rows, schema) -> str:
    schema_lines = "\n".join(
        f"- {name}: {meta['desc']} (type: {meta['type']})"
        for name, meta in schema.items()
    )
    return (
        "You are auditing a proposed column mapping for a commercial real estate comp file.\n\n"
        f"TARGET SCHEMA:\n{schema_lines}\n\n"
        f"PROPOSED MAPPING (raw_header -> target_field):\n{json.dumps(mappings, indent=2)}\n\n"
        f"SAMPLE DATA ROWS:\n{json.dumps(sample_rows[:5], default=str, indent=2)}\n\n"
        "Find contradictions where the column's VALUES do not match the assigned target_field "
        "(e.g. square footage mapped to a price field, a non-date mapped to a date field, "
        "a monthly rate mapped where an annual rate is expected, or two headers mapped to the "
        "same field).\n\n"
        'Return ONLY JSON: {"adjusted_confidence": {"<raw_header>": 0.0-1.0}, '
        '"flags": [{"header": "<raw_header>", "reason": "<why it is suspicious>", '
        '"suggested_field": "<the target_field the VALUES actually look like, chosen '
        'ONLY from the target schema above, or null if unclear>"}]}. '
        "Only include headers you are adjusting or flagging."
    )


def verify_mapping(mappings: dict[str, str], sample_rows: list[dict], schema: dict) -> dict:
    """Audit a mapping against sample values. Degrades to a no-op result on any error."""
    if not mappings:
        return {"adjusted_confidence": {}, "flags": []}
    try:
        raw = _chat_json(_build_prompt(mappings, sample_rows, schema), model=_MODEL)
    except Exception:
        return {"adjusted_confidence": {}, "flags": []}

    adjusted = {
        h: float(v)
        for h, v in (raw.get("adjusted_confidence") or {}).items()
        if h in mappings
    }
    flags = []
    for f in (raw.get("flags") or []):
        header = f.get("header", "")
        if header not in mappings:
            continue
        suggested = f.get("suggested_field")
        if not isinstance(suggested, str) or not suggested.strip():
            suggested = None
        flags.append({
            "header": header,
            "reason": f.get("reason", ""),
            "suggested_field": suggested,
        })
    return {"adjusted_confidence": adjusted, "flags": flags}
