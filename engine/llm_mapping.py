"""LLM-based column mapper (gpt-4o) — Tier-4 fallback replacement.

Reads raw headers + sample data rows + the target schema and returns a mapping
with per-header confidence. Few-shot examples (mined from the corrections corpus)
prime the model with the firm's conventions.
"""
from __future__ import annotations

import json
from typing import Optional

from engine.openai_client import _client
from engine.mapping_examples import format_examples

_MODEL = "gpt-4o"
_LLM_TIMEOUT_SECONDS = 30
_MAX_SAMPLE_ROWS = 5


def _chat_json(prompt: str, model: str = _MODEL) -> dict:
    """Single gpt-4o JSON call. Raises on missing key / API error (caller decides fallback)."""
    resp = _client().chat.completions.create(
        model=model,
        messages=[{"role": "user", "content": prompt}],
        response_format={"type": "json_object"},
        temperature=0,
        max_tokens=1500,
        timeout=_LLM_TIMEOUT_SECONDS,
    )
    return json.loads(resp.choices[0].message.content)


def _build_prompt(headers, sample_rows, schema, file_type, examples) -> str:
    schema_lines = "\n".join(
        f"- {name}: {meta['desc']} (type: {meta['type']})"
        for name, meta in schema.items()
    )
    rows_preview = json.dumps(sample_rows[:_MAX_SAMPLE_ROWS], default=str, indent=2)
    examples_block = format_examples(examples or [])
    examples_section = (
        f"\nKnown header conventions from past analyst corrections "
        f"(prefer these when a header matches):\n{examples_block}\n"
        if examples_block else ""
    )
    return (
        f"You are mapping columns of a commercial real estate {file_type} comp "
        f"spreadsheet to a fixed target schema.\n\n"
        f"TARGET SCHEMA (target_field: description):\n{schema_lines}\n"
        f"{examples_section}\n"
        f"RAW COLUMN HEADERS:\n{json.dumps([str(h) for h in headers])}\n\n"
        f"SAMPLE DATA ROWS (use the VALUES, not just header names, to decide):\n{rows_preview}\n\n"
        "Rules:\n"
        "- Map each raw header to AT MOST ONE target_field from the schema above.\n"
        "- Only use target_field names that appear in the schema. Never invent a field.\n"
        "- If a header does not clearly correspond to any target_field, list it in 'unmapped'.\n"
        "- Use the sample values as evidence (e.g. a column of dollar-per-sf values is rate_psf "
        "even if the header is unclear).\n\n"
        'Return ONLY JSON: {"mappings": {"<raw_header>": "<target_field>"}, '
        '"confidence": {"<raw_header>": 0.0-1.0}, "unmapped": ["<raw_header>"], '
        '"reasoning": "<one sentence>"}'
    )


def llm_map_columns(
    headers: list[str],
    sample_rows: list[dict],
    schema: dict,
    file_type: str,
    examples: Optional[list[dict]] = None,
) -> dict:
    """Map headers to schema via gpt-4o. Returns dict with mappings/confidence/unmapped/reasoning.

    Mappings to fields not in the schema are dropped and moved to 'unmapped' (never fabricate).
    Raises on API error so the caller can fall back to embeddings/heuristic.
    """
    prompt = _build_prompt(headers, sample_rows, schema, file_type, examples)
    raw = _chat_json(prompt)

    valid_targets = set(schema.keys())
    mappings: dict[str, str] = {}
    unmapped = list(raw.get("unmapped") or [])
    for header, target in (raw.get("mappings") or {}).items():
        if target in valid_targets:
            mappings[header] = target
        elif header not in unmapped:
            unmapped.append(header)

    confidence = {
        h: float(raw.get("confidence", {}).get(h, 0.0))
        for h in mappings
    }
    return {
        "mappings": mappings,
        "confidence": confidence,
        "unmapped": unmapped,
        "reasoning": str(raw.get("reasoning", "")),
    }
