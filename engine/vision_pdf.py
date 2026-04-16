"""PDF → schema rows via GPT-4o vision.

Assumes poppler-utils is installed (Dockerfile adds it in Task 0.2).
"""
from __future__ import annotations

import base64
import hashlib
import io
import json
from typing import Optional

import pandas as pd
from pdf2image import convert_from_path

from engine.openai_client import _client


VISION_PROMPT = (
    "This is one page of a commercial real estate comp file. Extract every row "
    "of tabular data into JSON. First, determine whether the page shows LEASE "
    "comps or SALE comps. Then, for each row, return these fields (null for "
    "missing):\n"
    "  LEASE: property_name, tenant, rent_psf, rate_basis, sf, lease_date, "
    "address, city, state, zip, lease_type, term_months\n"
    "  SALE: property_name, sale_price, sf, psf, sale_date, address, city, "
    "state, zip, buyer, seller, cap_rate\n"
    'Return strictly: {"file_type": "lease"|"sale", "rows": [...]}. '
    "Do not include any other commentary."
)


def _encode_image(pil_image) -> str:
    buf = io.BytesIO()
    pil_image.save(buf, format="PNG")
    return base64.b64encode(buf.getvalue()).decode("utf-8")


def _pdf_content_hash(pdf_path: str) -> str:
    h = hashlib.sha256()
    with open(pdf_path, "rb") as fh:
        for chunk in iter(lambda: fh.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def extract_pdf_to_rows(
    pdf_path: str,
    max_pages: Optional[int] = None,
) -> tuple[pd.DataFrame, str]:
    """Rasterize a PDF and send each page to GPT-4o vision.

    Returns (DataFrame of schema-shaped rows, file_type string).
    """
    pages = convert_from_path(pdf_path, dpi=200)
    if max_pages:
        pages = pages[:max_pages]

    all_rows: list[dict] = []
    file_type: Optional[str] = None

    for page in pages:
        b64 = _encode_image(page)
        resp = _client().chat.completions.create(
            model="gpt-4o",
            messages=[{
                "role": "user",
                "content": [
                    {"type": "text", "text": VISION_PROMPT},
                    {"type": "image_url",
                     "image_url": {"url": f"data:image/png;base64,{b64}"}},
                ],
            }],
            response_format={"type": "json_object"},
            temperature=0,
            max_tokens=4000,
        )
        try:
            parsed = json.loads(resp.choices[0].message.content)
        except (json.JSONDecodeError, AttributeError, IndexError):
            continue
        if file_type is None:
            file_type = parsed.get("file_type", "lease")
        all_rows.extend(parsed.get("rows", []))

    df = pd.DataFrame(all_rows)
    return df, (file_type or "lease")
