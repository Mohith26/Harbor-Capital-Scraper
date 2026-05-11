"""PDF → schema rows via GPT-4o vision.

Assumes poppler-utils is installed (Dockerfile adds it in Task 0.2).
"""
from __future__ import annotations

import base64
import hashlib
import io
import json
import time
from typing import Optional

import pandas as pd
from pdf2image import convert_from_path, pdfinfo_from_path

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


def _first_present(row: dict, *keys: str):
    for key in keys:
        value = row.get(key)
        if value not in (None, ""):
            return value
    return None


def _join_address_parts(*parts) -> str | None:
    cleaned = [str(p).strip() for p in parts if p not in (None, "") and str(p).strip()]
    return ", ".join(cleaned) if cleaned else None


def _normalize_row(row: dict, file_type: str) -> dict:
    """Translate vision prompt keys into the app's canonical DB schema."""
    ft = (file_type or "lease").lower()
    address = _first_present(row, "address", "property_address", "property_name", "property")
    raw_address_data = _join_address_parts(
        _first_present(row, "property_name", "property"),
        row.get("address"),
        row.get("city"),
        row.get("state"),
        _first_present(row, "zip", "zip_code"),
    )

    if ft == "sale":
        return {
            "address": address,
            "sale_price": _first_present(row, "sale_price", "price"),
            "building_size": _first_present(row, "sf", "building_size", "size"),
            "price_per_sf": _first_present(row, "psf", "price_per_sf", "price_psf"),
            "closing_date": _first_present(row, "sale_date", "closing_date", "date"),
            "buyer": row.get("buyer"),
            "seller": row.get("seller"),
            "cap_rate": row.get("cap_rate"),
            "city": row.get("city"),
            "zip_code": _first_present(row, "zip_code", "zip"),
            "raw_address_data": raw_address_data or address,
        }

    return {
        "address": address,
        "tenant_name": _first_present(row, "tenant_name", "tenant"),
        "rate_psf": _first_present(row, "rate_psf", "rent_psf", "rent"),
        "leased_sf": _first_present(row, "leased_sf", "sf", "size"),
        "commencement_date": _first_present(row, "commencement_date", "lease_date", "date"),
        "lease_type": row.get("lease_type"),
        "term_months": row.get("term_months"),
        "city": row.get("city"),
        "zip_code": _first_present(row, "zip_code", "zip"),
        "raw_address_data": raw_address_data or address,
    }


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
    total_timeout_seconds: Optional[int] = 120,
    raster_timeout_seconds: Optional[int] = 20,
    openai_timeout_seconds: Optional[int] = 45,
    dpi: int = 150,
) -> tuple[pd.DataFrame, str]:
    """Rasterize a PDF and send each page to GPT-4o vision.

    Returns (DataFrame of schema-shaped rows, file_type string).
    """
    all_rows: list[dict] = []
    file_type: Optional[str] = None
    warnings: list[str] = []
    deadline = (
        time.monotonic() + total_timeout_seconds
        if total_timeout_seconds and total_timeout_seconds > 0
        else None
    )

    def _remaining_timeout(default_timeout: Optional[int]) -> Optional[int]:
        if deadline is None:
            return default_timeout
        remaining = int(deadline - time.monotonic())
        if remaining <= 0:
            return 0
        if default_timeout is None or default_timeout <= 0:
            return remaining
        return max(1, min(default_timeout, remaining))

    try:
        info = pdfinfo_from_path(pdf_path, timeout=_remaining_timeout(raster_timeout_seconds))
        page_count = int(info.get("Pages", 1))
    except Exception as exc:
        page_count = max_pages or 1
        warnings.append(f"Could not read PDF page count: {exc}")

    pages_to_process = min(page_count, max_pages) if max_pages else page_count
    if max_pages and page_count > max_pages:
        warnings.append(f"Processing first {max_pages} of {page_count} PDF pages.")

    pages_processed = 0

    for page_number in range(1, pages_to_process + 1):
        if deadline is not None and time.monotonic() >= deadline:
            warnings.append("PDF processing stopped because the total timeout was reached.")
            break

        raster_timeout = _remaining_timeout(raster_timeout_seconds)
        if raster_timeout == 0:
            warnings.append("PDF processing stopped before rasterizing the next page.")
            break

        try:
            pages = convert_from_path(
                pdf_path,
                dpi=dpi,
                first_page=page_number,
                last_page=page_number,
                timeout=raster_timeout,
            )
        except Exception as exc:
            warnings.append(f"Stopped PDF rasterization at page {page_number}: {exc}")
            break
        if not pages:
            continue

        page = pages[0]
        b64 = _encode_image(page)
        request_timeout = _remaining_timeout(openai_timeout_seconds)
        if request_timeout == 0:
            warnings.append("PDF processing stopped before the next vision request.")
            break
        try:
            client = _client()
            if request_timeout:
                client = client.with_options(timeout=request_timeout)
            resp = client.chat.completions.create(
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
        except Exception as exc:
            warnings.append(f"Vision extraction skipped page {page_number}: {exc}")
            continue
        try:
            parsed = json.loads(resp.choices[0].message.content)
        except (json.JSONDecodeError, TypeError, AttributeError, IndexError):
            warnings.append(f"Vision extraction returned invalid JSON on page {page_number}.")
            continue
        if file_type is None:
            file_type = parsed.get("file_type", "lease")
        rows = parsed.get("rows", [])
        if not isinstance(rows, list):
            warnings.append(f"Vision extraction returned invalid rows on page {page_number}.")
            continue
        page_file_type = parsed.get("file_type", file_type)
        all_rows.extend(_normalize_row(r, page_file_type) for r in rows if isinstance(r, dict))
        pages_processed += 1

    df = pd.DataFrame(all_rows)
    df.attrs["warnings"] = warnings
    df.attrs["pdf_pages_total"] = page_count
    df.attrs["pdf_pages_attempted"] = pages_to_process
    df.attrs["pdf_pages_processed"] = pages_processed
    return df, (file_type or "lease")
