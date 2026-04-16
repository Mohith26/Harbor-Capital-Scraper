"""Tests for engine/vision_pdf.py — GPT-4o vision PDF extractor."""
from unittest.mock import MagicMock, patch
import pandas as pd
from engine.vision_pdf import extract_pdf_to_rows


def test_extract_pdf_returns_dataframe_from_vision_response():
    fake_resp = MagicMock()
    fake_resp.choices = [MagicMock(message=MagicMock(content=(
        '{"file_type": "sale", "rows": ['
        '{"property_name": "X", "sale_price": 1000000, "sf": 5000, '
        '"sale_date": "2024-01-01", "address": "123 A St, Houston, TX"}'
        ']}'
    )))]

    with patch("engine.vision_pdf._client") as mock_client, \
         patch("engine.vision_pdf.convert_from_path") as mock_convert:
        mock_convert.return_value = [MagicMock()]  # one fake page image
        mock_client.return_value.chat.completions.create.return_value = fake_resp
        df, file_type = extract_pdf_to_rows("fake.pdf")

    assert file_type == "sale"
    assert len(df) == 1
    assert df.iloc[0]["property_name"] == "X"


def test_extract_pdf_handles_multipage_and_concats():
    page_resp_1 = MagicMock()
    page_resp_1.choices = [MagicMock(message=MagicMock(content=(
        '{"file_type": "lease", "rows": ['
        '{"property_name": "A", "rent_psf": 18, "sf": 1000, "lease_date": "2024-01-01"}]}'
    )))]
    page_resp_2 = MagicMock()
    page_resp_2.choices = [MagicMock(message=MagicMock(content=(
        '{"file_type": "lease", "rows": ['
        '{"property_name": "B", "rent_psf": 19, "sf": 2000, "lease_date": "2024-02-01"}]}'
    )))]

    with patch("engine.vision_pdf._client") as mock_client, \
         patch("engine.vision_pdf.convert_from_path") as mock_convert:
        mock_convert.return_value = [MagicMock(), MagicMock()]
        mock_client.return_value.chat.completions.create.side_effect = [page_resp_1, page_resp_2]
        df, file_type = extract_pdf_to_rows("fake.pdf")

    assert file_type == "lease"
    assert len(df) == 2
    assert set(df["property_name"]) == {"A", "B"}
