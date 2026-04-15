"""Tests for extract_broker in engine/openai_client.py."""
from unittest.mock import MagicMock, patch
from engine.openai_client import extract_broker


def test_extract_broker_returns_name_from_llm():
    fake_resp = MagicMock()
    fake_resp.choices = [MagicMock(message=MagicMock(content='{"broker": "JLL", "confidence": 0.9}'))]

    with patch("engine.openai_client._client") as mock_client:
        mock_client.return_value.chat.completions.create.return_value = fake_resp
        result = extract_broker(sample_text="JLL - DFW Industrial Sales Comps - Rockwall", filename="JLL - DFW.xlsx")

    assert result["broker"] == "JLL"
    assert result["confidence"] >= 0.5


def test_extract_broker_returns_none_on_low_confidence():
    fake_resp = MagicMock()
    fake_resp.choices = [MagicMock(message=MagicMock(content='{"broker": null, "confidence": 0.1}'))]

    with patch("engine.openai_client._client") as mock_client:
        mock_client.return_value.chat.completions.create.return_value = fake_resp
        result = extract_broker(sample_text="generic file", filename="sheet.xlsx")

    assert result["broker"] is None
