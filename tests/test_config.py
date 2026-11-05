import pytest

from ingestion.config import (
    ConfigurationError,
    load_settings,
)


def test_load_settings_with_valid_api_key(monkeypatch):
    monkeypatch.setenv("ALPHA_VANTAGE_API_KEY", "test_key_123")

    settings = load_settings()

    assert settings.alpha_vantage_api_key == "test_key_123"


def test_load_settings_when_api_key_is_missing(monkeypatch):
    monkeypatch.delenv("ALPHA_VANTAGE_API_KEY", raising=False)

    with pytest.raises(ConfigurationError):
        load_settings()


def test_load_settings_rejects_placeholder(monkeypatch):
    monkeypatch.setenv(
        "ALPHA_VANTAGE_API_KEY",
        "your_api_key_here"
    )

    with pytest.raises(ConfigurationError):
        load_settings()
