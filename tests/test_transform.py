
from datetime import date
from decimal import Decimal

import pytest

from ingestion.transform import (
    TransformationError,
    normalize_daily_data,
)


@pytest.fixture
def sample_api_response():
    return {
        "Time Series (Daily)": {
            "2026-11-04": {
                "1. open": "273.50",
                "2. high": "278.20",
                "3. low": "271.10",
                "4. close": "276.40",
                "5. volume": "45230000",
            },
            "2026-11-03": {
                "1. open": "270.00",
                "2. high": "274.00",
                "3. low": "268.50",
                "4. close": "273.00",
                "5. volume": "39000000",
            },
        }
    }


def test_normalize_daily_data(sample_api_response):
    records = normalize_daily_data(sample_api_response, "AAPL")

    assert len(records) == 2
    assert records[0]["symbol"] == "AAPL"
    assert records[0]["date"] == date(2026, 11, 3)
    assert records[0]["open"] == Decimal("270.00")
    assert records[0]["volume"] == 39000000


def test_normalize_daily_data_sorts_dates(sample_api_response):
    records = normalize_daily_data(sample_api_response, "AAPL")

    assert records[0]["date"] < records[1]["date"]


def test_normalize_rejects_missing_time_series():
    with pytest.raises(TransformationError):
        normalize_daily_data({}, "AAPL")


def test_normalize_rejects_invalid_price(sample_api_response):
    sample_api_response["Time Series (Daily)"]["2026-11-04"][
        "1. open"
    ] = "invalid"

    with pytest.raises(TransformationError):
        normalize_daily_data(sample_api_response, "AAPL")


def test_normalize_rejects_invalid_symbol(sample_api_response):
    with pytest.raises(TransformationError):
        normalize_daily_data(sample_api_response, "")
