from datetime import date
from decimal import Decimal

from ingestion.transform import normalize_daily_data
from ingestion.validation import validate_stock_record


def test_api_data_normalization_and_validation():
    raw_api_data = {
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

    records = normalize_daily_data(raw_api_data, "AAPL")

    assert len(records) == 2

    for record in records:
        assert validate_stock_record(record) is True

    first_record = records[0]

    assert first_record["symbol"] == "AAPL"
    assert first_record["date"] == date(2026, 11, 3)
    assert isinstance(first_record["open"], Decimal)
    assert isinstance(first_record["volume"], int)