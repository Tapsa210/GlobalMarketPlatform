import pytest

from ingestion.validation import (
    DataValidationError,
    validate_stock_record,
)


@pytest.fixture
def valid_stock_record():
    return {
        "symbol": "AAPL",
        "date": "2026-11-05",
        "open": 270.50,
        "high": 275.00,
        "low": 268.00,
        "close": 273.50,
        "volume": 1500000,
    }


def test_valid_stock_record(valid_stock_record):
    assert validate_stock_record(valid_stock_record) is True


def test_missing_required_field(valid_stock_record):
    del valid_stock_record["close"]

    with pytest.raises(DataValidationError):
        validate_stock_record(valid_stock_record)


def test_negative_price(valid_stock_record):
    valid_stock_record["open"] = -10

    with pytest.raises(DataValidationError):
        validate_stock_record(valid_stock_record)


def test_invalid_high_price(valid_stock_record):
    valid_stock_record["high"] = 260

    with pytest.raises(DataValidationError):
        validate_stock_record(valid_stock_record)


def test_invalid_low_price(valid_stock_record):
    valid_stock_record["low"] = 280

    with pytest.raises(DataValidationError):
        validate_stock_record(valid_stock_record)


def test_invalid_date(valid_stock_record):
    valid_stock_record["date"] = "2026-15-40"

    with pytest.raises(DataValidationError):
        validate_stock_record(valid_stock_record)


def test_negative_volume(valid_stock_record):
    valid_stock_record["volume"] = -100

    with pytest.raises(DataValidationError):
        validate_stock_record(valid_stock_record)


def test_fractional_volume(valid_stock_record):
    valid_stock_record["volume"] = 100.5

    with pytest.raises(DataValidationError):
        validate_stock_record(valid_stock_record)


def test_empty_symbol(valid_stock_record):
    valid_stock_record["symbol"] = ""

    with pytest.raises(DataValidationError):
        validate_stock_record(valid_stock_record)

@pytest.mark.parametrize(
    "invalid_price",
    [
        float("nan"),
        float("inf"),
        float("-inf"),
        True,
    ],
)
def test_invalid_numeric_price(valid_stock_record, invalid_price):
    valid_stock_record["close"] = invalid_price

    with pytest.raises(DataValidationError):
        validate_stock_record(valid_stock_record)