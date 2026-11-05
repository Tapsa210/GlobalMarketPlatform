import math
from datetime import date
from decimal import Decimal


class DataValidationError(ValueError):
    """Raised when stock market data fails validation."""


def validate_stock_record(record: dict) -> bool:
    required_fields = {
        "symbol",
        "date",
        "open",
        "high",
        "low",
        "close",
        "volume",
    }

    if not isinstance(record, dict):
        raise DataValidationError("Record must be a dictionary.")

    missing_fields = required_fields - record.keys()
    if missing_fields:
        raise DataValidationError(
            f"Missing required fields: {sorted(missing_fields)}"
        )

    symbol = record["symbol"]
    if not isinstance(symbol, str) or not symbol.strip():
        raise DataValidationError("Symbol must be a non-empty string.")

    record_date = record["date"]

    if isinstance(record_date, date):
        parsed_date = record_date

    elif isinstance(record_date, str):
        try:
            parsed_date = date.fromisoformat(record_date)
        except ValueError:
            raise DataValidationError(
                "Date must use YYYY-MM-DD format."
            )

        if parsed_date.isoformat() != record_date:
            raise DataValidationError(
                "Date must use YYYY-MM-DD format."
            )

    else:
        raise DataValidationError(
            "Date must be a date object or YYYY-MM-DD string."
        )

    # --- الجزء الجديد الذي تسأل عنه ---
    prices = {
        price_field: record[price_field]
        for price_field in ("open", "high", "low", "close")
    }

    for price_field, value in prices.items():
        if isinstance(value, bool) or not isinstance(
            value, (int, float, Decimal)
        ):
            raise DataValidationError(
                f"{price_field} must be a numeric value."
            )
        if not math.isfinite(value):
            raise DataValidationError(f"{price_field} must be a finite number.")
        if value <= 0:
            raise DataValidationError(f"{price_field} must be greater than zero.")
    # -------------------------------

    if record["high"] < max(prices.values()):
        raise DataValidationError(
            "High must be greater than or equal to all prices."
        )

    if record["low"] > min(prices.values()):
        raise DataValidationError(
            "Low must be less than or equal to all prices."
        )

    volume = record["volume"]
    if isinstance(volume, bool) or not isinstance(volume, (int, float, Decimal)):
        raise DataValidationError("Volume must be a numeric value.")
    if isinstance(volume, float) and not math.isfinite(volume):
        raise DataValidationError("Volume must be a finite number.")
    if isinstance(volume, Decimal) and not volume.is_finite():
        raise DataValidationError("Volume must be a finite number.")
    if volume < 0:
        raise DataValidationError("Volume cannot be negative.")
    if volume % 1 != 0:
        raise DataValidationError("Volume must be a whole number.")

    return True