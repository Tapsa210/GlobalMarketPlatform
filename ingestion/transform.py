
from datetime import date
from decimal import Decimal, InvalidOperation


class TransformationError(ValueError):
    """Raised when API data cannot be normalized."""


def normalize_daily_data(data: dict, symbol: str) -> list[dict]:
    if not isinstance(data, dict):
        raise TransformationError("API data must be a dictionary.")

    time_series = data.get("Time Series (Daily)")

    if not isinstance(time_series, dict) or not time_series:
        raise TransformationError("Daily time series is missing or empty.")

    if not isinstance(symbol, str) or not symbol.strip():
        raise TransformationError("Symbol must be a non-empty string.")

    records = []

    for record_date, values in time_series.items():
        if not isinstance(values, dict):
            raise TransformationError(
                f"Invalid record for date {record_date}."
            )

        try:
            parsed_date = date.fromisoformat(record_date)

            open_price = Decimal(values["1. open"])
            high_price = Decimal(values["2. high"])
            low_price = Decimal(values["3. low"])
            close_price = Decimal(values["4. close"])
            volume = Decimal(values["5. volume"])

            if not volume.is_finite() or volume < 0 or volume % 1 != 0:
                raise TransformationError(
                    f"Failed to normalize record for {record_date}."
                )

        except (ValueError, KeyError, TypeError, InvalidOperation) as error:
            raise TransformationError(
                f"Failed to normalize record for {record_date}."
            ) from error

        record = {
            "symbol": symbol.strip().upper(),
            "date": parsed_date,
            "open": open_price,
            "high": high_price,
            "low": low_price,
            "close": close_price,
            "volume": int(volume),
        }

        records.append(record)

    records.sort(key=lambda item: item["date"])

    return records
