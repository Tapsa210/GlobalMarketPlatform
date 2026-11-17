
from datetime import date
from decimal import Decimal

from database.connection import get_connection


def save_stock_record(record: dict) -> bool:
    """
    Save one stock record into the Bronze stock prices table.

    Returns:
        True if a new record was inserted.
        False if the record already exists.
    """

    query = """
        INSERT INTO bronze_stock_prices (
            symbol,
            price_date,
            open_price,
            high_price,
            low_price,
            close_price,
            volume
        )
        VALUES (
            %(symbol)s,
            %(price_date)s,
            %(open_price)s,
            %(high_price)s,
            %(low_price)s,
            %(close_price)s,
            %(volume)s
        )
        ON CONFLICT (symbol, price_date)
        DO NOTHING
        RETURNING id;
    """

    params = {
        "symbol": record["symbol"],
        "price_date": date.fromisoformat(record["date"]),
        "open_price": Decimal(record["open"]),
        "high_price": Decimal(record["high"]),
        "low_price": Decimal(record["low"]),
        "close_price": Decimal(record["close"]),
        "volume": record["volume"],
    }

    with get_connection() as connection, connection.cursor() as cursor:
        cursor.execute(query, params)
        result = cursor.fetchone()

        return result is not None
