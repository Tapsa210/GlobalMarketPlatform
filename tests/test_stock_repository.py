
from datetime import date
from decimal import Decimal

from database.connection import get_connection
from database.stock_repository import save_stock_record


def test_save_stock_record():
    record = {
        "symbol": "AAPL",
        "date": "2026-11-07",
        "open": "270.50",
        "high": "275.00",
        "low": "268.00",
        "close": "273.80",
        "volume": 45000000,
    }

    save_stock_record(record)

    with get_connection() as connection, connection.cursor() as cursor:
        cursor.execute(
            """
                SELECT symbol, price_date, open_price,
                       high_price, low_price, close_price, volume
                FROM bronze_stock_prices
                WHERE symbol = %s AND price_date = %s;
                """,
            ("AAPL", date(2026, 11, 7)),
        )

        result = cursor.fetchone()

    assert result is not None
    assert result[0] == "AAPL"
    assert result[1] == date(2026, 11, 7)
    assert result[2] == Decimal("270.500000")
    assert result[6] == 45000000


def test_duplicate_stock_record_is_not_inserted():
    record = {
        "symbol": "MSFT",
        "date": "2026-11-07",
        "open": "500.00",
        "high": "510.00",
        "low": "495.00",
        "close": "505.00",
        "volume": 30000000,
    }

    save_stock_record(record)
    save_stock_record(record)

    with get_connection() as connection, connection.cursor() as cursor:
        cursor.execute(
            """
                SELECT COUNT(*)
                FROM bronze_stock_prices
                WHERE symbol = %s AND price_date = %s;
                """,
            ("MSFT", date(2026, 11, 7)),
        )

        count = cursor.fetchone()[0]

    assert count == 1
