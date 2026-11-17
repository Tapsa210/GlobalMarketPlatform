
import logging
from datetime import date
from decimal import Decimal

from messaging.producer import StockProducer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)


def main() -> None:
    sample_record = {
        "symbol": "AAPL",
        "date": date(2026, 11, 8),
        "open": Decimal("275.50"),
        "high": Decimal("280.00"),
        "low": Decimal("273.00"),
        "close": Decimal("278.80"),
        "volume": 46000000,
    }

    producer = StockProducer()

    try:
        producer.send_stock_record(sample_record)
    finally:
        producer.close()


if __name__ == "__main__":
    main()
