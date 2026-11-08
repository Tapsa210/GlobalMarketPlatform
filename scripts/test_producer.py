
import logging
from datetime import date
from decimal import Decimal

from messaging.producer import StockProducer


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)


sample_record = {
    "symbol": "AAPL",
    "date": date(2026, 11, 7),
    "open": Decimal("270.50"),
    "high": Decimal("275.00"),
    "low": Decimal("268.00"),
    "close": Decimal("273.80"),
    "volume": 45000000,
}


producer = StockProducer()

try:
    producer.send_stock_record(sample_record)
finally:
    producer.close()
