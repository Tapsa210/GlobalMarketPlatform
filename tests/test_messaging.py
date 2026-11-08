import json
from datetime import date
from decimal import Decimal

from messaging.consumer import StockRecordDeserializer
from messaging.producer import StockRecordSerializer


def test_stock_record_deserializer_uses_payload_after_headers():
    payload = {"symbol": "AAPL", "date": "2026-11-07"}

    result = StockRecordDeserializer().deserialize(
        "stock-prices",
        [],
        json.dumps(payload).encode("utf-8"),
    )

    assert result == payload


def test_stock_record_serializer_uses_payload_after_headers():
    record = {
        "symbol": "AAPL",
        "date": date(2026, 11, 7),
        "open": Decimal("270.50"),
        "high": Decimal("275.00"),
        "low": Decimal("268.00"),
        "close": Decimal("273.80"),
        "volume": 45000000,
    }

    result = StockRecordSerializer().serialize("stock-prices", [], record)

    assert json.loads(result) == {
        "symbol": "AAPL",
        "date": "2026-11-07",
        "open": "270.50",
        "high": "275.00",
        "low": "268.00",
        "close": "273.80",
        "volume": 45000000,
    }
