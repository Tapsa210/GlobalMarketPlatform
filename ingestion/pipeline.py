
import logging

from database.repository import save_raw_response
from ingestion.api_client import APIClient
from ingestion.config import load_settings
from ingestion.transform import normalize_daily_data
from ingestion.validation import validate_stock_record
from messaging.producer import StockProducer

logger = logging.getLogger(__name__)


def ingest_stock_data(symbol: str) -> int:
    """
    Fetch daily stock data, save the original API response
    into the Bronze layer, normalize all records, and publish
    them to Kafka.
    """

    if not isinstance(symbol, str) or not symbol.strip():
        raise ValueError("Symbol must be a non-empty string.")

    normalized_symbol = symbol.strip().upper()

    client = APIClient()
    response_data = client.get_daily_data(normalized_symbol)

    record_id = save_raw_response(
        symbol=normalized_symbol,
        source="alpha_vantage",
        response_data=response_data,
    )

    records = normalize_daily_data(
        data=response_data,
        symbol=normalized_symbol,
    )
    for record in records:
        validate_stock_record(record)

    producer = StockProducer()

    try:
        for record in records:
            producer.send_stock_record(record)
    finally:
        producer.close()

    return record_id


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )

    settings = load_settings()

    symbols = [
        symbol.strip().upper()
        for symbol in settings.stock_symbols.split(",")
        if symbol.strip()
    ]

    if not symbols:
        raise ValueError("No stock symbols configured.")

    for symbol in symbols:
        try:
            record_id = ingest_stock_data(symbol)

            logger.info(
                "Stock ingestion completed successfully. "
                "Symbol: %s, Raw response ID: %s",
                symbol,
                record_id,
            )

        except Exception:
            logger.exception(
                "Stock ingestion pipeline failed for %s.",
                symbol,
            )
            raise


if __name__ == "__main__":
    main()
