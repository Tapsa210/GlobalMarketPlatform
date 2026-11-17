
import json
import logging

from kafka import KafkaProducer
from kafka.errors import KafkaError
from kafka.serializer import Serializer

from ingestion.config import load_settings

logger = logging.getLogger(__name__)


class StockRecordSerializer(Serializer):

    def serialize(self, topic: str, headers, data: dict) -> bytes:
        record = data

        payload = {
            "symbol": record["symbol"],
            "date": record["date"].isoformat(),
            "open": str(record["open"]),
            "high": str(record["high"]),
            "low": str(record["low"]),
            "close": str(record["close"]),
            "volume": record["volume"],
        }

        return json.dumps(payload).encode("utf-8")


class StockProducer:
    """
    Publish stock market records to a Kafka topic.
    """

    def __init__(
        self,
        bootstrap_servers: str | None = None,
        topic: str = "stock-prices",
    ):
        if bootstrap_servers is None:
            settings = load_settings()
            bootstrap_servers = settings.kafka_bootstrap_servers

        self.topic = topic

        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=StockRecordSerializer(),
            acks="all",
            retries=5,
        )

        logger.info(
            "Kafka producer initialized. Topic: %s",
            self.topic,
        )

    def send_stock_record(self, record: dict) -> None:
        """
        Send one stock record to Kafka.
        """

        try:
            future = self.producer.send(
                self.topic,
                value=record,
                key=record["symbol"].encode("utf-8"),
            )

            metadata = future.get(timeout=10)

            logger.info(
                "Message delivered: topic=%s partition=%s offset=%s",
                metadata.topic,
                metadata.partition,
                metadata.offset,
            )

        except KafkaError:
            logger.exception("Failed to send stock record.")
            raise

    def close(self) -> None:
        """
        Flush pending messages and close the producer.
        """

        try:
            self.producer.flush(timeout=10)
        finally:
            self.producer.close(timeout=10)

        logger.info("Kafka producer closed.")
