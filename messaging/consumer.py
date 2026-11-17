
import json
import logging

from kafka import KafkaConsumer
from kafka.serializer import Deserializer

from database.stock_repository import save_stock_record
from ingestion.config import load_settings

logger = logging.getLogger(__name__)


class StockRecordDeserializer(Deserializer):
    def deserialize(self, topic: str, headers, data) -> dict:
        if isinstance(data, bytes):
            message_bytes = data
        else:
            message_bytes = bytes(data)

        return json.loads(message_bytes.decode("utf-8"))


class StockConsumer:
    def __init__(
        self,
        bootstrap_servers: str | None = None,
        topic: str = "stock-prices",
        group_id: str = "stock-consumer-group",
    ):
        if bootstrap_servers is None:
            settings = load_settings()
            bootstrap_servers = settings.kafka_bootstrap_servers

        self.consumer = KafkaConsumer(
            topic,
            bootstrap_servers=bootstrap_servers,
            group_id=group_id,
            auto_offset_reset="earliest",
            enable_auto_commit=False,
            value_deserializer=StockRecordDeserializer(),
        )

        logger.info(
            "Kafka consumer initialized. Topic: %s",
            topic,
        )

    def consume_messages(self):
        """
        Continuously consume stock records from Kafka.
        """

        try:
            for message in self.consumer:
                record = message.value

                logger.info(
                    "Received Kafka message: topic=%s partition=%s offset=%s "
                    "symbol=%s date=%s",
                    message.topic,
                    message.partition,
                    message.offset,
                    record.get("symbol"),
                    record.get("date"),
                )

                is_inserted = save_stock_record(record)

                if is_inserted:
                    logger.info(
                        "New stock record inserted: symbol=%s date=%s",
                        record.get("symbol"),
                        record.get("date"),
                    )
                else:
                    logger.info(
                        "Duplicate stock record ignored: symbol=%s date=%s",
                        record.get("symbol"),
                        record.get("date"),
                    )

                self.consumer.commit()

                logger.info(
                    "Kafka offset committed: topic=%s partition=%s offset=%s",
                    message.topic,
                    message.partition,
                    message.offset,
                )

        except KeyboardInterrupt:
            logger.info("Consumer stopped by user.")

        finally:
            self.consumer.close()
            logger.info("Kafka consumer closed.")


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )

    consumer = StockConsumer()
    consumer.consume_messages()
