import logging

from processing.gold import build_daily_stock_metrics, write_gold_stock_metrics
from processing.postgres_reader import read_bronze_stock_prices
from processing.silver import clean_bronze_stock_prices, write_silver_stock_prices
from processing.spark_session import create_spark_session

logger = logging.getLogger(__name__)


def run_silver_gold_pipeline() -> None:
    spark = create_spark_session()
    try:
        bronze = read_bronze_stock_prices(spark)
        silver = clean_bronze_stock_prices(bronze).cache()
        write_silver_stock_prices(silver)
        write_gold_stock_metrics(build_daily_stock_metrics(silver))
        logger.info(
            "Spark processing completed: bronze=%s silver=%s",
            bronze.count(),
            silver.count(),
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )
    run_silver_gold_pipeline()
