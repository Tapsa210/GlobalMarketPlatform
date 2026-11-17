from pyspark.sql import DataFrame, SparkSession

from ingestion.config import load_settings
from processing.spark_session import create_spark_session


def postgres_jdbc_options() -> dict[str, str]:
    settings = load_settings()
    return {
        "url": (
            f"jdbc:postgresql://{settings.postgres_host}:"
            f"{settings.postgres_port}/{settings.postgres_db}"
        ),
        "user": settings.postgres_user,
        "password": settings.postgres_password,
        "driver": "org.postgresql.Driver",
    }


def read_bronze_stock_prices(
    spark: SparkSession | None = None,
) -> DataFrame:
    """Read the Bronze stock table using the configured PostgreSQL connection."""
    session = spark or create_spark_session()
    options = postgres_jdbc_options()
    return (
        session.read.format("jdbc")
        .option("url", options["url"])
        .option("dbtable", "public.bronze_stock_prices")
        .options(**{key: value for key, value in options.items() if key != "url"})
        .load()
    )


def inspect_bronze_stock_prices(df: DataFrame) -> dict[str, object]:
    """Return operational data-quality facts for a Bronze DataFrame."""
    from pyspark.sql import functions as F

    return {
        "row_count": df.count(),
        "symbols": [
            row["symbol"]
            for row in df.select("symbol").distinct().orderBy("symbol").collect()
        ],
        "date_range": df.agg(
            F.min("price_date").alias("min_date"),
            F.max("price_date").alias("max_date"),
        ).first().asDict(),
        "duplicate_keys": df.groupBy("symbol", "price_date").count().filter(
            F.col("count") > 1
        ).count(),
        "null_values": df.select(
            F.sum(F.when(F.col(column).isNull(), 1).otherwise(0)).alias(column)
            for column in df.columns
        ).first().asDict(),
    }


if __name__ == "__main__":
    spark = create_spark_session()
    bronze = read_bronze_stock_prices(spark)
    bronze.printSchema()
    print(inspect_bronze_stock_prices(bronze))
    bronze.show(5, truncate=False)
    spark.stop()
