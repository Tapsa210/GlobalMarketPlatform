from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from database.connection import get_connection
from processing.postgres_reader import postgres_jdbc_options


def build_daily_stock_metrics(silver: DataFrame) -> DataFrame:
    """Create daily reporting metrics from the Silver stock table."""
    required = {
        "symbol",
        "price_date",
        "open_price",
        "high_price",
        "low_price",
        "close_price",
        "volume",
    }
    missing = required.difference(silver.columns)
    if missing:
        raise ValueError(f"Silver DataFrame is missing columns: {sorted(missing)}")

    ordered = Window.partitionBy("symbol").orderBy("price_date")
    return (
        silver.groupBy("symbol", "price_date")
        .agg(
            F.avg("close_price").alias("average_price"),
            F.max("high_price").alias("highest_price"),
            F.min("low_price").alias("lowest_price"),
            F.max("close_price").alias("close_price"),
            F.sum("volume").alias("total_volume"),
        )
        .withColumn("previous_close", F.lag("close_price").over(ordered))
        .withColumn(
            "daily_change",
            F.col("close_price") - F.col("previous_close"),
        )
        .withColumn(
            "daily_change_pct",
            F.when(
                F.col("previous_close") != 0,
                F.col("daily_change") / F.col("previous_close") * 100,
            ),
        )
        .select(
            "symbol",
            "price_date",
            "average_price",
            "highest_price",
            "lowest_price",
            "daily_change",
            "daily_change_pct",
            "total_volume",
        )
    )


def write_gold_stock_metrics(gold: DataFrame) -> None:
    """Upsert Gold metrics through a staging table for safe reruns."""
    options = postgres_jdbc_options()
    staging_table = "gold_daily_stock_metrics_staging"
    gold.write.format("jdbc").options(
        url=options["url"],
        dbtable=staging_table,
        user=options["user"],
        password=options["password"],
        driver=options["driver"],
    ).mode("overwrite").save()

    with get_connection() as connection, connection.cursor() as cursor:
        cursor.execute(
            """
            INSERT INTO gold_daily_stock_metrics (
                symbol, price_date, average_price, highest_price, lowest_price,
                daily_change, daily_change_pct, total_volume
            )
            SELECT symbol, price_date, average_price, highest_price, lowest_price,
                   daily_change, daily_change_pct, total_volume
            FROM gold_daily_stock_metrics_staging
            ON CONFLICT (symbol, price_date) DO UPDATE SET
                average_price = EXCLUDED.average_price,
                highest_price = EXCLUDED.highest_price,
                lowest_price = EXCLUDED.lowest_price,
                daily_change = EXCLUDED.daily_change,
                daily_change_pct = EXCLUDED.daily_change_pct,
                total_volume = EXCLUDED.total_volume;
            """
        )
        cursor.execute(f"DROP TABLE {staging_table};")
