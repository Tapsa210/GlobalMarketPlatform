from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType, LongType

from database.connection import get_connection
from processing.postgres_reader import postgres_jdbc_options

SILVER_COLUMNS = [
    "symbol",
    "price_date",
    "open_price",
    "high_price",
    "low_price",
    "close_price",
    "volume",
]


def clean_bronze_stock_prices(bronze: DataFrame) -> DataFrame:
    """Validate, normalize, and deduplicate Bronze stock prices."""
    required = set(SILVER_COLUMNS)
    missing = required.difference(bronze.columns)
    if missing:
        raise ValueError(f"Bronze DataFrame is missing columns: {sorted(missing)}")

    typed = bronze.select(
        F.upper(F.trim("symbol")).alias("symbol"),
        F.to_date("price_date").alias("price_date"),
        F.col("open_price").cast(DecimalType(18, 6)).alias("open_price"),
        F.col("high_price").cast(DecimalType(18, 6)).alias("high_price"),
        F.col("low_price").cast(DecimalType(18, 6)).alias("low_price"),
        F.col("close_price").cast(DecimalType(18, 6)).alias("close_price"),
        F.col("volume").cast(LongType()).alias("volume"),
        *(
            [F.col("ingested_at")]
            if "ingested_at" in bronze.columns
            else [F.current_timestamp().alias("ingested_at")]
        ),
    )

    valid = typed.filter(
        F.col("symbol").isNotNull()
        & (F.col("symbol") != "")
        & F.col("price_date").isNotNull()
        & F.col("open_price").isNotNull()
        & F.col("high_price").isNotNull()
        & F.col("low_price").isNotNull()
        & F.col("close_price").isNotNull()
        & F.col("volume").isNotNull()
        & (F.col("open_price") > 0)
        & (F.col("high_price") > 0)
        & (F.col("low_price") > 0)
        & (F.col("close_price") > 0)
        & (F.col("high_price") >= F.col("low_price"))
        & (F.col("volume") >= 0)
    )

    latest = Window.partitionBy("symbol", "price_date").orderBy(
        F.col("ingested_at").desc()
    )
    return (
        valid.withColumn("_row_number", F.row_number().over(latest))
        .filter(F.col("_row_number") == 1)
        .drop("_row_number", "ingested_at")
        .select(*SILVER_COLUMNS)
    )


def write_silver_stock_prices(silver: DataFrame) -> None:
    """Upsert Silver records through a staging table to make reruns idempotent."""
    options = postgres_jdbc_options()
    staging_table = "silver_stock_prices_staging"
    silver.write.format("jdbc").options(
        url=options["url"],
        dbtable=staging_table,
        user=options["user"],
        password=options["password"],
        driver=options["driver"],
    ).mode("overwrite").save()

    with get_connection() as connection, connection.cursor() as cursor:
        cursor.execute(
            """
            INSERT INTO silver_stock_prices (
                symbol, price_date, open_price, high_price, low_price,
                close_price, volume
            )
            SELECT symbol, price_date, open_price, high_price, low_price,
                   close_price, volume
            FROM silver_stock_prices_staging
            ON CONFLICT (symbol, price_date) DO UPDATE SET
                open_price = EXCLUDED.open_price,
                high_price = EXCLUDED.high_price,
                low_price = EXCLUDED.low_price,
                close_price = EXCLUDED.close_price,
                volume = EXCLUDED.volume;
            """
        )
        cursor.execute(f"DROP TABLE {staging_table};")
