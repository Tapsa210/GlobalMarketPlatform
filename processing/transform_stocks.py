
from pyspark.sql.functions import avg, col, count, sum

from processing.spark_session import create_spark_session


def main():

    spark = create_spark_session()

    # Sample stock market data
    data = [
        ("AAPL", 225.50, 1000),
        ("AAPL", 228.30, 1500),
        ("MSFT", 420.75, 800),
        ("MSFT", 425.20, 1200),
        ("GOOGL", 175.40, 900),
        ("GOOGL", 178.60, 1100),
    ]

    columns = ["symbol", "price", "volume"]

    df = spark.createDataFrame(data, columns)

    # Step 1: Create trade_value column
    df = df.withColumn(
        "trade_value",
        col("price") * col("volume")
    )

    print("Original Data with Trade Value:")
    df.show()

    # Step 2: Filter stocks above $200
    filtered_df = df.filter(
        col("price") > 200
    )

    print("Filtered Stocks:")
    filtered_df.show()

    # Step 3: GroupBy and Aggregations
    stock_summary = (
        filtered_df
        .groupBy("symbol")
        .agg(
            avg("price").alias("average_price"),
            sum("volume").alias("total_volume"),
            sum("trade_value").alias("total_trade_value"),
            count("*").alias("total_records")
        )
    )

    print("Stock Summary:")
    stock_summary.show()

    spark.stop()


if __name__ == "__main__":
    main()
