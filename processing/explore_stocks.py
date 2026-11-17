
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

    # Define the schema
    columns = ["symbol", "price", "volume"]

    # Create Spark DataFrame
    df = spark.createDataFrame(data, columns)

    # Explore the DataFrame
    print("Stock Market Data:")
    df.show()

    print("DataFrame Schema:")
    df.printSchema()

    print(f"Total Records: {df.count()}")

    # Check current partitions
    print(f"Initial Partitions: {df.rdd.getNumPartitions()}")

    # Redistribute data into 3 partitions
    df_repartitioned = df.repartition(3)

    print(f"New Partitions: {df_repartitioned.rdd.getNumPartitions()}")

    # Trigger execution
    df_repartitioned.count()

    spark.stop()


if __name__ == "__main__":
    main()
