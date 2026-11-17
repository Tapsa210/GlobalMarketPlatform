
from pyspark.sql.functions import col, when
from pyspark.sql.functions import count as spark_count
from pyspark.sql.functions import sum as spark_sum

from processing.spark_session import create_spark_session


def main():

    spark = create_spark_session()

    spark.sparkContext.setLogLevel("WARN")

    # Configure shuffle partitions
    spark.conf.set("spark.sql.shuffle.partitions", "8")

    print("\n========== SPARK UI ==========")
    print("Spark UI URL:", spark.sparkContext.uiWebUrl)

    # Step 1: Generate data
    print("\n========== GENERATING DATA ==========")

    df = spark.range(1_000_000)

    # Step 2: Create categories
    df = df.withColumn(
        "category",
        when(col("id") < 700_000, "Technology")
        .when(col("id") < 900_000, "Finance")
        .otherwise("Healthcare")
    )

    df = df.withColumn(
        "amount",
        (col("id") % 100 + 100).cast("double")
    )

    # Step 3: Repartition
    print("\n========== REPARTITION ==========")

    partitioned_df = df.repartition(8)

    print("Number of Partitions:",
          partitioned_df.rdd.getNumPartitions())

    # Step 4: Aggregation
    print("\n========== RUNNING AGGREGATION ==========")

    result = (
        partitioned_df
        .groupBy("category")
        .agg(
            spark_sum("amount").alias("total_amount"),
            spark_count("*").alias("total_records")
        )
        .orderBy("category")
    )

    # Action: Trigger Spark execution
    result.show()

    print("\n========== SPARK UI IS READY ==========")

    print("Open: http://localhost:4040")

    input("\nPress ENTER to stop Spark...")

    spark.stop()

    print("Spark stopped successfully.")


if __name__ == "__main__":
    main()
