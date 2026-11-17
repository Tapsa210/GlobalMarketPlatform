
from statistics import mean, median
from time import perf_counter

from pyspark.sql.functions import col, concat, lit, pmod, when
from pyspark.sql.functions import count as spark_count
from pyspark.sql.functions import sum as spark_sum

from processing.spark_session import create_spark_session

# =====================================
# Benchmark Function
# =====================================

def benchmark_comparison(normal_df, salted_df, repeats=10):

    print("\n========== BENCHMARK WARM-UP ==========")

    # Warm-up both methods
    normal_df.collect()
    salted_df.collect()

    normal_times = []
    salted_times = []

    print("\n========== RUNNING BENCHMARK ==========")

    for i in range(repeats):

        print(f"\nRound {i + 1}/{repeats}")

        # Alternate execution order
        if i % 2 == 0:
            methods = [
                ("normal", normal_df),
                ("salted", salted_df)
            ]
        else:
            methods = [
                ("salted", salted_df),
                ("normal", normal_df)
            ]

        for name, dataframe in methods:

            start = perf_counter()

            dataframe.collect()

            elapsed = perf_counter() - start

            if name == "normal":
                normal_times.append(elapsed)
            else:
                salted_times.append(elapsed)

            print(f"{name.upper()}: {elapsed:.4f} seconds")

    # =====================================
    # Calculate Statistics
    # =====================================

    normal_average = mean(normal_times)
    salted_average = mean(salted_times)

    normal_median = median(normal_times)
    salted_median = median(salted_times)

    print("\n========== BENCHMARK RESULTS ==========")

    print("\nNORMAL AGGREGATION")
    print(f"Average: {normal_average:.4f} seconds")
    print(f"Median:  {normal_median:.4f} seconds")

    print("\nSALTED AGGREGATION")
    print(f"Average: {salted_average:.4f} seconds")
    print(f"Median:  {salted_median:.4f} seconds")

    # =====================================
    # Performance Comparison
    # =====================================

    improvement = (
        (normal_average - salted_average)
        / normal_average
    ) * 100

    print("\n========== PERFORMANCE COMPARISON ==========")

    print(f"Normal Average: {normal_average:.4f} seconds")
    print(f"Salted Average: {salted_average:.4f} seconds")

    print(f"Average Improvement: {improvement:.2f}%")

    median_improvement = (
        (normal_median - salted_median)
        / normal_median
    ) * 100

    print(f"Median Improvement: {median_improvement:.2f}%")

    if improvement > 0:
        print("\nSalting was faster in this experiment.")
    elif improvement < 0:
        print("\nNormal aggregation was faster in this experiment.")
    else:
        print("\nBoth methods had the same average execution time.")


# =====================================
# Main Function
# =====================================

def main():

    spark = create_spark_session()

    spark.sparkContext.setLogLevel("ERROR")

    # =====================================
    # Step 1: Generate 1 Million Records
    # =====================================

    print("\n========== GENERATING DATA ==========")

    df = spark.range(1_000_000)

    # =====================================
    # Step 2: Create Skewed Stock Data
    # =====================================

    df = df.withColumn(
        "symbol",
        when(col("id") < 950_000, "AAPL")
        .when(col("id") < 975_000, "MSFT")
        .otherwise("GOOGL")
    )

    # =====================================
    # Step 3: Generate Sample Stock Prices
    # =====================================

    df = df.withColumn(
        "price",
        (col("id") % 100 + 100).cast("double")
    )

    # Cache source data
    df.cache()

    # Materialize cache
    df.count()

    print("\n========== DATASET PREVIEW ==========")

    df.show(10)

    print("\n========== STOCK DISTRIBUTION ==========")

    df.groupBy("symbol").count().show()

    print("Total Records:", df.count())

    # =====================================
    # Step 4: Normal Aggregation
    # =====================================

    print("\n========== NORMAL AGGREGATION ==========")

    normal_summary = (
        df.groupBy("symbol")
        .agg(
            spark_sum("price").alias("total_sum"),
            spark_count("*").alias("total_count")
        )
        .withColumn(
            "average_price",
            col("total_sum") / col("total_count")
        )
    )

    normal_summary.orderBy("symbol").show()

    # =====================================
    # Step 5: Apply Salting
    # =====================================

    salted_df = (
        df
        .withColumn(
            "salt",
            pmod(col("id"), lit(5))
        )
        .withColumn(
            "salted_symbol",
            concat(
                col("symbol"),
                lit("_"),
                col("salt")
            )
        )
    )

    print("\n========== SALTED DISTRIBUTION ==========")

    salted_df.groupBy("salted_symbol").count().orderBy(
        "salted_symbol"
    ).show(20)

    # =====================================
    # Step 6: Partial Aggregation
    # =====================================

    partial_summary = (
        salted_df
        .groupBy("symbol", "salt")
        .agg(
            spark_sum("price").alias("partial_sum"),
            spark_count("*").alias("partial_count")
        )
    )

    print("\n========== PARTIAL AGGREGATION ==========")

    partial_summary.orderBy("symbol", "salt").show(20)

    # =====================================
    # Step 7: Final Aggregation
    # =====================================

    final_summary = (
        partial_summary
        .groupBy("symbol")
        .agg(
            spark_sum("partial_sum").alias("total_sum"),
            spark_sum("partial_count").alias("total_count")
        )
        .withColumn(
            "average_price",
            col("total_sum") / col("total_count")
        )
    )

    print("\n========== FINAL AGGREGATION ==========")

    final_summary.orderBy("symbol").show()

    # =====================================
    # Step 8: Validate Results
    # =====================================

    print("\n========== VALIDATING RESULTS ==========")

    normal_results = normal_summary.collect()
    salted_results = final_summary.collect()

    normal_dict = {
        row["symbol"]: row for row in normal_results
    }

    salted_dict = {
        row["symbol"]: row for row in salted_results
    }

    assert normal_dict.keys() == salted_dict.keys()

    for symbol, normal in normal_dict.items():
        salted = salted_dict[symbol]

        assert normal["total_count"] == salted["total_count"]

        assert abs(
            normal["total_sum"] - salted["total_sum"]
        ) < 0.000001

        assert abs(
            normal["average_price"] - salted["average_price"]
        ) < 0.000001

    print("RESULT VALIDATION: PASSED")

    # =====================================
    # Step 9: Execution Plans
    # =====================================

    print("\n========== NORMAL EXECUTION PLAN ==========")

    normal_summary.explain("formatted")

    print("\n========== SALTED EXECUTION PLAN ==========")

    final_summary.explain("formatted")

    # =====================================
    # Step 10: Performance Benchmark
    # =====================================

    benchmark_comparison(
        normal_summary,
        final_summary,
        repeats=10
    )

    # =====================================
    # Step 11: Cleanup
    # =====================================

    print("\n========== CLEANING UP ==========")

    df.unpersist()

    spark.stop()

    print("Spark session stopped successfully.")


if __name__ == "__main__":
    main()
