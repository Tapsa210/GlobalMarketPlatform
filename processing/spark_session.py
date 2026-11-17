
import os
from pathlib import Path

from pyspark.sql import SparkSession


def create_spark_session():
    if os.name == "nt":
        hadoop_home = os.environ.get("HADOOP_HOME")
        winutils = (
            Path(hadoop_home) / "bin" / "winutils.exe"
            if hadoop_home
            else None
        )
        if winutils is None or not winutils.is_file():
            raise RuntimeError(
                "Local Spark on Windows requires HADOOP_HOME\\bin\\winutils.exe. "
                "Run Spark in Docker or install winutils and configure HADOOP_HOME."
            )

    return (
        SparkSession.builder.appName("GlobalMarketPlatform")
        .master("local[*]")
        .config("spark.jars.packages", "org.postgresql:postgresql:42.7.5")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.adaptive.enabled", "true")
        .getOrCreate()
    )


if __name__ == "__main__":
    spark = create_spark_session()
    print("Spark Session Created Successfully!")
    print("Spark Version:", spark.version)