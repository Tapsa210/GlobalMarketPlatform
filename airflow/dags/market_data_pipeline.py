import logging
import os
import sys
from datetime import timedelta

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.utils.dates import days_ago

from airflow import DAG

sys.path.insert(0, "/opt/airflow/project")

from database.connection import get_connection

logger = logging.getLogger(__name__)


def check_database() -> None:
    with get_connection() as connection, connection.cursor() as cursor:
        cursor.execute("SELECT 1;")
        if cursor.fetchone()[0] != 1:
            raise RuntimeError("PostgreSQL health check returned an unexpected value.")


def check_bronze_data() -> None:
    with get_connection() as connection, connection.cursor() as cursor:
        cursor.execute("SELECT COUNT(*) FROM bronze_stock_prices;")
        count = cursor.fetchone()[0]
    logger.info("Bronze layer contains %s records.", count)
    if count == 0:
        raise RuntimeError("Ingestion completed but Bronze contains no records.")


def validate_processed_data() -> None:
    with get_connection() as connection, connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT COUNT(*)
            FROM silver_stock_prices
            WHERE symbol IS NULL
               OR price_date IS NULL
               OR open_price <= 0
               OR high_price < low_price
               OR volume < 0;
            """
        )
        invalid_count = cursor.fetchone()[0]
        cursor.execute(
            """
            SELECT COUNT(*)
            FROM silver_stock_prices;
            """
        )
        silver_count = cursor.fetchone()[0]
        cursor.execute(
            """
            SELECT COUNT(*)
            FROM gold_daily_stock_metrics;
            """
        )
        gold_count = cursor.fetchone()[0]

    logger.info(
        "Quality gate results: invalid=%s silver=%s gold=%s",
        invalid_count,
        silver_count,
        gold_count,
    )
    if invalid_count or silver_count != gold_count:
        raise RuntimeError("Silver/Gold data quality gate failed.")


default_args = {
    "owner": "data-platform",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=30),
}


with DAG(
    dag_id="market_data_pipeline",
    description="Ingest market data and build Bronze, Silver, and Gold layers.",
    default_args=default_args,
    start_date=days_ago(1),
    schedule="0 18 * * 1-5",
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(hours=1),
    tags=["market-data", "spark", "postgres"],
) as dag:
    check_services = PythonOperator(
        task_id="check_database",
        python_callable=check_database,
    )

    ingest_data = DockerOperator(
        task_id="ingest_market_data",
        image="globalmarketplatform-ingestion:latest",
        command="python -m ingestion.pipeline",
        docker_url="unix://var/run/docker.sock",
        network_mode="globalmarketplatform_default",
        auto_remove="success",
        mount_tmp_dir=False,
        environment={
            "ALPHA_VANTAGE_API_KEY": os.environ.get("ALPHA_VANTAGE_API_KEY", ""),
            "POSTGRES_HOST": "postgres",
            "POSTGRES_PORT": "5432",
            "POSTGRES_DB": "market_data",
            "POSTGRES_USER": "market_user",
            "POSTGRES_PASSWORD": os.environ.get("POSTGRES_PASSWORD", ""),
            "KAFKA_BOOTSTRAP_SERVERS": "kafka:29092",
            "STOCK_SYMBOLS": os.environ.get("STOCK_SYMBOLS", "AAPL"),
        },
    )

    check_bronze = PythonOperator(
        task_id="check_bronze_data",
        python_callable=check_bronze_data,
    )

    process_spark_layers = DockerOperator(
        task_id="process_silver_and_gold",
        image="globalmarketplatform-ingestion:latest",
        command="python -m processing.run_pipeline",
        docker_url="unix://var/run/docker.sock",
        network_mode="globalmarketplatform_default",
        auto_remove="success",
        mount_tmp_dir=False,
        environment={
            "ALPHA_VANTAGE_API_KEY": os.environ.get("ALPHA_VANTAGE_API_KEY", ""),
            "POSTGRES_HOST": "postgres",
            "POSTGRES_PORT": "5432",
            "POSTGRES_DB": "market_data",
            "POSTGRES_USER": "market_user",
            "POSTGRES_PASSWORD": os.environ.get("POSTGRES_PASSWORD", ""),
            "KAFKA_BOOTSTRAP_SERVERS": "kafka:29092",
        },
    )

    quality_gate = PythonOperator(
        task_id="validate_processed_data",
        python_callable=validate_processed_data,
    )

    report_completion = BashOperator(
        task_id="report_completion",
        bash_command="echo 'Market data pipeline completed successfully.'",
    )

    (
        check_services
        >> ingest_data
        >> check_bronze
        >> process_spark_layers
        >> quality_gate
        >> report_completion
    )
