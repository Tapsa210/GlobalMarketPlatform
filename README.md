# Global Market Data Engineering Platform

End-to-end market data pipeline using Alpha Vantage, Kafka, PostgreSQL,
PySpark, and Docker Compose.

## Architecture

```text
Alpha Vantage -> Python Producer -> Kafka -> Python Consumer
                                      |
                                      v
                               PostgreSQL Bronze
                                      |
                                      v
                           PySpark Silver -> Gold
```

Airflow orchestration is included; metrics and cloud deployment remain planned
next.

## Quick start

1. Copy `.env.example` to `.env` and set your own API key and password.
2. Start the infrastructure:

   ```powershell
   docker compose up -d postgres kafka consumer
   ```

3. Run ingestion:

   ```powershell
   docker compose --profile ingestion run --rm ingestion
   ```

4. Run Silver and Gold processing:

   ```powershell
   docker compose run --rm ingestion python -m processing.run_pipeline
   ```

5. Check services:

   ```powershell
   docker compose ps
   ```

PostgreSQL is exposed on `localhost:5433`; Kafka is exposed on
`localhost:9092`. The database initialization scripts create Bronze,
Silver, and Gold tables on the first PostgreSQL startup.

## Airflow orchestration

The `market_data_pipeline` DAG runs on weekdays at 18:00 in the configured
Airflow timezone. It checks PostgreSQL, runs ingestion, checks Bronze, runs
Spark Silver/Gold processing, validates the output, and reports completion.
Retries are set to two attempts with a five-minute delay; each task has a
30-minute timeout and each DAG run has a one-hour timeout.

Start Airflow after PostgreSQL and Kafka are running:

```powershell
docker compose up -d airflow-init
docker compose up -d airflow-webserver airflow-scheduler
```

Open `http://localhost:8080`, enable `market_data_pipeline`, and trigger it
manually from the Airflow UI. Airflow uses DockerOperator to run the existing
application image for ingestion and Spark, so Spark dependencies are not
duplicated in the Airflow image. The Docker socket mount is for local
development; use a remote worker or Kubernetes executor in production.

## Local development

```powershell
.\.venv\Scripts\python.exe -m pytest -q
.\.venv\Scripts\python.exe -m ruff check .
```

The Spark JDBC driver is downloaded by Spark using the configured package
coordinate. A Java runtime compatible with PySpark is required for local
Spark execution.

## Data layers

- **Bronze:** raw API responses and normalized stock prices.
- **Silver:** validated, typed, deduplicated OHLCV records.
- **Gold:** daily average, high, low, change, percentage change, and volume.

Silver and Gold writes use PostgreSQL upserts on `(symbol, price_date)` so
re-running the processing job does not create duplicates.

## Security

- Never commit `.env`, API keys, or passwords.
- `.env.example` contains placeholders only.
- Kafka uses plaintext for local development; production requires TLS,
  authentication, and ACLs.
