# Global Market Data Engineering Platform

An end-to-end data engineering project that builds a market data pipeline using Python, Apache Kafka, PostgreSQL, PySpark, Apache Airflow, and Docker.

The platform ingests stock market data from the Alpha Vantage API, streams records through Kafka, stores raw data in PostgreSQL, and transforms it into analytical datasets using a Bronze–Silver–Gold architecture.

## Architecture

```text
                 Alpha Vantage API
                         |
                         v
                  Python Producer
                         |
                         v
                       Kafka
                         |
                         v
                  Python Consumer
                         |
                         v
                PostgreSQL Bronze
                         |
                         v
                      PySpark
                         |
                  Data Processing
                         |
                  +------+------+
                  |             |
                  v             v
                Silver         Gold
                  |             |
                  +------+------+
                         |
                         v
                    PostgreSQL

                  Apache Airflow
                Workflow Orchestration
```

Apache Airflow orchestrates the ingestion and processing workflow, while Docker Compose manages the local infrastructure.

## Technology Stack

| Technology | Purpose |
|---|---|
| Python | Data ingestion and application logic |
| Alpha Vantage API | Stock market data source |
| Apache Kafka | Data streaming and messaging |
| PostgreSQL | Relational database and data warehouse |
| PySpark | Data transformation and processing |
| Apache Airflow | Workflow orchestration |
| Docker | Application containerization |
| Docker Compose | Infrastructure management |
| Pytest | Automated testing |
| Ruff | Code quality and linting |

## Key Features

- API-based stock market data ingestion.
- Kafka-based producer and consumer architecture.
- PostgreSQL Bronze, Silver, and Gold data layers.
- Distributed data processing with PySpark.
- Data validation, cleaning, and deduplication.
- Idempotent database writes using PostgreSQL upserts.
- Automated workflow orchestration with Apache Airflow.
- Containerized services using Docker Compose.
- Automated testing and code quality checks.

## Data Pipeline

### 1. Data Ingestion

Stock market data is retrieved from the Alpha Vantage API using Python.

The producer publishes market data messages to Kafka, where the consumer processes incoming records and stores them in PostgreSQL.

### 2. Bronze Layer

The Bronze layer stores raw API responses and normalized stock price records.

This layer provides the initial data foundation for downstream processing.

### 3. Silver Layer

PySpark transforms Bronze data into validated and cleaned records.

Processing includes:

- Handling missing and invalid values.
- Validating OHLCV fields.
- Normalizing data types.
- Removing duplicate records.
- Preparing structured market data for analytics.

### 4. Gold Layer

The Gold layer contains aggregated market data for analytical use cases.

Calculated metrics include:

- Daily average price.
- Daily highest price.
- Daily lowest price.
- Daily price change.
- Daily percentage change.
- Total trading volume.

### Idempotency

Silver and Gold database writes use PostgreSQL upserts based on the composite key:

```text
(symbol, price_date)
```

This allows processing jobs to be rerun without creating duplicate records for the same stock and trading date.

## Airflow Orchestration

The project includes an Apache Airflow DAG named `market_data_pipeline`.

### Workflow

```text
Check PostgreSQL
       |
       v
    Ingestion
       |
       v
  Check Bronze
       |
       v
 Spark Processing
       |
       v
   Quality Gate
       |
       v
     Report
```

### DAG Configuration

| Setting | Value |
|---|---|
| Schedule | Weekdays at 18:00 |
| Timezone | Asia/Manila |
| Retries | 2 attempts |
| Retry Delay | 5 minutes |
| Task Timeout | 30 minutes |
| DAG Timeout | 1 hour |

Airflow uses DockerOperator to execute ingestion and Spark processing through the existing application image.

The Docker socket configuration is intended for local development and should be replaced with a more secure execution architecture for production deployments.

## Getting Started

### Prerequisites

- Python 3.11 or compatible version.
- Docker Desktop.
- Docker Compose.
- Java runtime compatible with PySpark.
- Alpha Vantage API key.

### 1. Clone the repository

```bash
git clone https://github.com/Tapsa210/GlobalMarketPlatform.git
cd GlobalMarketPlatform
```

### 2. Configure environment variables

Copy the example environment file:

**Windows PowerShell:**

```powershell
Copy-Item .env.example .env
```

**Linux / macOS:**

```bash
cp .env.example .env
```

Update `.env` with your own API key and database credentials.

Never commit `.env` or expose API keys and passwords.

### 3. Start infrastructure

```bash
docker compose up -d postgres kafka consumer
```

### 4. Run ingestion

```bash
docker compose --profile ingestion run --rm ingestion
```

### 5. Run Silver and Gold processing

```bash
docker compose run --rm ingestion python -m processing.run_pipeline
```

### 6. Check services

```bash
docker compose ps
```

## Airflow Setup

Start the Airflow initialization service:

```bash
docker compose up -d airflow-init
```

Start the webserver and scheduler:

```bash
docker compose up -d airflow-webserver airflow-scheduler
```

Open the Airflow interface:

```text
http://localhost:8080
```

Enable `market_data_pipeline` and trigger a manual DAG run.

## Database Access

| Service | Address |
|---|---|
| PostgreSQL | `localhost:5433` |
| Kafka | `localhost:9092` |
| Airflow UI | `http://localhost:8080` |

Database initialization scripts create the Bronze, Silver, and Gold tables during the first PostgreSQL startup.

## Testing and Code Quality

Run automated tests:

```powershell
.\.venv\Scripts\python.exe -m pytest -q
```

Run Ruff:

```powershell
.\.venv\Scripts\python.exe -m ruff check .
```

### Verified Results

- 42 automated tests passed.
- Ruff checks passed.
- Python syntax validation passed.
- Docker Compose configuration validation passed.
- Airflow DAG executed successfully.
- DockerOperator successfully executed Spark processing.
- Bronze: 101 records.
- Silver: 101 records.
- Gold: 101 records.

These results reflect the verified local test run.

## Security Considerations

- Never commit `.env` files, API keys, or passwords.
- `.env.example` should contain placeholders only.
- Kafka uses plaintext communication for local development.
- Production environments require TLS, authentication, and Kafka ACLs.
- Docker socket access should be restricted and secured.
- Production deployments should use appropriate secrets management.

## Future Improvements

Planned enhancements include:

- Cloud deployment.
- Pipeline monitoring and metrics.
- Advanced data quality monitoring.
- Centralized logging and observability.
- Cloud storage integration.
- Production-grade security and infrastructure.

## Project Status

The local end-to-end data pipeline and Airflow orchestration have been implemented and successfully tested.

Cloud deployment, monitoring, and production hardening are planned future enhancements.

## Author

**ABDULRAHMAN YASSIN ADAM**

GitHub: [@Tapsa210](https://github.com/Tapsa210)

---

*Built as a practical Data Engineering portfolio project.*
