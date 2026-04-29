# Professional Data Lakehouse Pipeline

> **v2.0.0** — A professional-grade data engineering pipeline demonstrating modern medallion architecture.

This project implements a robust, idempotent data lakehouse using **Apache Airflow**, **PySpark**, **Apache Iceberg**, and **Project Nessie**. It ingests data from the [OpenBreweryDB API](https://www.openbrewerydb.org/), processes it through Bronze, Silver, and Gold layers, and manages it with a Git-like data catalog.

## 🏗️ Architecture

```text
┌──────────────────────────────────────────────────────────────────────┐
│                        DOCKER COMPOSE NETWORK                        │
│                                                                      │
│  ┌─────────────┐    ┌──────────────┐    ┌────────────────────────┐   │
│  │  Airflow     │    │  Spark       │    │  MinIO (S3)            │   │
│  │  Scheduler   │───▶│  Master      │───▶│  bronze/ (JSON)        │   │
│  │  + Webserver │    │  + Worker    │    │  silver/ (Iceberg)     │   │
│  │  (TaskFlow)  │    │  (Iceberg    │    │  gold/   (Iceberg)     │   │
│  └──────┬───────┘    │   Runtime)   │    └────────────────────────┘   │
│         │            └──────┬───────┘              ▲                  │
│         │                   │                      │                  │
│  ┌──────┴───────┐    ┌──────┴───────┐    ┌────────┴───────┐         │
│  │  PostgreSQL   │    │  Nessie      │    │  MinIO Setup   │         │
│  │  (Airflow DB) │    │  REST Catalog│    │  (bucket init) │         │
│  └───────────────┘    │  (Iceberg)   │    └────────────────┘         │
│                       └──────────────┘                               │
└──────────────────────────────────────────────────────────────────────┘
```

### Data Flow
1. **Bronze:** API raw ingestion to MinIO (JSON) via Airflow TaskFlow.
2. **Silver:** Transformation to Iceberg tables, partitioned by `state`, cleaned and validated.
3. **Gold:** Business-level aggregations (breweries per city/type) stored as Iceberg tables.

## 🛠️ Tech Stack

- **Orchestration:** [Apache Airflow 2.11.0](https://airflow.apache.org/) (TaskFlow API)
- **Processing:** [Apache Spark 3.5.2](https://spark.apache.org/) (PySpark)
- **Table Format:** [Apache Iceberg 1.5.2](https://iceberg.apache.org/)
- **Data Catalog:** [Project Nessie 0.79.0](https://projectnessie.org/) (REST API)
- **Storage:** [MinIO](https://min.io/) (S3-compatible)
- **Configuration:** [Pydantic Settings v2](https://docs.pydantic.dev/latest/usage/pydantic_settings/)
- **Quality & Linting:** [Ruff](https://astral.sh/ruff), [Pytest](https://pytest.org/), [Chispa](https://github.com/MrPowers/chispa)

## 🚀 Quickstart

### 1. Prerequisites
- Docker & Docker Compose
- Python 3.12+ (for local development)
- `make` (optional)

### 2. Setup
```bash
# Clone and enter
git clone <repo-url>
cd data_lake

# Environment config
cp .env.example .env
cp airflow.env.example airflow.env

# Generate security keys and add them to the .env files
# 1. Generate a Fernet key for Airflow:
make fernet-key
# 2. Generate a Secret Key for Airflow Webserver:
python -c "import secrets; print(secrets.token_urlsafe(32))"
```

> [!IMPORTANT]
> Never commit `.env` or `airflow.env` files to version control. They are ignored by default in this project.

### 3. Launch Infrastructure
```bash
# Start all services in the background
docker-compose up --build -d
```

### 4. Access UIs
- **Airflow:** [http://localhost:8080](http://localhost:8080) (Credentials in `.env`)
- **MinIO:** [http://localhost:9001](http://localhost:9001) (Credentials in `.env`)
- **Nessie:** [http://localhost:19120](http://localhost:19120)
- **Spark:** [http://localhost:9090](http://localhost:9090)

## 🏗️ Running the Pipeline

1. **Activate the DAG:** Open the Airflow UI, find `breweries_pipeline`, and toggle the switch to "On".
2. **Trigger Manually:** Click the "Play" button to start a run immediately.
3. **Monitor:**
   - Watch the task progress in the Airflow Graph View.
   - Check **MinIO** (`bronze` bucket) for raw JSON files.
   - Check **Nessie UI** to see the Iceberg table snapshots and commits for `silver` and `gold` layers.

## 💎 Engineering Principles

- **Idempotency:** Silver layer uses `overwritePartitions()` to ensure re-runs don't duplicate data.
- **Type Safety:** Centralized configuration using Pydantic validates environment variables on startup.
- **Data Quality:** Inline Spark validation gates prevent corrupted data from reaching the Gold layer.
- **Schema Evolution:** Iceberg handles schema changes and partitioning without expensive table rewrites.
- **Unicode Resilience:** Native Python UDFs handle complex character encoding from upstream APIs.

## 🧪 Development & Testing

```bash
# Install with dev dependencies
pip install -e ".[dev,airflow]"

# Run comprehensive test suite (>85% coverage)
pytest tests/

# Lint and Format
ruff check .
ruff format .
```

## 🧠 Technical Lessons & Troubleshooting

During the stabilization of this project (v2.0.0), several critical engineering hurdles were overcome:

### 1. Python Version Alignment
The Spark Worker must run the **exact same Python version** as the Airflow Driver. We build Python 3.12.4 from source in the Spark image to prevent `PYTHON_VERSION_MISMATCH` errors.

### 2. Iceberg FileIO Strategy
We use `HadoopFileIO` for S3 connectivity. While `S3FileIO` is native, `HadoopFileIO` is more robust in local containerized environments that rely on `hadoop-aws` JARs for S3A filesystem support.

### 3. Nessie Catalog Integration
For Spark 3.5+, we utilize the Nessie REST endpoint (`type=rest`). This avoids complex JAR dependencies and follows the vendor-neutral Iceberg REST specification.

### 4. Memory Management
The Spark Master and Workers are tuned for local execution with `2g` limits. If encountering OOMs, adjust `SPARK_EXECUTOR_MEMORY` in `.env`.

## 🔒 Security

- **Secrets Management:** This project uses a dual `.env` and `airflow.env` strategy.
- **Environment Isolation:** Hardcoded defaults have been removed from the codebase to ensure all credentials must be explicitly provided via environment variables.
- **Audit:** Automated security scans are performed to prevent secret leaks and ensure Pydantic validation of all critical settings.

## 📄 License
This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.