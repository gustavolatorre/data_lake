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
- **Docker & Docker Compose** (Desktop or Engine)
- **Python 3.12+** (required for local development/testing)
- **Make** (optional, but recommended for automation)

### 2. Setup Environment
You must configure two environment files to handle infrastructure and Airflow settings respectively.

```bash
# Clone the repository
git clone <repo-url>
cd data_lake

# 1. Copy example files
cp .env.example .env
cp airflow.env.example airflow.env

# 2. Generate required Airflow keys
# Generate a Fernet key:
make fernet-key 
# (Paste the result into AIRFLOW__CORE__FERNET_KEY in airflow.env)

# Generate a Webserver Secret Key:
python -c "import secrets; print(secrets.token_urlsafe(32))"
# (Paste the result into AIRFLOW__WEBSERVER__SECRET_KEY in airflow.env)
```

> [!IMPORTANT]
> **Credential Synchronization:** The Airflow variables in `airflow.env` **must match** the MinIO root credentials defined in your `.env` file. If they differ, the pipeline will fail to authenticate with the storage layer.
> ```env
> # In airflow.env, these must match MINIO_ROOT_USER/PASSWORD from .env
> AIRFLOW_VAR_MINIO_ROOT_USER=admin
> AIRFLOW_VAR_MINIO_ROOT_PASSWORD=password
> ```

### 3. Launch Infrastructure
Use the `Makefile` for a streamlined startup or run docker-compose directly.

```bash
# Option A: Using Makefile (recommended)
make up

# Option B: Using Docker Compose
docker-compose up --build -d
```

Wait for all containers to reach a **Healthy** status.

### 4. Access Monitoring & UIs

| Service | URL | Credentials (Default) |
| :--- | :--- | :--- |
| **Airflow** | [http://localhost:8080](http://localhost:8080) | `airflow` / `airflow` |
| **MinIO Console** | [http://localhost:9001](http://localhost:9001) | `admin` / `password` |
| **Nessie UI** | [http://localhost:19120](http://localhost:19120) | N/A (Public UI) |
| **Spark Master** | [http://localhost:9090](http://localhost:9090) | N/A (Status only) |

## 🏗️ Running the Pipeline

1. **Access Airflow:** Log in at `localhost:8080`.
2. **Activate DAG:** Locate `breweries_pipeline` and toggle it to **On**.
3. **Trigger:** Click the "Play" button to start an immediate execution.
4. **Verify Results:**
   - **MinIO:** Check the `bronze` bucket for partitioned JSON files.
   - **Nessie:** Explore the `main` branch to see Iceberg table commits for `silver.breweries` and `gold.breweries_summary`.
   - **Logs:** Run `make logs-airflow` to monitor task execution in real-time.

## 💎 Engineering Principles

- **Idempotency:** Silver layer uses `overwritePartitions()` to ensure re-runs don't duplicate data.
- **Type Safety:** Centralized configuration using Pydantic validates environment variables on startup.
- **Data Quality:** Inline Spark validation gates prevent corrupted data from reaching the Gold layer.
- **Schema Evolution:** Iceberg handles schema changes and partitioning without expensive table rewrites.
- **Unicode Resilience:** Native Python UDFs handle complex character encoding from upstream APIs.

## 🧪 Development & Testing

```bash
# Install local environment with dev dependencies
pip install -e ".[dev,airflow]"

# Run comprehensive test suite (>85% coverage)
make test

# Lint and Format
make lint
make fmt
```

## 🧠 Troubleshooting & Technical Decisions

During development (v2.0.0), several architectural challenges were addressed:

1. **Python Versioning:** To avoid `PYTHON_VERSION_MISMATCH`, we build Python 3.12.4 from source in the Spark images to match the Airflow environment exactly.
2. **S3 Connectivity:** We use `HadoopFileIO` instead of `S3FileIO` for better compatibility with local MinIO setups using S3A filesystem JARs.
3. **Iceberg Catalog:** The project uses the **Nessie REST API** for catalog management, allowing Git-like versioning (branching/tagging) of the entire data lake.
4. **Memory Management:** If Spark jobs fail due to OOM, increase `SPARK_EXECUTOR_MEMORY` in `.env` (default is `2g`).

## 🔒 Security

- **Secrets Strategy:** Credentials are never hardcoded; they are injected via `.env` (infrastructure) and `airflow.env` (application).
- **Network Isolation:** All services communicate within a dedicated Docker bridge network.
- **Security Audit:** `.env` files are strictly ignored by `.gitignore` to prevent accidental credential leaks.

## 📄 License
This project is licensed under the MIT License.