"""Airflow 3.2.1 DAG — Staging Ingestion.

Fetches brewery data from the OpenBreweryDB API and uploads raw JSON files
to the MinIO Staging bucket. Emits the staging_breweries_raw asset.
"""

import logging
from datetime import timedelta

import pendulum
from airflow.sdk import Asset, dag, task

logger = logging.getLogger("airflow.task")

local_tz = pendulum.timezone("America/Sao_Paulo")

# Definindo o Asset reativo de saída
staging_breweries_raw = Asset("s3://staging/breweries")


def on_failure_callback(context: dict) -> None:
    """Log detailed failure information when a task fails."""
    task_instance = context.get("task_instance")
    dag_id = context.get("dag").dag_id if context.get("dag") else "unknown"
    task_id = task_instance.task_id if task_instance else "unknown"
    execution_date = context.get("ds", "unknown")
    exception = context.get("exception", "No exception info")

    logger.error(
        "STAGE INGESTION FAILURE | dag=%s | task=%s | date=%s | error=%s",
        dag_id,
        task_id,
        execution_date,
        exception,
    )


@dag(
    dag_id="staging_ingestion",
    description="Ingestion layer: OpenBreweryDB API → MinIO Staging",
    schedule="@daily",
    start_date=pendulum.datetime(2024, 1, 1, tz=local_tz),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "data-engineering",
        "depends_on_past": False,
        "retries": 3,
        "retry_delay": timedelta(minutes=5),
        "execution_timeout": timedelta(minutes=30),
        "on_failure_callback": on_failure_callback,
    },
    tags=["brewery", "staging", "ingestion"],
)
def staging_ingestion_pipeline():
    """Ingest brewery data from API and emit staging_breweries_raw asset."""

    @task(outlets=[staging_breweries_raw])
    def fetch_to_staging(**context) -> int:
        """Fetch brewery data from API and upload to MinIO Staging bucket.

        Returns:
            Number of records fetched.
        """
        execution_date = context.get("ds") or pendulum.now(local_tz).strftime("%Y-%m-%d")
        logger.info("Starting Staging ingestion for date=%s", execution_date)

        # Importações pesadas de dependências do Spark ou locais dentro da função
        # Evita lentidão no carregamento (DAG parsing) do Scheduler do Airflow
        from src.staging.fetch_breweries import fetch_and_upload

        total = fetch_and_upload(execution_date)
        logger.info("Staging ingestion complete: %d records", total)
        return total

    fetch_to_staging()


# Instanciar a pipeline de ingestão
staging_ingestion_pipeline()
