"""Airflow 3.2.1 DAG — Bronze & Silver Processing.

Triggers when the staging_breweries_raw asset is updated.
Runs Spark jobs to ingest to Bronze and transform to Silver Iceberg tables.
Emits the iceberg_silver_breweries asset.
"""

import logging
from datetime import timedelta

import pendulum
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.sdk import Asset, dag

logger = logging.getLogger("airflow.task")

local_tz = pendulum.timezone("America/Sao_Paulo")

# Definindo os Assets
staging_breweries_raw = Asset("s3://staging/breweries")
iceberg_silver_breweries = Asset("iceberg://nessie/silver/breweries")

# Configurações otimizadas do Spark (Configurações de catálogo e MinIO centralizadas em src/utils/spark_session.py)
SPARK_CONF = {
    "spark.driver.memory": "2g",
    "spark.executor.memory": "2g",
    "spark.executor.instances": "1",
}


def on_failure_callback(context: dict) -> None:
    """Log detailed failure information when a task fails."""
    task_instance = context.get("task_instance")
    dag_id = context.get("dag").dag_id if context.get("dag") else "unknown"
    task_id = task_instance.task_id if task_instance else "unknown"
    execution_date = context.get("ds", "unknown")
    exception = context.get("exception", "No exception info")

    logger.error(
        "BRONZE/SILVER PROCESSING FAILURE | dag=%s | task=%s | date=%s | error=%s",
        dag_id,
        task_id,
        execution_date,
        exception,
    )


@dag(
    dag_id="bronze_silver_processing",
    description="Processing layer: Staging → Iceberg Bronze → Iceberg Silver",
    schedule=staging_breweries_raw,  # Agendamento reativo por Asset
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
    tags=["brewery", "bronze", "silver", "iceberg"],
)
def bronze_silver_pipeline():
    """Ingest to Bronze and MERGE into Silver Iceberg tables on staging update."""

    staging_to_bronze = SparkSubmitOperator(
        task_id="staging_to_bronze",
        conn_id="spark_docker",
        application="/opt/airflow/src/bronze/ingest_breweries.py",
        application_args=["{{ dag_run.logical_date.strftime('%Y-%m-%d') if (dag_run is defined and dag_run.logical_date is not none) else macros.datetime.now().strftime('%Y-%m-%d') }}"],
        conf=SPARK_CONF,
        execution_timeout=timedelta(minutes=20),
        retries=2,
        retry_delay=timedelta(minutes=3),
    )

    # Definimos o outlet do asset iceberg_silver_breweries nesta tarefa final do fluxo
    bronze_to_silver = SparkSubmitOperator(
        task_id="bronze_to_silver",
        conn_id="spark_docker",
        application="/opt/airflow/src/silver/transform_breweries.py",
        application_args=["{{ dag_run.logical_date.strftime('%Y-%m-%d') if (dag_run is defined and dag_run.logical_date is not none) else macros.datetime.now().strftime('%Y-%m-%d') }}"],
        conf=SPARK_CONF,
        outlets=[iceberg_silver_breweries],
        execution_timeout=timedelta(minutes=20),
        retries=2,
        retry_delay=timedelta(minutes=3),
    )

    # Fluxo da pipeline
    staging_to_bronze >> bronze_to_silver


# Instanciar a pipeline
bronze_silver_pipeline()
