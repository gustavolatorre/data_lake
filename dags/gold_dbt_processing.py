"""Airflow 3.2.1 DAG — Gold dbt Processing.

Triggers when the iceberg_silver_breweries asset is updated.
Runs dbt run and dbt test against Dremio.
Emits the iceberg_gold_breweries asset.
"""

import logging
from datetime import timedelta

import pendulum
from airflow.operators.bash import BashOperator
from airflow.sdk import Asset, dag

logger = logging.getLogger("airflow.task")

local_tz = pendulum.timezone("America/Sao_Paulo")

# Definindo os Assets
iceberg_silver_breweries = Asset("iceberg://nessie/silver/breweries")
iceberg_gold_breweries = Asset("iceberg://nessie/gold/breweries")


def on_failure_callback(context: dict) -> None:
    """Log detailed failure information when a task fails."""
    task_instance = context.get("task_instance")
    dag_id = context.get("dag").dag_id if context.get("dag") else "unknown"
    task_id = task_instance.task_id if task_instance else "unknown"
    execution_date = context.get("ds", "unknown")
    exception = context.get("exception", "No exception info")

    logger.error(
        "GOLD DBT PROCESSING FAILURE | dag=%s | task=%s | date=%s | error=%s",
        dag_id,
        task_id,
        execution_date,
        exception,
    )


@dag(
    dag_id="gold_dbt_processing",
    description="Analytics layer: Iceberg Silver → dbt Gold on Dremio",
    schedule=iceberg_silver_breweries,  # Agendamento reativo por Asset
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
    tags=["brewery", "gold", "dbt", "dremio"],
)
def gold_dbt_pipeline():
    """Execute dbt run and test to compile Gold layers in Dremio on Silver update."""

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=("dbt run --profiles-dir /opt/airflow/dbt_project --project-dir /opt/airflow/dbt_project"),
        execution_timeout=timedelta(minutes=15),
        retries=2,
        retry_delay=timedelta(minutes=3),
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=("dbt test --profiles-dir /opt/airflow/dbt_project --project-dir /opt/airflow/dbt_project"),
        outlets=[iceberg_gold_breweries],  # Emite o asset final gold
        execution_timeout=timedelta(minutes=10),
        retries=2,
        retry_delay=timedelta(minutes=3),
    )

    # Fluxo
    dbt_run >> dbt_test


# Instanciar a pipeline
gold_dbt_pipeline()
