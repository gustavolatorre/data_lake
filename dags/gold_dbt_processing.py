"""Airflow 3.2.1 DAG — Gold dbt Processing.

Triggers when the iceberg_silver_breweries asset is updated.
Runs ``dbt build`` against Dremio — a single command that executes seeds,
models, snapshots, and tests in topological order, with intermediate tests
acting as blocking quality gates (e.g. a failing test on stg_silver_breweries
prevents the downstream dimension/fact models from running).
Emits the iceberg_gold_breweries asset only if the entire build succeeds.
"""

import logging
from datetime import timedelta

import pendulum
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import Asset, dag
from callbacks import build_failure_callback

logger = logging.getLogger("airflow.task")

local_tz = pendulum.timezone("America/Sao_Paulo")

# Definindo os Assets
iceberg_silver_breweries = Asset("iceberg://nessie/silver/breweries")
iceberg_gold_breweries = Asset("iceberg://nessie/gold/breweries")

on_failure_callback = build_failure_callback("GOLD DBT PROCESSING")

DBT_PROJECT_DIR = "/opt/airflow/dbt_project"


@dag(
    dag_id="gold_dbt_processing",
    description="Analytics layer: Iceberg Silver → dbt build (Gold) on Dremio",
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
    """Execute a single ``dbt build`` to materialize Gold and run quality gates."""

    BashOperator(
        task_id="dbt_build",
        # `dbt build` runs in topological DAG order: seeds → snapshots → models
        # → tests (per-resource). Tests are blocking — a downstream model is
        # skipped when an upstream test fails. This replaces the previous
        # `dbt run` >> `dbt test` two-task pipeline, which only caught
        # failures after every model was already materialized.
        bash_command=(
            f"dbt build --profiles-dir {DBT_PROJECT_DIR} --project-dir {DBT_PROJECT_DIR}"
        ),
        outlets=[iceberg_gold_breweries],  # Asset emitido apenas se build inteiro passar
        execution_timeout=timedelta(minutes=20),
        retries=2,
        retry_delay=timedelta(minutes=3),
    )


# Instanciar a pipeline
gold_dbt_pipeline()
