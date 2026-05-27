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
from callbacks import build_failure_callback

logger = logging.getLogger("airflow.task")

local_tz = pendulum.timezone("America/Sao_Paulo")

# Definindo os Assets
iceberg_silver_breweries = Asset("iceberg://nessie/silver/breweries")
iceberg_gold_breweries = Asset("iceberg://nessie/gold/breweries")

on_failure_callback = build_failure_callback("GOLD DBT PROCESSING")


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
