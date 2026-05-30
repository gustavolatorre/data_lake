"""Airflow 3.2.1 DAG — Bronze & Silver Processing for Brasileirao with Nessie branching.

Triggers when the staging_brasileirao_raw asset is updated. Lifecycle:

    create_branch  ──▶  staging_to_bronze  ──▶  (silver_placeholder)  ──▶  merge_branch
                                       │                                         │
                                       └──────────── cleanup_branch ◀────────────┘
                                              (runs only on upstream failure)

* The first task creates ``etl_bronze_silver_brasileirao_<date>`` off ``main``.
* Bronze and Silver Spark jobs bind to that branch via ``--nessie-ref``;
  every write stays isolated.
* ``merge_branch`` only emits the asset after a clean Spark fan-out.
* ``cleanup_branch`` runs on upstream failure (``trigger_rule="one_failed"``).

Emits the iceberg_silver_brasileirao asset.
"""

import logging
from datetime import timedelta

import pendulum
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import Asset, dag, task
from airflow.task.trigger_rule import TriggerRule
from callbacks import build_failure_callback

logger = logging.getLogger("airflow.task")

local_tz = pendulum.timezone("America/Sao_Paulo")

# Definindo os Assets
staging_brasileirao_raw = Asset("s3://staging/brasileirao")
iceberg_silver_brasileirao = Asset("iceberg://nessie/silver/brasileirao")

SPARK_CONF = {
    "spark.driver.memory": "2g",
    "spark.executor.memory": "2g",
    "spark.executor.instances": "1",
}

EXECUTION_DATE_TEMPLATE = (
    "{{ dag_run.logical_date.strftime('%Y-%m-%d') "
    "if (dag_run is defined and dag_run.logical_date is not none) "
    "else macros.datetime.now().strftime('%Y-%m-%d') }}"
)

NESSIE_BRANCH_TEMPLATE = (
    "etl_bronze_silver_brasileirao_{{ (dag_run.logical_date.strftime('%Y-%m-%d') "
    "if (dag_run is defined and dag_run.logical_date is not none) "
    "else macros.datetime.now().strftime('%Y-%m-%d'))"
    ".replace('-', '_') }}"
)

on_failure_callback = build_failure_callback("BRONZE/SILVER BRASILEIRAO PROCESSING")


@dag(
    dag_id="bronze_silver_brasileirao_processing",
    description="Processing layer with Nessie branch isolation: Staging → Bronze → Silver → MERGE",
    schedule=staging_brasileirao_raw,
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
    tags=["brasileirao", "bronze", "silver", "iceberg", "nessie-branching"],
)
def bronze_silver_brasileirao_pipeline():
    """Branch-isolated Bronze/Silver pipeline for Brasileirao."""

    @task(task_id="create_branch", retries=1)
    def create_branch(**context) -> str:
        """Create the isolated Nessie branch and return its name."""
        from src.utils.nessie_branch import build_branch_name
        from src.utils.nessie_branch import create_branch as _create

        execution_date = context.get("ds") or pendulum.now(local_tz).strftime("%Y-%m-%d")
        name = build_branch_name(dag_id="bronze_silver_brasileirao", execution_date=execution_date)
        logger.info("Creating isolated Nessie branch '%s'", name)
        _create(name, source_ref="main")
        return name

    branch_name = create_branch()

    staging_to_bronze = SparkSubmitOperator(
        task_id="staging_to_bronze",
        application="/opt/airflow/src/bronze/ingest_brasileirao.py",
        name="brasileirao_bronze_ingestion",
        conn_id="spark_docker",
        conf=SPARK_CONF,
        application_args=[
            EXECUTION_DATE_TEMPLATE,
            "--nessie-ref",
            NESSIE_BRANCH_TEMPLATE,
        ],
        verbose=False,
    )

    # Placeholder for the future Silver processing step
    bronze_to_silver = EmptyOperator(
        task_id="bronze_to_silver_placeholder",
    )

    @task(task_id="merge_branch", outlets=[iceberg_silver_brasileirao], retries=1)
    def merge_branch(branch_ref: str) -> None:
        """Merge the isolated branch back into main upon success."""
        from src.utils.nessie_branch import merge_branch as _merge

        logger.info("Merging branch '%s' into 'main'", branch_ref)
        _merge(branch_ref, target="main")

    merge_task = merge_branch(branch_name)

    @task(task_id="cleanup_branch", trigger_rule=TriggerRule.ONE_FAILED, retries=1)
    def cleanup_branch(branch_ref: str) -> None:
        """Drop the branch if any upstream Spark job fails."""
        from src.utils.nessie_branch import drop_branch

        logger.warning("Upstream failure detected. Cleaning up branch '%s'", branch_ref)
        drop_branch(branch_ref)

    cleanup_task = cleanup_branch(branch_name)

    # Topological execution order
    branch_name >> staging_to_bronze >> bronze_to_silver >> merge_task
    # Cleanup only runs if staging_to_bronze or bronze_to_silver fail
    staging_to_bronze >> cleanup_task
    bronze_to_silver >> cleanup_task


bronze_silver_brasileirao_pipeline()
