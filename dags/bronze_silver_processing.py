"""Airflow 3.2.1 DAG — Bronze & Silver Processing with Nessie branching (P3.1).

Triggers when the staging_breweries_raw asset is updated. Lifecycle:

    create_branch  ──▶  staging_to_bronze  ──▶  bronze_to_silver  ──▶  merge_branch
                                       │                                     │
                                       └──────────── cleanup_branch ◀────────┘
                                              (runs only on upstream failure)

* The first task creates ``etl_bronze_silver_<date>`` off ``main``.
* Bronze and Silver Spark jobs bind to that branch via ``--nessie-ref``;
  every write stays isolated, so a half-finished Silver MERGE never leaks
  into the Gold-facing ``main``.
* ``merge_branch`` only emits the asset after a clean Spark fan-out — that
  is the point at which Gold gets to see the new data.
* ``cleanup_branch`` runs on upstream failure (``trigger_rule="one_failed"``)
  and drops the branch so we don't accumulate orphans.

Emits the iceberg_silver_breweries asset.
"""

import logging
from datetime import timedelta

import pendulum
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.sdk import Asset, dag, task
from airflow.task.trigger_rule import TriggerRule
from callbacks import build_failure_callback

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

# Jinja template used by SparkSubmit task `application_args`. The same
# `execution_date` value is used to derive both the partition keys and the
# Nessie branch name, so re-runs on the same logical date hit the same branch.
EXECUTION_DATE_TEMPLATE = (
    "{{ dag_run.logical_date.strftime('%Y-%m-%d') "
    "if (dag_run is defined and dag_run.logical_date is not none) "
    "else macros.datetime.now().strftime('%Y-%m-%d') }}"
)

# Templated branch name — matches `nessie_branch.build_branch_name`.
NESSIE_BRANCH_TEMPLATE = (
    "etl_bronze_silver_{{ (dag_run.logical_date.strftime('%Y-%m-%d') "
    "if (dag_run is defined and dag_run.logical_date is not none) "
    "else macros.datetime.now().strftime('%Y-%m-%d'))"
    ".replace('-', '_') }}"
)

on_failure_callback = build_failure_callback("BRONZE/SILVER PROCESSING")


@dag(
    dag_id="bronze_silver_processing",
    description="Processing layer with Nessie branch isolation: Staging → Bronze → Silver → MERGE",
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
    tags=["brewery", "bronze", "silver", "iceberg", "nessie-branching"],
)
def bronze_silver_pipeline():
    """Branch-isolated Bronze/Silver pipeline.

    Each run carves out its own Nessie branch, fans out the Spark jobs on
    that branch, and only fast-forwards ``main`` after both jobs succeeded.
    Failures drop the branch so ``main`` stays clean.
    """

    @task(task_id="create_branch", retries=1)
    def create_branch(**context) -> str:
        """Create the isolated Nessie branch and return its name."""
        from src.utils.nessie_branch import build_branch_name
        from src.utils.nessie_branch import create_branch as _create

        execution_date = context.get("ds") or pendulum.now(local_tz).strftime("%Y-%m-%d")
        name = build_branch_name(dag_id="bronze_silver", execution_date=execution_date)
        logger.info("Creating isolated Nessie branch '%s'", name)
        _create(name, source_ref="main")
        return name

    branch_name = create_branch()

    staging_to_bronze = SparkSubmitOperator(
        task_id="staging_to_bronze",
        conn_id="spark_docker",
        application="/opt/airflow/src/bronze/ingest_breweries.py",
        application_args=[EXECUTION_DATE_TEMPLATE, "--nessie-ref", NESSIE_BRANCH_TEMPLATE],
        conf=SPARK_CONF,
        execution_timeout=timedelta(minutes=20),
        retries=2,
        retry_delay=timedelta(minutes=3),
    )

    bronze_to_silver = SparkSubmitOperator(
        task_id="bronze_to_silver",
        conn_id="spark_docker",
        application="/opt/airflow/src/silver/transform_breweries.py",
        application_args=[EXECUTION_DATE_TEMPLATE, "--nessie-ref", NESSIE_BRANCH_TEMPLATE],
        conf=SPARK_CONF,
        execution_timeout=timedelta(minutes=20),
        retries=2,
        retry_delay=timedelta(minutes=3),
    )

    @task(task_id="merge_branch", outlets=[iceberg_silver_breweries], retries=2)
    def merge_branch(**context) -> None:
        """Fast-forward ``main`` to the run's branch and emit the asset."""
        from src.utils.nessie_branch import build_branch_name
        from src.utils.nessie_branch import merge_branch as _merge

        execution_date = context.get("ds") or pendulum.now(local_tz).strftime("%Y-%m-%d")
        name = build_branch_name(dag_id="bronze_silver", execution_date=execution_date)
        logger.info("Merging Nessie branch '%s' back into main", name)
        _merge(name, target="main")

    @task(
        task_id="cleanup_branch",
        trigger_rule=TriggerRule.ONE_FAILED,
        retries=2,
    )
    def cleanup_branch(**context) -> None:
        """Drop the isolated branch if any upstream Spark / merge step failed.

        ``trigger_rule="one_failed"`` makes Airflow fire this only when the
        upstream chain reports a failure — on the happy path it's skipped.
        """
        from src.utils.nessie_branch import build_branch_name, drop_branch

        execution_date = context.get("ds") or pendulum.now(local_tz).strftime("%Y-%m-%d")
        name = build_branch_name(dag_id="bronze_silver", execution_date=execution_date)
        logger.warning("Upstream failed — dropping orphan Nessie branch '%s'", name)
        drop_branch(name)

    merge = merge_branch()
    cleanup = cleanup_branch()

    # Wiring:
    #   create_branch -> bronze -> silver -> merge
    #                                   \-> cleanup (only if anything before merge failed)
    branch_name >> staging_to_bronze >> bronze_to_silver >> merge
    [staging_to_bronze, bronze_to_silver, merge] >> cleanup


# Instanciar a pipeline
bronze_silver_pipeline()
