"""Airflow 3.2.1 DAG — Iceberg table maintenance.

Runs weekly to keep the Bronze and Silver Iceberg tables healthy:
- ``rewrite_data_files`` compacts small files into right-sized ones, reducing
  read amplification on the next pipeline runs.
- ``expire_snapshots`` reclaims storage by removing snapshots older than the
  retention window, while keeping the minimum required for time-travel.
- ``remove_orphan_files`` cleans up data files no longer referenced by any
  snapshot (typically left behind by failed writes).

These are non-destructive when configured correctly: time-travel within the
retention window still works, and the current snapshot of each table is always
preserved.
"""

import logging
from datetime import timedelta

import pendulum
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.sdk import dag
from callbacks import build_failure_callback

logger = logging.getLogger("airflow.task")

local_tz = pendulum.timezone("America/Sao_Paulo")

SPARK_CONF = {
    "spark.driver.memory": "2g",
    "spark.executor.memory": "2g",
    "spark.executor.instances": "1",
}

# Retain ~30 days of snapshots on production tables. This balances storage
# overhead against the ability to roll back / time-travel for incident response.
SNAPSHOT_RETENTION_DAYS = 30
# Keep at least this many recent snapshots even if all are within the retention
# window. Belt-and-suspenders so a sudden flurry of writes can't expire history
# we might still need.
MIN_SNAPSHOTS_TO_KEEP = 5

on_failure_callback = build_failure_callback("ICEBERG MAINTENANCE")


@dag(
    dag_id="iceberg_maintenance",
    description="Weekly maintenance: rewrite data files, expire snapshots, remove orphan files",
    schedule="@weekly",
    start_date=pendulum.datetime(2024, 1, 1, tz=local_tz),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "data-engineering",
        "depends_on_past": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=10),
        "execution_timeout": timedelta(hours=1),
        "on_failure_callback": on_failure_callback,
    },
    tags=["iceberg", "maintenance", "weekly"],
)
def iceberg_maintenance_pipeline():
    """Schedule maintenance procedures across Bronze and Silver tables."""
    SparkSubmitOperator(
        task_id="run_iceberg_maintenance",
        conn_id="spark_docker",
        application="/opt/airflow/src/maintenance/iceberg_maintenance.py",
        application_args=[
            "--retention-days",
            str(SNAPSHOT_RETENTION_DAYS),
            "--min-snapshots",
            str(MIN_SNAPSHOTS_TO_KEEP),
        ],
        conf=SPARK_CONF,
        execution_timeout=timedelta(minutes=45),
    )


iceberg_maintenance_pipeline()
