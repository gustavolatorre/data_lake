"""Airflow DAG — Breweries Medallion Pipeline.

Orchestrates the full data lakehouse pipeline:
    1. Fetch brewery data from OpenBreweryDB API → MinIO Bronze (JSON)
    2. Transform and clean → Iceberg Silver (partitioned by state)
    3. Aggregate by type and location → Iceberg Gold

Uses TaskFlow API for the Bronze task and SparkSubmitOperator for Silver/Gold
Spark jobs. Includes on_failure_callback for alerting.
"""

import logging
from datetime import timedelta

import pendulum
from airflow.decorators import dag, task
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

logger = logging.getLogger("airflow.task")

local_tz = pendulum.timezone("America/Sao_Paulo")

# Iceberg + S3A JARs are pre-installed in the Spark image (Dockerfile.spark)
# No --packages needed: baked JARs in /opt/spark/jars/ avoid classloader conflicts

SPARK_CONF = {
    "spark.driver.memory": "2g",
    "spark.executor.memory": "2g",
    "spark.executor.instances": "1",
    "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    "spark.sql.catalog.nessie": "org.apache.iceberg.spark.SparkCatalog",
    "spark.sql.catalog.nessie.catalog-impl": "org.apache.iceberg.nessie.NessieCatalog",
    "spark.sql.catalog.nessie.uri": "http://nessie:19120/api/v2",
    "spark.sql.catalog.nessie.ref": "main",
    "spark.sql.catalog.nessie.io-impl": "org.apache.iceberg.hadoop.HadoopFileIO",
    "spark.sql.catalog.nessie.warehouse": "s3a://warehouse/",
    "spark.hadoop.fs.s3a.endpoint": "http://minio:9000",
    "spark.hadoop.fs.s3a.access.key": "{{ var.value.MINIO_ROOT_USER }}",
    "spark.hadoop.fs.s3a.secret.key": "{{ var.value.MINIO_ROOT_PASSWORD }}",
    "spark.hadoop.fs.s3a.path.style.access": "true",
    "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
}


def on_failure_callback(context: dict) -> None:
    """Log detailed failure information when a task fails.

    In a production environment, this would send alerts via Slack, PagerDuty,
    or email. For this portfolio project, it logs comprehensive context.

    Args:
        context: Airflow context dictionary with task instance details.
    """
    task_instance = context.get("task_instance")
    dag_id = context.get("dag").dag_id if context.get("dag") else "unknown"
    task_id = task_instance.task_id if task_instance else "unknown"
    execution_date = context.get("ds", "unknown")
    exception = context.get("exception", "No exception info")

    logger.error(
        "PIPELINE FAILURE | dag=%s | task=%s | date=%s | error=%s",
        dag_id,
        task_id,
        execution_date,
        exception,
    )


@dag(
    dag_id="breweries_pipeline",
    description="Medallion pipeline: OpenBreweryDB API → Bronze → Silver (Iceberg) → Gold (Iceberg)",
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
    tags=["brewery", "medallion", "iceberg", "nessie"],
)
def breweries_pipeline():
    """Breweries data pipeline using medallion architecture with Apache Iceberg."""

    @task()
    def fetch_bronze(**context) -> int:
        """Fetch brewery data from API and upload to MinIO Bronze bucket.

        Returns:
            Number of records fetched.
        """
        execution_date = context["ds"]
        logger.info("Starting Bronze ingestion for date=%s", execution_date)

        from src.bronze.fetch_breweries import fetch_and_upload

        total = fetch_and_upload(execution_date)
        logger.info("Bronze ingestion complete: %d records", total)
        return total

    bronze_to_silver = SparkSubmitOperator(
        task_id="bronze_to_silver",
        conn_id="spark_docker",
        application="/opt/airflow/src/silver/transform_breweries.py",
        application_args=["{{ ds }}"],
        conf=SPARK_CONF,
        execution_timeout=timedelta(minutes=20),
        retries=2,
        retry_delay=timedelta(minutes=3),
    )

    silver_to_gold = SparkSubmitOperator(
        task_id="silver_to_gold",
        conn_id="spark_docker",
        application="/opt/airflow/src/gold/aggregate_breweries.py",
        conf=SPARK_CONF,
        execution_timeout=timedelta(minutes=20),
        retries=2,
        retry_delay=timedelta(minutes=3),
    )

    # Pipeline flow
    fetch_bronze() >> bronze_to_silver >> silver_to_gold


# Instantiate the DAG
breweries_pipeline()
