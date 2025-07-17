from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
import logging
import traceback
import pendulum

from scripts.breweries_fetch_and_upload_to_bronze import breweries_fetch_and_upload_to_bronze

local_tz = pendulum.timezone("America/Sao_Paulo")

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=30),
}

def start_task():
    logging.info("Starting breweries pipeline...")

def safe_breweries_fetch_and_upload_to_bronze(**kwargs):
    try:
        breweries_fetch_and_upload_to_bronze()
    except Exception:
        logging.error("Erro ao buscar e salvar dados da API:")
        logging.error(traceback.format_exc())
        raise

def end_task():
    logging.info("Breweries pipeline completed successfully.")

with DAG(
    dag_id="breweries_pipeline",
    default_args=default_args,
    description="Busca dados da API OpenBreweryDB e salva na camada bronze",
    schedule_interval="@daily",
    start_date=pendulum.datetime(2024, 1, 1, tz=local_tz),
    catchup=False,
    max_active_runs=1,
    tags=["api", "minio"]
) as dag:

    start = PythonOperator(
        task_id="start",
        python_callable=start_task,
    )

    task_fetch_and_upload = PythonOperator(
        task_id="breweries_fetch_and_upload_to_bronze",
        python_callable=safe_breweries_fetch_and_upload_to_bronze,
        provide_context=True,
    )

    breweries_bronze_to_silver = SparkSubmitOperator(
        task_id="breweries_bronze_to_silver",
        conn_id="spark_docker",
        application="jobs/python/breweries_bronze_to_silver.py",
        packages=(
            "org.apache.hudi:hudi-spark3.5-bundle_2.12:0.15.0,"
            "org.apache.hadoop:hadoop-aws:3.3.2"
        ),
        conf={
            "spark.driver.memory": "2g",
            "spark.executor.memory": "2g",
            "spark.executor.instances": "1",
            "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
            "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.hudi.catalog.HoodieCatalog",
            "spark.sql.extensions": "org.apache.spark.sql.hudi.HoodieSparkSessionExtension",
            "spark.kryo.registrator": "org.apache.spark.HoodieSparkKryoRegistrar"
        },
        execution_timeout=timedelta(minutes=20),
        retries=2,
        retry_delay=timedelta(minutes=3),
    )

    aggregate_breweries = SparkSubmitOperator(
        task_id="aggregate_breweries",
        conn_id="spark_docker",
        application="jobs/python/aggregate_breweries.py",
        packages=(
            "org.apache.hudi:hudi-spark3.5-bundle_2.12:0.15.0,"
            "org.apache.hadoop:hadoop-aws:3.3.2"
        ),
        conf={
            "spark.driver.memory": "2g",
            "spark.executor.memory": "2g",
            "spark.executor.instances": "1",
            "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
        },
        execution_timeout=timedelta(minutes=20),
        retries=2,
        retry_delay=timedelta(minutes=3),
    )

    end = PythonOperator(
        task_id="end",
        python_callable=end_task,
    )

    start >> task_fetch_and_upload >> breweries_bronze_to_silver >> aggregate_breweries >> end
