"""Bronze layer — ingest raw JSON from staging into an Iceberg table.

Reads raw Brasileirao JSON files from the staging bucket and writes them to the
Bronze layer as a partitioned Iceberg table via the Nessie catalog.
"""

import logging
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Iceberg partition transform.
from pyspark.sql.functions.partitioning import days
from pyspark.sql.types import BooleanType, IntegerType, StringType, StructField, StructType

from src.utils.data_quality import check_row_count, log_quality_summary
from src.utils.quality_runner import run_quality_checks
from src.utils.spark_session import create_spark_session

logger = logging.getLogger(__name__)

# Schema matching the API response exactly
BRASILEIRAO_SCHEMA = StructType(
    [
        StructField("matchweek", IntegerType(), True),
        StructField("home_team", StringType(), True),
        StructField("home_team_code", StringType(), True),
        StructField("away_team", StringType(), True),
        StructField("away_team_code", StringType(), True),
        StructField("score_home", IntegerType(), True),
        StructField("score_away", IntegerType(), True),
        StructField("date", StringType(), True),
        StructField("kickoff_time", StringType(), True),
        StructField("stadium", StringType(), True),
        StructField("broadcast", StringType(), True),
        StructField("match_url", StringType(), True),
        StructField("match_started", BooleanType(), True),
        StructField("source", StringType(), True),
        StructField("ge_match_id", IntegerType(), True),  # GE ID is typically an integer
    ]
)


def ingest(execution_date: str, nessie_ref: str = "main") -> None:
    """Execute the Staging -> Bronze ingestion.

    Reads JSON from staging, adds metadata columns, and writes idempotently
    to the Bronze Iceberg table via overwritePartitions on the daily partition.

    Args:
        execution_date: Date string (YYYY-MM-DD) for the staging partition to read.
        nessie_ref: Nessie branch (or tag/hash) the Spark session should bind to.
    """
    spark = create_spark_session("BrasileiraoStagingToBronze", nessie_ref=nessie_ref)

    try:
        _run_ingest(spark, execution_date)
    finally:
        spark.stop()
        logger.info("SparkSession stopped")


def _run_ingest(
    spark: SparkSession,
    execution_date: str,
    *,
    staging_path_base: str = "s3a://staging/brasileirao",
) -> None:
    """Internal ingestion logic.

    Args:
        spark: Active SparkSession.
        execution_date: Date string (YYYY-MM-DD).
        staging_path_base: Base URI for the staging area.
    """
    staging_path = f"{staging_path_base.rstrip('/')}/{execution_date}/"
    logger.info("Reading staging data from %s", staging_path)

    df = spark.read.schema(BRASILEIRAO_SCHEMA).option("multiline", "true").json(staging_path)

    # ingestion_date: stable string YYYY-MM-DD; used by Silver as a join key.
    # ingested_at:    real wall-clock — useful in forensics + dedup window.
    # ingestion_ts:   logical timestamp tied to execution_date — drives the
    #                 hidden partitioning. Two re-runs of the same
    #                 execution_date keep landing on the SAME partition,
    #                 preserving idempotency under overwritePartitions().
    df = df.withColumn("ingestion_date", F.lit(execution_date))
    df = df.withColumn("ingested_at", F.current_timestamp())
    df = df.withColumn("ingestion_ts", F.to_timestamp(F.lit(execution_date), "yyyy-MM-dd"))

    df.cache()
    try:
        # Minimal data quality validation
        check_row_count(df, min_rows=1)
        # As requested, no critical columns for Bronze Brasileirao ("as is")
        log_quality_summary(df, "bronze", critical_columns=[])

        # Declarative quality contract for the Bronze layer.
        run_quality_checks(df, checks_file="bronze_brasileirao.yml")

        table_name = "nessie.bronze.brasileirao"
        logger.info("Writing Bronze Iceberg table: %s", table_name)

        spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.bronze")

        writer = (
            df.writeTo(table_name)
            .tableProperty("format-version", "2")
            .tableProperty("gc.enabled", "true")
            .partitionedBy(days(F.col("ingestion_ts")))
        )

        if spark.catalog.tableExists(table_name):
            logger.info("Table %s exists. Overwriting partition for ingestion_date.", table_name)
            writer.overwritePartitions()
        else:
            logger.info("Table %s does not exist. Creating with initial partition.", table_name)
            writer.create()

        record_count = df.count()
        logger.info("Bronze ingestion complete: %d records written for %s", record_count, execution_date)
    finally:
        df.unpersist()


if __name__ == "__main__":
    import argparse

    from src.utils.logging_config import setup_logging

    setup_logging()

    parser = argparse.ArgumentParser(description="Staging -> Bronze ingestion for Brasileirao")
    parser.add_argument("execution_date", help="YYYY-MM-DD (matches the Airflow logical date)")
    parser.add_argument(
        "--nessie-ref",
        default="main",
        help="Nessie branch / tag / hash to bind the catalog to (default: main)",
    )
    args = parser.parse_args(sys.argv[1:])

    ingest(args.execution_date, nessie_ref=args.nessie_ref)
