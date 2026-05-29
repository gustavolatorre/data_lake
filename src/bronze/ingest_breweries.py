"""Bronze layer — ingest raw JSON from staging into an Iceberg table.

Reads raw brewery JSON files from the staging bucket and writes them to the
Bronze layer as a partitioned Iceberg table via the Nessie catalog.
"""

import logging
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StringType, StructField, StructType

from src.utils.data_quality import check_row_count, log_quality_summary
from src.utils.quality_runner import run_quality_checks
from src.utils.spark_session import create_spark_session

logger = logging.getLogger(__name__)

# Schema matching the API response exactly
BREWERY_SCHEMA = StructType(
    [
        StructField("id", StringType(), True),
        StructField("name", StringType(), True),
        StructField("brewery_type", StringType(), True),
        StructField("address_1", StringType(), True),
        StructField("address_2", StringType(), True),
        StructField("address_3", StringType(), True),
        StructField("city", StringType(), True),
        StructField("state_province", StringType(), True),
        StructField("postal_code", StringType(), True),
        StructField("country", StringType(), True),
        StructField("longitude", DoubleType(), True),
        StructField("latitude", DoubleType(), True),
        StructField("phone", StringType(), True),
        StructField("website_url", StringType(), True),
        StructField("state", StringType(), True),
        StructField("street", StringType(), True),
    ]
)


def ingest(execution_date: str) -> None:
    """Execute the Staging → Bronze ingestion.

    Reads JSON from staging, adds metadata columns, and writes idempotently
    to the Bronze Iceberg table via overwritePartitions on the daily partition.

    Args:
        execution_date: Date string (YYYY-MM-DD) for the staging partition to read.
    """
    spark = create_spark_session("BreweriesStagingToBronze")

    try:
        _run_ingest(spark, execution_date)
    finally:
        spark.stop()
        logger.info("SparkSession stopped")


def _run_ingest(spark: SparkSession, execution_date: str) -> None:
    """Internal ingestion logic.

    Args:
        spark: Active SparkSession.
        execution_date: Date string (YYYY-MM-DD).
    """
    # Read Staging JSON
    staging_path = f"s3a://staging/breweries/{execution_date}/"
    logger.info("Reading staging data from %s", staging_path)

    df = spark.read.schema(BREWERY_SCHEMA).option("multiline", "true").json(staging_path)

    # Add ingestion metadata
    df = df.withColumn("ingestion_date", F.lit(execution_date))
    df = df.withColumn("ingested_at", F.current_timestamp())

    # Cache before quality checks + write — avoids 3x re-scan of the JSON in S3
    df.cache()
    try:
        # Validate before writing — fail fast if staging produced no data
        check_row_count(df, min_rows=1)
        log_quality_summary(df, "bronze", critical_columns=["id", "name", "brewery_type"])

        # P3.7 — Declarative quality contract for the Bronze layer. Rules
        # live in quality/checks/bronze_breweries.yml; any FAIL-severity
        # rule violation raises QualityCheckError and aborts the run.
        run_quality_checks(df, checks_file="bronze_breweries.yml")

        # Write to Iceberg Bronze
        # Idempotency: overwritePartitions() atomically replaces only the partitions
        # present in `df` (i.e. the current ingestion_date). Re-running the same
        # execution_date produces the same final state — no duplicates.
        table_name = "nessie.bronze.breweries"
        logger.info("Writing Bronze Iceberg table: %s", table_name)

        spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.bronze")

        # format-version=2 enables row-level updates and compatibility with Dremio.
        # Only applies to new tables.
        writer = df.writeTo(table_name).tableProperty("format-version", "2").partitionedBy(F.col("ingestion_date"))

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
    from src.utils.logging_config import setup_logging

    setup_logging()

    if len(sys.argv) < 2:
        logger.error("Usage: ingest_to_bronze.py <execution_date>")
        sys.exit(1)

    ingest(sys.argv[1])
