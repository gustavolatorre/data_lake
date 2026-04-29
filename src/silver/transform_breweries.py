"""Silver layer — transform raw brewery JSON into a clean Iceberg table.

Reads raw JSON from the Bronze layer, applies data quality checks and
transformations, and writes a partitioned Iceberg table to the Silver layer
via the Nessie REST catalog.

Transformations applied:
    - Unicode normalization on ``state`` column (generic, not hardcoded)
    - Null ``state`` values replaced with ``__UNKNOWN__``
    - Schema validation against expected columns
    - Deduplication by ``id`` (keep latest by ``ingestion_date``)
    - Addition of ``ingestion_date`` column from Airflow execution date
"""

import logging
import sys
import unicodedata

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StringType, StructField, StructType

from src.config.settings import get_settings
from src.utils.data_quality import check_null_counts, check_row_count, check_schema, log_quality_summary
from src.utils.spark_session import create_spark_session

logger = logging.getLogger(__name__)

# Expected schema for raw brewery JSON
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

EXPECTED_COLUMNS = {field.name for field in BREWERY_SCHEMA.fields}

CRITICAL_COLUMNS = ["id", "name", "brewery_type"]


def normalize_unicode(text: str | None) -> str | None:
    """Remove diacritical marks from a string using NFC → NFD decomposition.

    Handles the API's encoding corruption (e.g., ``Kärnten`` → ``Karnten``)
    generically rather than with hardcoded replacements.

    Args:
        text: Input string to normalize.

    Returns:
        Normalized string without diacritical marks, or None if input is None.
    """
    if text is None:
        return None
    normalized = unicodedata.normalize("NFD", text)
    return "".join(c for c in normalized if unicodedata.category(c) != "Mn")


def transform(execution_date: str) -> None:
    """Execute the Bronze → Silver transformation.

    Reads raw JSON from MinIO, validates, cleans, and writes an Iceberg table
    partitioned by ``state`` to the Nessie catalog.

    Args:
        execution_date: Date string (YYYY-MM-DD) for the Bronze partition to read
            and for the ``ingestion_date`` column value.
    """
    spark = create_spark_session("BreweriesBronzeToSilver")

    try:
        _run_transform(spark, execution_date)
    finally:
        spark.stop()
        logger.info("SparkSession stopped")


def _run_transform(spark: SparkSession, execution_date: str) -> None:
    """Internal transformation logic.

    Args:
        spark: Active SparkSession.
        execution_date: Date string (YYYY-MM-DD).
    """
    # Read Bronze JSON
    bronze_path = f"s3a://bronze/breweries/{execution_date}/"
    logger.info("Reading Bronze data from %s", bronze_path)

    df = spark.read.schema(BREWERY_SCHEMA).option("multiline", "true").json(bronze_path)

    # Data quality checks
    check_schema(df, EXPECTED_COLUMNS)
    check_row_count(df, min_rows=1)
    check_null_counts(df, CRITICAL_COLUMNS)
    log_quality_summary(df, "bronze", CRITICAL_COLUMNS)

    # Apply transformations
    df = _apply_transformations(df, execution_date, spark)

    # Write to Iceberg via Nessie catalog
    _write_iceberg(spark, df)


def _apply_transformations(
    df: DataFrame,
    execution_date: str,
    spark: SparkSession,
) -> DataFrame:
    """Apply all cleaning and enrichment transformations.

    Args:
        df: Raw Bronze DataFrame.
        execution_date: Date string for the ingestion_date column.
        spark: SparkSession (needed for UDF registration).

    Returns:
        Cleaned and enriched DataFrame ready for Silver write.
    """
    # Register UDF for unicode normalization
    normalize_udf = F.udf(normalize_unicode, StringType())

    # Normalize state names (generic unicode handling)
    df = df.withColumn("state", normalize_udf(F.col("state")))

    # Replace null states with a sentinel value for clean partitioning
    df = df.withColumn(
        "state",
        F.when(F.col("state").isNull(), F.lit("__UNKNOWN__")).otherwise(F.col("state")),
    )

    # Add ingestion date (from Airflow execution_date for idempotency)
    df = df.withColumn("ingestion_date", F.lit(execution_date))

    # Deduplicate by id, keeping the latest record
    from pyspark.sql.window import Window

    window = Window.partitionBy("id").orderBy(F.col("ingestion_date").desc())
    df = df.withColumn("_row_num", F.row_number().over(window)).filter(F.col("_row_num") == 1).drop("_row_num")

    record_count = df.count()
    logger.info("Transformation complete: %d records after dedup and cleaning", record_count)

    return df


def _write_iceberg(spark: SparkSession, df: DataFrame) -> None:
    """Write the transformed DataFrame as an Iceberg table in Nessie catalog.

    Creates the ``nessie.silver`` namespace if it doesn't exist, then writes
    the table with ``createOrReplace`` for idempotent execution.

    Args:
        spark: Active SparkSession.
        df: Transformed DataFrame to write.
    """
    logger.info("Writing Silver Iceberg table to Nessie catalog")

    spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.silver")

    df.writeTo("nessie.silver.breweries").tableProperty("format-version", "2").partitionedBy(
        F.col("state")
    ).createOrReplace()

    logger.info("Silver Iceberg table written successfully: nessie.silver.breweries")


if __name__ == "__main__":
    logging.basicConfig(stream=sys.stdout, level=logging.INFO)

    if len(sys.argv) < 2:
        logger.error("Usage: transform_breweries.py <execution_date>")
        sys.exit(1)

    transform(sys.argv[1])
