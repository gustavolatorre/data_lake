"""Silver layer — transform Bronze Iceberg table into a clean Silver table.

Reads from the Bronze Iceberg table, applies native Spark transformations,
and performs an atomic MERGE into the Silver Iceberg table with Soft Delete
logic to handle deletions at the source.
"""

import logging
import sys

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.utils.data_quality import check_null_counts, log_quality_summary
from src.utils.spark_session import create_spark_session

logger = logging.getLogger(__name__)


def transform(execution_date: str) -> None:
    """Execute the Bronze → Silver transformation.

    Args:
        execution_date: Date string (YYYY-MM-DD) for the Bronze partition to process.
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
    # 1. Read the latest snapshot from Bronze (filtered by execution_date)
    logger.info("Reading Bronze data for ingestion_date=%s", execution_date)

    # We read the specific partition to ensure we are only processing the latest arrival
    source_df = spark.table("nessie.bronze.breweries").filter(F.col("ingestion_date") == execution_date)

    if source_df.isEmpty():
        logger.warning("No data found in Bronze for date %s", execution_date)
        return

    log_quality_summary(
        source_df,
        "bronze",
        critical_columns=["id", "name", "brewery_type", "city", "state", "country"],
    )

    # 2. Apply transformations using native Spark functions (No UDFs!)
    transformed_df = _apply_native_transformations(source_df)

    # Ensure id was never nullified by transformations before writing to Silver
    check_null_counts(transformed_df, ["id"], fail_on_nulls=True)
    log_quality_summary(
        transformed_df,
        "silver",
        critical_columns=["id", "name", "brewery_type", "city", "state", "country"],
    )

    # 3. Create a temporary view for the MERGE operation
    transformed_df.createOrReplaceTempView("v_transformed_breweries")

    # 4. Perform Atomic MERGE into Silver
    _execute_merge(spark)


def _apply_native_transformations(df: DataFrame) -> DataFrame:
    """Apply cleaning using native Spark SQL functions.

    Args:
        df: Input Bronze DataFrame.

    Returns:
        Transformed DataFrame.
    """
    # Unicode normalization (native translate instead of Python UDF)
    # This covers common Portuguese/European accents
    accents = "áàâãäéèêëíìîïóòôõöúùûüçÁÀÂÃÄÉÈÊËÍÌÎÏÓÒÔÕÖÚÙÛÜÇ"
    clean = "aaaaaeeeeiiiiooooouuuucAAAAAEEEEIIIIOOOOOUUUUC"

    df = df.withColumn("state", F.translate(F.col("state"), accents, clean))

    # Replace nulls with sentinel value for partitioning
    df = df.withColumn("state", F.coalesce(F.col("state"), F.lit("__UNKNOWN__")))

    # Deduplicate within the source snapshot (just in case)
    # Using window function here is fine because it's on the source delta, not the target table
    from pyspark.sql.window import Window

    window = Window.partitionBy("id").orderBy(F.col("ingested_at").desc())

    df = df.withColumn("_rn", F.row_number().over(window)).filter(F.col("_rn") == 1).drop("_rn")

    return df


def _execute_merge(spark: SparkSession) -> None:
    """Execute Iceberg MERGE INTO with Soft Delete logic.

    Logic:
    - Match by ID: Update attributes and set is_active = true.
    - Not Matched by Source: Update is_active = false (Deletions).
    - Not Matched: Insert as new record with is_active = true.
    """
    logger.info("Executing MERGE INTO nessie.silver.breweries")

    spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.silver")

    # Ensure target table exists with correct schema before MERGE
    # In a real scenario, this would be handled by a migration or DDL script
    if not spark.catalog.tableExists("nessie.silver.breweries"):
        logger.info("Creating Silver table for the first time")
        spark.sql("""
            CREATE TABLE IF NOT EXISTS nessie.silver.breweries (
                id STRING,
                name STRING,
                brewery_type STRING,
                address_1 STRING,
                city STRING,
                state STRING,
                country STRING,
                is_active BOOLEAN,
                updated_at TIMESTAMP,
                ingestion_date STRING
            )
            USING iceberg
            PARTITIONED BY (state)
            TBLPROPERTIES ('format-version'='2')
        """)

    spark.sql("""
        MERGE INTO nessie.silver.breweries t
        USING v_transformed_breweries s
        ON t.id = s.id
        WHEN MATCHED THEN
            UPDATE SET
                t.name = s.name,
                t.brewery_type = s.brewery_type,
                t.address_1 = s.address_1,
                t.city = s.city,
                t.state = s.state,
                t.country = s.country,
                t.is_active = true,
                t.updated_at = current_timestamp()
        WHEN NOT MATCHED THEN
            INSERT (id, name, brewery_type, address_1, city, state, country, is_active, updated_at, ingestion_date)
            VALUES (s.id, s.name, s.brewery_type, s.address_1, s.city, s.state, s.country, true, current_timestamp(), s.ingestion_date)
        WHEN NOT MATCHED BY SOURCE THEN
            UPDATE SET
                t.is_active = false,
                t.updated_at = current_timestamp()
    """)

    logger.info("Silver MERGE complete")


if __name__ == "__main__":
    logging.basicConfig(stream=sys.stdout, level=logging.INFO)

    if len(sys.argv) < 2:
        logger.error("Usage: transform_breweries.py <execution_date>")
        sys.exit(1)

    transform(sys.argv[1])
