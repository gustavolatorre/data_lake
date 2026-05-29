"""Silver layer — transform Bronze Iceberg table into a clean Silver table.

Reads from the Bronze Iceberg table, applies native Spark transformations,
and performs an atomic MERGE into the Silver Iceberg table with Soft Delete
logic to handle deletions at the source.

Records that fail the row-level quality rules (currently: NULL ``id``) are
diverted to ``nessie.silver.breweries_quarantine`` instead of aborting the
whole run. The MERGE then proceeds on the cleaned subset only.
"""

import logging
import sys

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from src.utils.data_quality import log_quality_summary
from src.utils.spark_session import create_spark_session

logger = logging.getLogger(__name__)

# Guard rail: abort the MERGE if today's snapshot is more than this much smaller
# than the previous active count. Protects against a partial fetch (e.g. network
# instability dropping pagination mid-run) silently soft-deleting half the
# Silver table via WHEN NOT MATCHED BY SOURCE.
MAX_SOURCE_SHRINK_RATIO = 0.20  # 20% drop = abort

# Reason codes written into nessie.silver.breweries_quarantine.quarantine_reason.
# Keep these stable — downstream alerting / dashboards filter on them.
REASON_NULL_ID = "NULL_ID"


class SourceShrinkError(RuntimeError):
    """Raised when today's source count is suspiciously smaller than yesterday's."""


def transform(execution_date: str, nessie_ref: str = "main") -> None:
    """Execute the Bronze → Silver transformation.

    Args:
        execution_date: Date string (YYYY-MM-DD) for the Bronze partition to process.
        nessie_ref: Nessie branch (or tag/hash) the Spark session should bind
            to. P3.1 — Bronze/Silver DAGs pass an isolated ``etl_*`` branch so
            failures don't pollute ``main``.
    """
    spark = create_spark_session("BreweriesBronzeToSilver", nessie_ref=nessie_ref)

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

    # We read the specific partition to ensure we are only processing the latest arrival.
    # The dual filter is intentional after P2.3:
    #   * ingestion_date (string)     — preserves the historical query contract
    #     so anything joining on it keeps working.
    #   * ingestion_ts (timestamp)    — gives Iceberg the predicate it needs to
    #     prune to a single days(ingestion_ts) partition, instead of scanning
    #     the whole Bronze table.
    execution_ts = F.to_timestamp(F.lit(execution_date), "yyyy-MM-dd")
    source_df = (
        spark.table("nessie.bronze.breweries")
        .filter(F.col("ingestion_date") == execution_date)
        .filter(F.col("ingestion_ts") == execution_ts)
    )

    if source_df.isEmpty():
        logger.warning("No data found in Bronze for date %s", execution_date)
        return

    log_quality_summary(
        source_df,
        "bronze",
        critical_columns=["id", "name", "brewery_type", "city", "state", "country"],
    )

    # 2. Split off rows that would fail the MERGE primary key (NULL id) into
    #    the quarantine sink. This used to be a hard abort via
    #    check_null_counts(fail_on_nulls=True); diverting instead keeps the
    #    rest of the day's data flowing while preserving the bad rows for
    #    investigation.
    bad_df = source_df.filter(F.col("id").isNull())
    good_df = source_df.filter(F.col("id").isNotNull())

    bad_count = bad_df.count()
    if bad_count > 0:
        logger.warning("Quarantining %d row(s) with NULL id", bad_count)
        _quarantine_invalid_records(spark, bad_df, REASON_NULL_ID, execution_date)
    else:
        logger.info("No NULL-id rows to quarantine for %s", execution_date)

    if good_df.isEmpty():
        logger.warning("All rows for %s were quarantined; nothing to MERGE", execution_date)
        return

    # 3. Apply transformations on the clean subset only (No UDFs!)
    transformed_df = _apply_native_transformations(good_df)

    # Cache the transformed DF before the chain of count-driven steps below
    # (log_quality_summary, shrink guard, MERGE source view).
    # Without this each step re-runs the full Bronze read + transformations.
    transformed_df.cache()
    try:
        log_quality_summary(
            transformed_df,
            "silver",
            critical_columns=["id", "name", "brewery_type", "city", "state", "country"],
        )

        # 4. Guard rail — protect against partial fetch silently soft-deleting rows
        _assert_source_not_shrinking(spark, transformed_df)

        # 5. Create a temporary view for the MERGE operation
        transformed_df.createOrReplaceTempView("v_transformed_breweries")

        # 6. Perform Atomic MERGE into Silver
        _execute_merge(spark)
    finally:
        transformed_df.unpersist()


def _quarantine_invalid_records(
    spark: SparkSession,
    bad_df: DataFrame,
    reason: str,
    execution_date: str,
) -> None:
    """Append rows that failed Silver quality rules to the quarantine table.

    The quarantine table is append-only and partitioned by ``quarantine_date``,
    so a re-run of the same ``execution_date`` reinserts the same rows
    (idempotency caller's responsibility — clearing the partition before re-run
    if needed). This is intentional: we'd rather see duplicate quarantine rows
    than drop incident evidence silently.

    Args:
        spark: Active SparkSession.
        bad_df: DataFrame containing the rows to quarantine. Expected schema is
            the Bronze schema (id, name, brewery_type, city, state, country,
            ingestion_date, ingested_at).
        reason: Stable string code (e.g. ``REASON_NULL_ID``). Goes into the
            ``quarantine_reason`` column for downstream filtering.
        execution_date: Date the upstream ingest ran for. Stored on every
            quarantined row.
    """
    enriched = (
        bad_df.select(
            "id",
            "name",
            "brewery_type",
            "address_1",
            "city",
            "state",
            "country",
            "ingestion_date",
            "ingested_at",
        )
        .withColumn("quarantine_reason", F.lit(reason))
        .withColumn("quarantined_at", F.current_timestamp())
        .withColumn("quarantine_date", F.lit(execution_date))
    )

    table_name = "nessie.silver.breweries_quarantine"
    spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.silver")

    if not spark.catalog.tableExists(table_name):
        logger.info("Creating quarantine table %s for the first time", table_name)
        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                id STRING,
                name STRING,
                brewery_type STRING,
                address_1 STRING,
                city STRING,
                state STRING,
                country STRING,
                ingestion_date STRING,
                ingested_at TIMESTAMP,
                quarantine_reason STRING,
                quarantined_at TIMESTAMP,
                quarantine_date STRING
            )
            USING iceberg
            PARTITIONED BY (quarantine_date)
            TBLPROPERTIES ('format-version'='2')
        """)

    # Append-only; we want history. Idempotency is via re-running on a fresh
    # partition only (clear the partition before re-run if needed).
    enriched.writeTo(table_name).append()
    logger.info("Appended %d row(s) to %s (reason=%s)", enriched.count(), table_name, reason)


def _assert_source_not_shrinking(spark: SparkSession, source_df: DataFrame) -> None:
    """Abort the run if today's source is much smaller than yesterday's Silver.

    Compares the row count of ``source_df`` to the count of currently-active
    rows in ``nessie.silver.breweries``. A drop greater than
    ``MAX_SOURCE_SHRINK_RATIO`` aborts the MERGE — the most likely cause is a
    partial API fetch (pagination interrupted), and a global soft-delete via
    ``WHEN NOT MATCHED BY SOURCE`` would deactivate the missing rows
    incorrectly.

    Skips the check when the Silver table does not exist yet (first run) or
    has zero active rows — there's nothing to lose.

    Args:
        spark: Active SparkSession.
        source_df: The (already-transformed) source DataFrame about to be
            MERGE'd into Silver.

    Raises:
        SourceShrinkError: When today's source has shrunk past the threshold.
    """
    if not spark.catalog.tableExists("nessie.silver.breweries"):
        logger.info("Silver table does not exist yet — skipping shrink guard")
        return

    prev_active = spark.sql("SELECT COUNT(*) AS c FROM nessie.silver.breweries WHERE is_active = true").collect()[0][
        "c"
    ]
    if prev_active == 0:
        logger.info("Silver table has 0 active rows — skipping shrink guard")
        return

    curr_count = source_df.count()
    threshold = int(prev_active * (1 - MAX_SOURCE_SHRINK_RATIO))

    if curr_count < threshold:
        msg = (
            f"Source row count dropped > {MAX_SOURCE_SHRINK_RATIO:.0%}: "
            f"previous active={prev_active}, current source={curr_count}. "
            f"Aborting MERGE to prevent mass false-positive soft-deletes "
            f"(suspect: partial API fetch)."
        )
        logger.error(msg)
        raise SourceShrinkError(msg)

    logger.info(
        "Shrink guard OK: previous active=%d, current source=%d (threshold=%d)",
        prev_active,
        curr_count,
        threshold,
    )


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
    import argparse

    from src.utils.logging_config import setup_logging

    setup_logging()

    parser = argparse.ArgumentParser(description="Bronze -> Silver transformation")
    parser.add_argument("execution_date", help="YYYY-MM-DD (matches the Airflow logical date)")
    parser.add_argument(
        "--nessie-ref",
        default="main",
        help="Nessie branch / tag / hash to bind the catalog to (default: main)",
    )
    args = parser.parse_args(sys.argv[1:])

    transform(args.execution_date, nessie_ref=args.nessie_ref)
