"""Bronze layer — multi-date staging scan with high-watermark for Brasileirao.

Why this differs from the breweries Bronze:
The Brasileirao staging partitions files by **match date** (the day the game
was played), not by Airflow's ``execution_date``. A single staging run can
upload N dates at once (first-run backfill of historical matches) and an
asset-triggered Bronze run cannot know in advance which dates landed.

So instead of reading a single ``{execution_date}`` folder like breweries
does, this script:

* scans **all** ``s3a://staging/brasileirao/*/matches.json`` files,
* derives ``ingestion_date`` from the **path** (source of truth — survives
  retroactive amendments to ``match.date`` inside the JSON),
* asks the existing Bronze table for ``MAX(ingestion_date)`` (the
  high-watermark) — Iceberg answers that from manifest metadata, no scan,
* filters the new DataFrame with ``ingestion_date >= HW`` (note: ``>=``,
  not ``>``, so the HW date is always re-processed — that captures updates
  to a same-day staging file, e.g. game 1 ingested at 15:00 then game 2
  re-uploaded at 23:00 sobrescrevendo o mesmo arquivo),
* writes idempotently via ``overwritePartitions()`` — only the partitions
  present in the DataFrame are touched; historical partitions are intact.

``--full-refresh`` is an opt-in manual flag for the rare case of a
retroactive amendment to a date older than HW (e.g. arbitrage review a
week later). The daily DAG never passes it.
"""

import logging
import sys

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

# Iceberg partition transform. The shim in pyspark.sql.functions was
# deprecated in 4.0 in favor of pyspark.sql.functions.partitioning.
from pyspark.sql.functions.partitioning import days
from pyspark.sql.types import BooleanType, IntegerType, StringType, StructField, StructType

from src.utils.data_quality import check_row_count, log_quality_summary
from src.utils.quality_runner import run_quality_checks
from src.utils.spark_session import create_spark_session

logger = logging.getLogger(__name__)

BRASILEIRAO_TABLE = "nessie.bronze.brasileirao"
STAGING_PATH_BASE = "s3a://staging/brasileirao"

# Regex extracts the date folder (YYYY-MM-DD) from a staging file path like
# ``s3a://staging/brasileirao/2026-05-30/matches.json``. Anchored on the
# ``/brasileirao/`` segment so it can't pick up a stray date elsewhere in
# the URI.
_PATH_DATE_REGEX = r"/brasileirao/(\d{4}-\d{2}-\d{2})/"

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
        # GE API returns `id` as a UUID-style string (the campeonato itself is
        # a UUID — see GE_API_BASE in fetch_brasileirao.py). IntegerType here
        # silently nulls every value under Spark's PERMISSIVE read mode.
        StructField("ge_match_id", StringType(), True),
    ]
)


def ingest(execution_date: str, nessie_ref: str = "main", *, full_refresh: bool = False) -> None:
    """Execute the Staging -> Bronze ingestion.

    Args:
        execution_date: Date string (YYYY-MM-DD) from the Airflow DAG. With
            the multi-date scan, this is purely **informational** (logged
            for context) — partitioning is driven by the path-derived
            ``ingestion_date`` of each row, not by this argument.
        nessie_ref: Nessie branch (or tag/hash) the Spark session should
            bind to. The Bronze/Silver DAG passes an isolated ``etl_*``
            branch so failures don't pollute ``main``.
        full_refresh: When True, ignore the Bronze high-watermark and
            re-ingest every staging date. Manual escape hatch for
            retroactive amendments to a date older than HW; never used by
            the daily DAG.
    """
    spark = create_spark_session("BrasileiraoStagingToBronze", nessie_ref=nessie_ref)

    try:
        _run_ingest(spark, execution_date, full_refresh=full_refresh)
    finally:
        spark.stop()
        logger.info("SparkSession stopped")


def _run_ingest(
    spark: SparkSession,
    execution_date: str,
    *,
    full_refresh: bool = False,
    staging_path_base: str = STAGING_PATH_BASE,
) -> None:
    """Internal ingestion logic.

    Args:
        spark: Active SparkSession.
        execution_date: Informational date string (YYYY-MM-DD); see ``ingest``.
        full_refresh: Skip the high-watermark filter when True.
        staging_path_base: Base URI for the staging area. Tests pass a
            ``file://`` path for local Hadoop-catalog warehouses.
    """
    staging_glob = f"{staging_path_base.rstrip('/')}/*/matches.json"
    logger.info(
        "Reading staging glob=%s (execution_date=%s, full_refresh=%s)",
        staging_glob,
        execution_date,
        full_refresh,
    )

    df = _read_staging_with_ingestion_date(spark, staging_glob)
    if df is None:
        logger.info("No staging files matched %s — nothing to ingest", staging_glob)
        return

    df = _apply_watermark_filter(spark, df, full_refresh=full_refresh)

    if df.rdd.isEmpty():
        logger.info("No staging dates beyond watermark — skipping write")
        return

    # ingested_at: real wall-clock, useful in forensics / cross-batch dedup.
    # ingestion_ts: logical timestamp derived from ingestion_date — drives
    #   the hidden partitioning so overwritePartitions() is idempotent per
    #   day, regardless of when the Bronze job actually ran.
    df = df.withColumn("ingested_at", F.current_timestamp())
    df = df.withColumn("ingestion_ts", F.to_timestamp(F.col("ingestion_date"), "yyyy-MM-dd"))

    df.cache()
    try:
        # Per-date observability snapshot. Useful when a single Bronze run
        # spans several staging dates (first-run backfill, recovery after
        # downtime) — surfaces in the SparkSubmit task log so we can confirm
        # what was written without opening Iceberg.
        per_date_rows = df.groupBy("ingestion_date").count().orderBy("ingestion_date").collect()
        dates_to_write = [r.ingestion_date for r in per_date_rows]
        for r in per_date_rows:
            logger.info("Planned write: date=%s rows=%d", r.ingestion_date, r["count"])

        check_row_count(df, min_rows=1)
        log_quality_summary(df, "bronze", critical_columns=[])
        run_quality_checks(df, checks_file="bronze_brasileirao.yml")

        spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.bronze")

        writer = (
            df.writeTo(BRASILEIRAO_TABLE)
            .tableProperty("format-version", "2")
            .tableProperty("gc.enabled", "true")
            .partitionedBy(days(F.col("ingestion_ts")))
        )

        if spark.catalog.tableExists(BRASILEIRAO_TABLE):
            logger.info("Overwriting partitions in %s: %s", BRASILEIRAO_TABLE, dates_to_write)
            writer.overwritePartitions()
        else:
            logger.info("Creating %s with initial partitions: %s", BRASILEIRAO_TABLE, dates_to_write)
            writer.create()

        total = sum(r["count"] for r in per_date_rows)
        logger.info("Bronze ingestion complete: %d records across %d date(s)", total, len(dates_to_write))
    finally:
        df.unpersist()


def _read_staging_with_ingestion_date(spark: SparkSession, staging_glob: str) -> DataFrame | None:
    """Read the staging glob and add a path-derived ``ingestion_date`` column.

    Returns None when the glob matches zero files — the asset can be
    emitted by staging even when no upload happened, and we want a clean
    skip rather than a cryptic AnalysisException.
    """
    try:
        raw = spark.read.schema(BRASILEIRAO_SCHEMA).option("multiline", "true").json(staging_glob)
    except Exception as exc:  # noqa: BLE001
        # AnalysisException + a "Path does not exist" / "0 files" message
        # is the expected shape when the bucket exists but the brasileirao
        # prefix is empty. Catching broadly because PySpark 3.5/4.0 split
        # this between pyspark.errors.AnalysisException and the legacy
        # pyspark.sql.utils variant.
        msg = str(exc).lower()
        if "path does not exist" in msg or "unable to infer schema" in msg or "no files" in msg:
            return None
        raise

    return raw.withColumn(
        "ingestion_date",
        F.regexp_extract(F.input_file_name(), _PATH_DATE_REGEX, 1),
    )


def _apply_watermark_filter(spark: SparkSession, df: DataFrame, *, full_refresh: bool) -> DataFrame:
    """Filter ``df`` so only rows from dates >= the Bronze high-watermark survive.

    ``>=`` (not ``>``) so the watermark date is re-processed every run —
    that's what catches same-day updates to a staging file. Iceberg's
    ``overwritePartitions`` makes this atomic per date.

    Returns the unfiltered DataFrame on ``full_refresh=True`` or when the
    Bronze table has not been created yet.
    """
    if full_refresh:
        logger.warning("--full-refresh: ignoring high-watermark, reprocessing all staging dates")
        return df

    hw = _get_high_watermark(spark, BRASILEIRAO_TABLE)
    if hw is None:
        logger.info("No Bronze high-watermark (table missing or empty) — processing all staging dates")
        return df

    logger.info("Bronze high-watermark=%s — keeping rows with ingestion_date >= %s", hw, hw)
    return df.filter(F.col("ingestion_date") >= hw)


def _get_high_watermark(spark: SparkSession, table: str) -> str | None:
    """Return ``MAX(ingestion_date)`` from ``table``, or None if absent.

    Iceberg answers this from manifest-level min/max metadata in the
    typical case — no data scan, no cost worth optimizing.
    """
    if not spark.catalog.tableExists(table):
        return None
    row = spark.sql(f"SELECT MAX(ingestion_date) AS hw FROM {table}").first()
    return row.hw if row and row.hw else None


if __name__ == "__main__":
    import argparse

    from src.utils.logging_config import setup_logging

    setup_logging()

    parser = argparse.ArgumentParser(description="Staging -> Bronze ingestion for Brasileirao")
    parser.add_argument(
        "execution_date",
        help="YYYY-MM-DD (informational only — partitioning is path-derived)",
    )
    parser.add_argument(
        "--nessie-ref",
        default="main",
        help="Nessie branch / tag / hash to bind the catalog to (default: main)",
    )
    parser.add_argument(
        "--full-refresh",
        action="store_true",
        help="Ignore the Bronze high-watermark and reprocess every staging date "
        "(opt-in escape hatch for retroactive amendments)",
    )
    args = parser.parse_args(sys.argv[1:])

    ingest(args.execution_date, nessie_ref=args.nessie_ref, full_refresh=args.full_refresh)
