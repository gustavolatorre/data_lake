"""Silver layer — transform Bronze Brasileirao into a clean, enriched Silver table.

Differences vs the breweries Silver (intentional):

* **No soft-delete logic** (no ``WHEN NOT MATCHED BY SOURCE``). Jogos de
  futebol não desaparecem da fonte: uma partida histórica permanece
  registrada para sempre. The MERGE is just UPSERT (update mutable fields
  on hit, insert on miss).
* **No shrink guard**. The staging side is incremental (one date at a
  time), not a full catalogue snapshot, so a smaller batch isn't anomalous.
* **Path-driven incremental read** via a Bronze-side high-watermark on
  ``ingested_at`` — captures inserts AND updates from the Bronze
  ``overwritePartitions`` flow. Idempotent through the MERGE.
* **Enrichment**: derives ``stadium_state`` (UF) and ``stadium_state_origin``
  via the inline dicts in :mod:`src.silver.stadium_enrichment`, joined via
  broadcast (~130 rows total). Cascade: stadium → home_team → UNKNOWN.
"""

import logging
import sys

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from src.silver.stadium_enrichment import (
    ORIGIN_HOME_TEAM,
    ORIGIN_STADIUM,
    ORIGIN_UNKNOWN,
    SENTINEL_STATE,
    build_lookup_frames,
)
from src.utils.data_quality import log_quality_summary
from src.utils.quality_runner import run_quality_checks
from src.utils.spark_session import create_spark_session

logger = logging.getLogger(__name__)

BRONZE_TABLE = "nessie.bronze.brasileirao"
SILVER_TABLE = "nessie.silver.brasileirao"
QUARANTINE_TABLE = "nessie.silver.brasileirao_quarantine"

REASON_NULL_ID = "NULL_GE_MATCH_ID"

# Accent normalization map (same as breweries — Portuguese coverage).
# Applied via F.translate() to home_team, away_team, stadium.
_ACCENTS = "áàâãäéèêëíìîïóòôõöúùûüçÁÀÂÃÄÉÈÊËÍÌÎÏÓÒÔÕÖÚÙÛÜÇ"
_PLAIN = "aaaaaeeeeiiiiooooouuuucAAAAAEEEEIIIIOOOOOUUUUC"


def transform(execution_date: str, nessie_ref: str = "main", *, full_refresh: bool = False) -> None:
    """Execute the Bronze → Silver transformation.

    Args:
        execution_date: Informational date string (YYYY-MM-DD) from the
            Airflow DAG. Partitioning is path/date-driven; this is logged
            for context only.
        nessie_ref: Nessie branch the Spark session should bind to (the
            isolated ``etl_*`` branch created by ``create_branch``).
        full_refresh: Skip the high-watermark and re-process the entire
            Bronze table. Manual escape hatch — never used by the DAG.
    """
    spark = create_spark_session("BrasileiraoBronzeToSilver", nessie_ref=nessie_ref)
    try:
        _run_transform(spark, execution_date, full_refresh=full_refresh)
    finally:
        spark.stop()
        logger.info("SparkSession stopped")


def _run_transform(spark: SparkSession, execution_date: str, *, full_refresh: bool = False) -> None:
    logger.info("Bronze → Silver transform start (execution_date=%s, full_refresh=%s)", execution_date, full_refresh)

    source_df = _read_bronze_incremental(spark, full_refresh=full_refresh)
    if source_df is None or source_df.isEmpty():
        logger.info("No Bronze rows beyond Silver watermark — nothing to do")
        return

    log_quality_summary(source_df, "bronze→silver source", critical_columns=["ge_match_id", "date", "stadium"])

    # 1. Quarantine NULL ge_match_id rows (MERGE primary key)
    bad_df = source_df.filter(F.col("ge_match_id").isNull())
    good_df = source_df.filter(F.col("ge_match_id").isNotNull())
    bad_count = bad_df.count()
    if bad_count > 0:
        logger.warning("Quarantining %d row(s) with NULL ge_match_id", bad_count)
        _quarantine_invalid_records(spark, bad_df, REASON_NULL_ID, execution_date)
    else:
        logger.info("No NULL-id rows to quarantine")

    if good_df.isEmpty():
        logger.warning("All rows were quarantined; nothing to MERGE")
        return

    # 2. Dedup by ge_match_id within the source delta (defensive — Bronze
    #    can theoretically have multiple ingestions for the same match
    #    if a staging file is re-uploaded with corrections).
    dedup_window = Window.partitionBy("ge_match_id").orderBy(F.col("ingested_at").desc())
    deduped_df = good_df.withColumn("_rn", F.row_number().over(dedup_window)).filter(F.col("_rn") == 1).drop("_rn")

    # 3. Native Spark transformations (zero Python UDFs)
    transformed_df = _apply_native_transformations(deduped_df)

    # 4. Enrichment via broadcast joins
    transformed_df = _enrich_with_stadium_state(spark, transformed_df)

    transformed_df.cache()
    try:
        log_quality_summary(
            transformed_df,
            "silver",
            critical_columns=["ge_match_id", "match_date", "stadium_state"],
        )

        transformed_df.createOrReplaceTempView("v_transformed_brasileirao")
        _execute_merge(spark)

        # 5. Post-MERGE declarative quality contract. A FAIL-severity
        #    violation raises QualityCheckError, the SparkSubmit task
        #    fails, and `cleanup_branch` in the DAG drops the isolated
        #    Nessie branch so `main` never sees the corrupted state.
        silver_df = spark.table(SILVER_TABLE)
        run_quality_checks(silver_df, checks_file="silver_brasileirao.yml")
    finally:
        transformed_df.unpersist()


def _read_bronze_incremental(spark: SparkSession, *, full_refresh: bool) -> DataFrame | None:
    """Return the Bronze rows to process, filtered by the Silver high-watermark.

    The watermark is ``MAX(updated_at)`` on the Silver table — that field
    is bumped on every successful MERGE write, so filtering Bronze rows
    with ``ingested_at >= HW`` captures any Bronze row that landed since
    the last Silver run, including updates to a previously-seen match
    (e.g. placar corrigido).

    Returns ``None`` when the Bronze table does not exist yet.
    """
    if not spark.catalog.tableExists(BRONZE_TABLE):
        logger.info("Bronze table %s does not exist yet", BRONZE_TABLE)
        return None

    bronze = spark.table(BRONZE_TABLE)

    if full_refresh:
        logger.warning("--full-refresh: ignoring Silver watermark, reading full Bronze table")
        return bronze

    hw = _get_silver_watermark(spark)
    if hw is None:
        logger.info("No Silver watermark (Silver table missing or empty) — reading full Bronze")
        return bronze

    logger.info("Silver watermark=%s — filtering Bronze rows with ingested_at >= %s", hw, hw)
    return bronze.filter(F.col("ingested_at") >= F.lit(hw))


def _get_silver_watermark(spark: SparkSession) -> str | None:
    """Return ``MAX(updated_at)`` from Silver, or None if absent/empty."""
    if not spark.catalog.tableExists(SILVER_TABLE):
        return None
    row = spark.sql(f"SELECT MAX(updated_at) AS hw FROM {SILVER_TABLE}").first()
    return row.hw if row and row.hw else None


def _apply_native_transformations(df: DataFrame) -> DataFrame:
    """Cast types, derive analytical columns, normalize text — no UDFs."""
    return (
        df
        # Strip accents on string columns used by lookups / display.
        .withColumn("home_team", F.translate(F.col("home_team"), _ACCENTS, _PLAIN))
        .withColumn("away_team", F.translate(F.col("away_team"), _ACCENTS, _PLAIN))
        .withColumn("stadium", F.translate(F.col("stadium"), _ACCENTS, _PLAIN))
        # Real date type — Bronze stores it as YYYY-MM-DD string.
        .withColumn("match_date", F.to_date(F.col("date"), "yyyy-MM-dd"))
        # Compose kickoff timestamp. kickoff_time is "HH:mm" from GE; some
        # rows can be empty, in which case the cast returns NULL.
        .withColumn(
            "kickoff_ts",
            F.to_timestamp(F.concat_ws(" ", F.col("date"), F.col("kickoff_time")), "yyyy-MM-dd HH:mm"),
        )
        # Derived analytical columns.
        .withColumn("total_goals", F.col("score_home") + F.col("score_away"))
        .withColumn(
            "match_outcome",
            F.when(F.col("score_home") > F.col("score_away"), F.lit("HOME_WIN"))
            .when(F.col("score_home") < F.col("score_away"), F.lit("AWAY_WIN"))
            .otherwise(F.lit("DRAW")),
        )
    )


def _enrich_with_stadium_state(spark: SparkSession, df: DataFrame) -> DataFrame:
    """Add ``stadium_state`` + ``stadium_state_origin`` via broadcast joins.

    Cascade:
        1. stadium found in STADIUM_TO_STATE       → STADIUM_LOOKUP
        2. home_team found in HOME_TEAM_TO_STATE   → HOME_TEAM_FALLBACK
        3. neither matched                          → UNKNOWN (sentinel)
    """
    stadiums, teams = build_lookup_frames(spark)

    enriched = (
        df.join(F.broadcast(stadiums), df["stadium"] == stadiums["_lookup_stadium"], "left")
        .join(F.broadcast(teams), df["home_team"] == teams["_lookup_home_team"], "left")
        .withColumn(
            "stadium_state",
            F.coalesce(
                F.col("_lookup_stadium_state"),
                F.col("_lookup_home_team_state"),
                F.lit(SENTINEL_STATE),
            ),
        )
        .withColumn(
            "stadium_state_origin",
            F.when(F.col("_lookup_stadium_state").isNotNull(), F.lit(ORIGIN_STADIUM))
            .when(F.col("_lookup_home_team_state").isNotNull(), F.lit(ORIGIN_HOME_TEAM))
            .otherwise(F.lit(ORIGIN_UNKNOWN)),
        )
        .drop("_lookup_stadium", "_lookup_stadium_state", "_lookup_home_team", "_lookup_home_team_state")
    )

    # Visibility: log the coverage breakdown so an operator notices when
    # the UNKNOWN bucket starts growing (signal that the dicts need an
    # update).
    coverage = enriched.groupBy("stadium_state_origin").count().collect()
    for r in coverage:
        logger.info("Enrichment coverage: %s → %d row(s)", r["stadium_state_origin"], r["count"])

    return enriched


def _quarantine_invalid_records(spark: SparkSession, bad_df: DataFrame, reason: str, execution_date: str) -> None:
    """Append rows that failed Silver row-level rules to the quarantine table.

    Append-only, partitioned by quarantine_date — duplicate quarantine rows
    are preferred over silently dropping incident evidence.
    """
    enriched = (
        bad_df.select(
            "ge_match_id",
            "matchweek",
            "home_team",
            "away_team",
            "date",
            "stadium",
            "ingestion_date",
            "ingested_at",
        )
        .withColumn("quarantine_reason", F.lit(reason))
        .withColumn("quarantined_at", F.current_timestamp())
        .withColumn("quarantine_date", F.lit(execution_date))
    )

    spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.silver")

    if not spark.catalog.tableExists(QUARANTINE_TABLE):
        logger.info("Creating quarantine table %s", QUARANTINE_TABLE)
        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {QUARANTINE_TABLE} (
                ge_match_id STRING,
                matchweek INT,
                home_team STRING,
                away_team STRING,
                date STRING,
                stadium STRING,
                ingestion_date STRING,
                ingested_at TIMESTAMP,
                quarantine_reason STRING,
                quarantined_at TIMESTAMP,
                quarantine_date STRING
            )
            USING iceberg
            PARTITIONED BY (quarantine_date)
            TBLPROPERTIES ('format-version'='2', 'gc.enabled'='true')
        """)

    enriched.writeTo(QUARANTINE_TABLE).append()
    logger.info("Appended %d row(s) to %s (reason=%s)", enriched.count(), QUARANTINE_TABLE, reason)


def _execute_merge(spark: SparkSession) -> None:
    """Execute the upsert MERGE into the Silver table.

    No ``WHEN NOT MATCHED BY SOURCE`` clause — a partida histórica não
    deixa de existir entre runs. Only mutable fields (score, broadcast,
    stadium info from re-extraction) get updated; immutable identifiers
    (matchweek, date, teams) stay locked to the original insert.
    """
    spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.silver")

    if not spark.catalog.tableExists(SILVER_TABLE):
        logger.info("Creating Silver table %s for the first time", SILVER_TABLE)
        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {SILVER_TABLE} (
                ge_match_id STRING,
                matchweek INT,
                match_date DATE,
                kickoff_ts TIMESTAMP,
                home_team STRING,
                home_team_code STRING,
                away_team STRING,
                away_team_code STRING,
                score_home INT,
                score_away INT,
                total_goals INT,
                match_outcome STRING,
                stadium STRING,
                stadium_state STRING,
                stadium_state_origin STRING,
                is_active BOOLEAN,
                updated_at TIMESTAMP,
                ingestion_date STRING
            )
            USING iceberg
            PARTITIONED BY (months(match_date))
            TBLPROPERTIES ('format-version'='2', 'gc.enabled'='true')
        """)

    logger.info("Executing MERGE INTO %s", SILVER_TABLE)
    spark.sql(f"""
        MERGE INTO {SILVER_TABLE} t
        USING v_transformed_brasileirao s
        ON t.ge_match_id = s.ge_match_id
        WHEN MATCHED THEN UPDATE SET
            t.score_home = s.score_home,
            t.score_away = s.score_away,
            t.total_goals = s.total_goals,
            t.match_outcome = s.match_outcome,
            t.stadium = s.stadium,
            t.stadium_state = s.stadium_state,
            t.stadium_state_origin = s.stadium_state_origin,
            t.is_active = true,
            t.updated_at = current_timestamp()
        WHEN NOT MATCHED THEN INSERT (
            ge_match_id, matchweek, match_date, kickoff_ts,
            home_team, home_team_code, away_team, away_team_code,
            score_home, score_away, total_goals, match_outcome,
            stadium, stadium_state, stadium_state_origin,
            is_active, updated_at, ingestion_date
        ) VALUES (
            s.ge_match_id, s.matchweek, s.match_date, s.kickoff_ts,
            s.home_team, s.home_team_code, s.away_team, s.away_team_code,
            s.score_home, s.score_away, s.total_goals, s.match_outcome,
            s.stadium, s.stadium_state, s.stadium_state_origin,
            true, current_timestamp(), s.ingestion_date
        )
    """)
    logger.info("Silver MERGE complete")


if __name__ == "__main__":
    import argparse

    from src.utils.logging_config import setup_logging

    setup_logging()

    parser = argparse.ArgumentParser(description="Bronze -> Silver transformation for Brasileirao")
    parser.add_argument(
        "execution_date",
        help="YYYY-MM-DD (informational only — incremental driven by Silver watermark)",
    )
    parser.add_argument(
        "--nessie-ref",
        default="main",
        help="Nessie branch / tag / hash to bind the catalog to (default: main)",
    )
    parser.add_argument(
        "--full-refresh",
        action="store_true",
        help="Ignore the Silver watermark and re-process the entire Bronze table "
        "(opt-in escape hatch for retroactive amendments)",
    )
    args = parser.parse_args(sys.argv[1:])

    transform(args.execution_date, nessie_ref=args.nessie_ref, full_refresh=args.full_refresh)
