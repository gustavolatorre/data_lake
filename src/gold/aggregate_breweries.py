"""Gold layer — aggregate brewery counts by type and location.

Reads the Silver Iceberg table from the Nessie catalog, performs a GroupBy
aggregation on ``brewery_type`` and ``state``, and writes the result as an
Iceberg table to the Gold namespace.
"""

import logging
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from src.utils.data_quality import check_row_count, log_quality_summary
from src.utils.spark_session import create_spark_session

logger = logging.getLogger(__name__)


def aggregate() -> None:
    """Execute the Silver → Gold aggregation.

    Reads the Silver Iceberg table, aggregates brewery counts by type and
    state, and writes the result to the Gold Iceberg table.
    """
    spark = create_spark_session("AggregateBreweries")

    try:
        _run_aggregation(spark)
    finally:
        spark.stop()
        logger.info("SparkSession stopped")


def _run_aggregation(spark: SparkSession) -> None:
    """Internal aggregation logic.

    Args:
        spark: Active SparkSession.
    """
    logger.info("Reading Silver Iceberg table: nessie.silver.breweries")

    df = spark.read.table("nessie.silver.breweries")

    check_row_count(df, min_rows=1)
    log_quality_summary(df, "silver_input", ["brewery_type", "state"])

    # Aggregate: count breweries by type and state
    df_agg = (
        df.groupBy("brewery_type", "state")
        .agg(F.count("*").alias("quantity"))
        .orderBy("state", "brewery_type")
    )

    agg_count = df_agg.count()
    logger.info("Aggregation complete: %d unique (type, state) combinations", agg_count)

    # Write to Gold Iceberg table
    _write_iceberg(spark, df_agg)


def _write_iceberg(spark: SparkSession, df) -> None:
    """Write aggregated DataFrame as an Iceberg table in Nessie catalog.

    Uses ``createOrReplace`` for full overwrite semantics — each run
    produces a complete, fresh aggregation.

    Args:
        spark: Active SparkSession.
        df: Aggregated DataFrame to write.
    """
    logger.info("Writing Gold Iceberg table to Nessie catalog")

    spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.gold")

    df.writeTo("nessie.gold.breweries_aggregated").tableProperty(
        "format-version", "2"
    ).createOrReplace()

    logger.info("Gold Iceberg table written successfully: nessie.gold.breweries_aggregated")


if __name__ == "__main__":
    logging.basicConfig(stream=sys.stdout, level=logging.INFO)
    aggregate()
