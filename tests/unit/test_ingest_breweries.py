"""Unit tests for ``src.bronze.ingest_breweries``.

We avoid spinning up a real Iceberg-enabled Spark session (heavy classpath,
JARs, Nessie). Instead we mock the SparkSession that ``_run_ingest`` receives
plus the ``pyspark.sql.functions`` shim, then verify the call shape:
namespace creation, partitioning, idempotent writer choice
(overwritePartitions vs create), and quality gates.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from src.bronze import ingest_breweries
from src.bronze.ingest_breweries import BREWERY_SCHEMA, _run_ingest, ingest


@pytest.fixture
def patched_functions():
    """Patch ``F`` so F.lit / F.col / F.current_timestamp don't need a SparkContext."""
    with patch("src.bronze.ingest_breweries.F") as mock_f:
        mock_f.lit.return_value = MagicMock(name="lit_col")
        mock_f.col.return_value = MagicMock(name="col_col")
        mock_f.current_timestamp.return_value = MagicMock(name="ts_col")
        yield mock_f


def _make_mock_spark(table_exists: bool, row_count: int = 5):
    """Build a SparkSession mock wired for ``_run_ingest``.

    Args:
        table_exists: Drives ``spark.catalog.tableExists`` so the test can
            exercise the create vs overwritePartitions branch.
        row_count: What ``df.count()`` should return (used by both the row-count
            guard and the final log line).
    """
    spark = MagicMock(name="SparkSession")
    spark.catalog.tableExists.return_value = table_exists

    df = MagicMock(name="DataFrame")
    df.count.return_value = row_count
    df.columns = ["id", "name", "brewery_type"]
    # Chained ``df = df.withColumn(...).withColumn(...)`` — each chained call
    # returns the same mock so the final binding behaves like the original df.
    df.withColumn.return_value = df

    spark.read.schema.return_value.option.return_value.json.return_value = df

    writer = MagicMock(name="DataFrameWriter")
    writer.tableProperty.return_value = writer
    writer.partitionedBy.return_value = writer
    df.writeTo.return_value = writer

    return spark, df, writer


class TestBrewerySchema:
    def test_schema_has_expected_columns(self):
        names = {field.name for field in BREWERY_SCHEMA.fields}
        assert {"id", "name", "brewery_type", "state", "country", "longitude", "latitude"} <= names


class TestRunIngest:
    @patch("src.bronze.ingest_breweries.log_quality_summary")
    @patch("src.bronze.ingest_breweries.check_row_count")
    def test_overwrites_partition_when_table_exists(self, _check, _summary, patched_functions):
        spark, _df, writer = _make_mock_spark(table_exists=True, row_count=10)

        _run_ingest(spark, "2026-04-29")

        writer.overwritePartitions.assert_called_once()
        writer.create.assert_not_called()

    @patch("src.bronze.ingest_breweries.log_quality_summary")
    @patch("src.bronze.ingest_breweries.check_row_count")
    def test_creates_table_on_first_run(self, _check, _summary, patched_functions):
        spark, _df, writer = _make_mock_spark(table_exists=False, row_count=10)

        _run_ingest(spark, "2026-04-29")

        writer.create.assert_called_once()
        writer.overwritePartitions.assert_not_called()

    @patch("src.bronze.ingest_breweries.log_quality_summary")
    @patch("src.bronze.ingest_breweries.check_row_count")
    def test_creates_namespace_idempotently(self, _check, _summary, patched_functions):
        spark, _df, _writer = _make_mock_spark(table_exists=True)

        _run_ingest(spark, "2026-04-29")

        spark.sql.assert_any_call("CREATE NAMESPACE IF NOT EXISTS nessie.bronze")

    @patch("src.bronze.ingest_breweries.log_quality_summary")
    @patch("src.bronze.ingest_breweries.check_row_count")
    def test_uses_format_version_2(self, _check, _summary, patched_functions):
        spark, _df, writer = _make_mock_spark(table_exists=True)

        _run_ingest(spark, "2026-04-29")

        writer.tableProperty.assert_any_call("format-version", "2")

    @patch("src.bronze.ingest_breweries.log_quality_summary")
    @patch("src.bronze.ingest_breweries.check_row_count")
    def test_caches_and_unpersists(self, _check, _summary, patched_functions):
        spark, df, _writer = _make_mock_spark(table_exists=True)

        _run_ingest(spark, "2026-04-29")

        df.cache.assert_called_once()
        df.unpersist.assert_called_once()

    @patch("src.bronze.ingest_breweries.log_quality_summary")
    @patch("src.bronze.ingest_breweries.check_row_count", side_effect=ValueError("0 rows"))
    def test_empty_source_unpersists_and_raises(self, _check, _summary, patched_functions):
        """Even when the quality gate fires, cache must be released."""
        spark, df, writer = _make_mock_spark(table_exists=True)

        with pytest.raises(ValueError, match="0 rows"):
            _run_ingest(spark, "2026-04-29")

        df.unpersist.assert_called_once()
        writer.overwritePartitions.assert_not_called()
        writer.create.assert_not_called()


class TestIngestEntrypoint:
    @patch("src.bronze.ingest_breweries._run_ingest")
    @patch("src.bronze.ingest_breweries.create_spark_session")
    def test_stops_spark_even_on_failure(self, mock_create, mock_run):
        mock_spark = MagicMock()
        mock_create.return_value = mock_spark
        mock_run.side_effect = RuntimeError("boom")

        with pytest.raises(RuntimeError, match="boom"):
            ingest("2026-04-29")

        mock_spark.stop.assert_called_once()

    @patch("src.bronze.ingest_breweries._run_ingest")
    @patch("src.bronze.ingest_breweries.create_spark_session")
    def test_happy_path_stops_spark(self, mock_create, mock_run):
        mock_spark = MagicMock()
        mock_create.return_value = mock_spark

        ingest("2026-04-29")

        mock_run.assert_called_once_with(mock_spark, "2026-04-29")
        mock_spark.stop.assert_called_once()


class TestModuleConstants:
    def test_module_logger_named_after_module(self):
        assert ingest_breweries.logger.name == "src.bronze.ingest_breweries"
