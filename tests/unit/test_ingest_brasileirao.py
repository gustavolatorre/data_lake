"""Unit tests for ``src.bronze.ingest_brasileirao``.

Mirrors the strategy in ``test_ingest_breweries``: mock the SparkSession
and the ``pyspark.sql.functions`` shim so the Iceberg runtime / Nessie
classpath isn't required.
"""

from unittest.mock import MagicMock, patch

import pytest

from src.bronze import ingest_brasileirao
from src.bronze.ingest_brasileirao import (
    BRASILEIRAO_SCHEMA,
    BRASILEIRAO_TABLE,
    _apply_watermark_filter,
    _get_high_watermark,
    _read_staging_with_ingestion_date,
    _run_ingest,
    ingest,
)


@pytest.fixture
def patched_functions():
    """Patch ``F`` so calls don't need a SparkContext.

    The Column mock returned by ``F.col`` is configured to support the
    comparison operators the production code uses (``>=`` in the
    watermark filter), because plain MagicMock raises TypeError on those.
    """
    with patch("src.bronze.ingest_brasileirao.F") as mock_f:
        col_mock = MagicMock(name="col_col")
        col_mock.__ge__ = MagicMock(return_value=MagicMock(name="col_ge"))
        col_mock.__gt__ = MagicMock(return_value=MagicMock(name="col_gt"))
        mock_f.lit.return_value = MagicMock(name="lit_col")
        mock_f.col.return_value = col_mock
        mock_f.current_timestamp.return_value = MagicMock(name="ts_col")
        mock_f.to_timestamp.return_value = MagicMock(name="to_ts_col")
        mock_f.input_file_name.return_value = MagicMock(name="input_file_col")
        mock_f.regexp_extract.return_value = MagicMock(name="regex_extract_col")
        with patch("src.bronze.ingest_brasileirao.days") as mock_days:
            mock_days.return_value = MagicMock(name="days_transform_col")
            mock_f._days = mock_days  # type: ignore[attr-defined]
            yield mock_f


@pytest.fixture(autouse=True)
def _no_op_quality_runner():
    """Stub the YAML quality runner — the rule file isn't on the test box."""
    with patch("src.bronze.ingest_brasileirao.run_quality_checks") as stub:
        yield stub


def _make_mock_df(*, empty: bool = False, row_count: int = 5):
    """Build a DataFrame mock that survives the chained transformations
    in ``_run_ingest`` (.withColumn × N, .filter, .cache, .groupBy, .writeTo).
    """
    df = MagicMock(name="DataFrame")
    df.withColumn.return_value = df
    df.filter.return_value = df
    df.rdd.isEmpty.return_value = empty
    df.count.return_value = row_count

    # df.groupBy("ingestion_date").count().orderBy().collect()
    grouped = MagicMock()
    grouped.count.return_value.orderBy.return_value.collect.return_value = (
        []
        if empty
        else [
            MagicMock(ingestion_date="2026-05-25", **{"__getitem__": lambda _self, _k: 5}),
        ]
    )
    df.groupBy.return_value = grouped

    writer = MagicMock(name="DataFrameWriter")
    writer.tableProperty.return_value = writer
    writer.partitionedBy.return_value = writer
    df.writeTo.return_value = writer

    return df, writer


def _make_mock_spark(*, table_exists: bool, df: MagicMock):
    spark = MagicMock(name="SparkSession")
    spark.catalog.tableExists.return_value = table_exists
    spark.read.schema.return_value.option.return_value.json.return_value = df
    # _get_high_watermark via spark.sql
    spark.sql.return_value.first.return_value = MagicMock(hw=None)
    return spark


# ──────────────────────────────────────────────────────────────────────────
# Schema contract
# ──────────────────────────────────────────────────────────────────────────


class TestBrasileiraoSchema:
    def test_schema_has_required_columns(self):
        names = {f.name for f in BRASILEIRAO_SCHEMA.fields}
        required = {
            "ge_match_id",
            "matchweek",
            "home_team",
            "away_team",
            "score_home",
            "score_away",
            "date",
            "stadium",
        }
        assert required <= names

    def test_ge_match_id_is_string_type(self):
        """Bug fix from #59: GE returns UUID-style IDs, not integers."""
        ge_id = next(f for f in BRASILEIRAO_SCHEMA.fields if f.name == "ge_match_id")
        assert ge_id.dataType.simpleString() == "string"


# ──────────────────────────────────────────────────────────────────────────
# _get_high_watermark
# ──────────────────────────────────────────────────────────────────────────


class TestGetHighWatermark:
    def test_returns_none_when_table_missing(self):
        spark = MagicMock()
        spark.catalog.tableExists.return_value = False
        assert _get_high_watermark(spark, BRASILEIRAO_TABLE) is None
        spark.sql.assert_not_called()

    def test_returns_none_when_table_empty(self):
        spark = MagicMock()
        spark.catalog.tableExists.return_value = True
        spark.sql.return_value.first.return_value = MagicMock(hw=None)
        assert _get_high_watermark(spark, BRASILEIRAO_TABLE) is None

    def test_returns_max_when_table_populated(self):
        spark = MagicMock()
        spark.catalog.tableExists.return_value = True
        spark.sql.return_value.first.return_value = MagicMock(hw="2026-05-25")
        assert _get_high_watermark(spark, BRASILEIRAO_TABLE) == "2026-05-25"


# ──────────────────────────────────────────────────────────────────────────
# _apply_watermark_filter
# ──────────────────────────────────────────────────────────────────────────


class TestApplyWatermarkFilter:
    def test_full_refresh_returns_unfiltered(self, patched_functions):
        spark = MagicMock()
        df = MagicMock()
        out = _apply_watermark_filter(spark, df, full_refresh=True)
        assert out is df
        df.filter.assert_not_called()

    @patch("src.bronze.ingest_brasileirao._get_high_watermark", return_value=None)
    def test_no_watermark_returns_unfiltered(self, _hw, patched_functions):
        spark = MagicMock()
        df = MagicMock()
        out = _apply_watermark_filter(spark, df, full_refresh=False)
        assert out is df
        df.filter.assert_not_called()

    @patch("src.bronze.ingest_brasileirao._get_high_watermark", return_value="2026-05-25")
    def test_with_watermark_applies_filter(self, _hw, patched_functions):
        spark = MagicMock()
        df = MagicMock()
        filtered = MagicMock()
        df.filter.return_value = filtered

        out = _apply_watermark_filter(spark, df, full_refresh=False)

        assert out is filtered
        df.filter.assert_called_once()


# ──────────────────────────────────────────────────────────────────────────
# _read_staging_with_ingestion_date
# ──────────────────────────────────────────────────────────────────────────


class TestReadStaging:
    def test_returns_none_on_path_does_not_exist(self, patched_functions):
        spark = MagicMock()
        spark.read.schema.return_value.option.return_value.json.side_effect = RuntimeError(
            "Path does not exist: s3a://staging/brasileirao"
        )
        out = _read_staging_with_ingestion_date(spark, "s3a://staging/brasileirao/*/matches.json")
        assert out is None

    def test_returns_none_on_unable_to_infer_schema(self, patched_functions):
        spark = MagicMock()
        spark.read.schema.return_value.option.return_value.json.side_effect = RuntimeError("Unable to infer schema")
        out = _read_staging_with_ingestion_date(spark, "s3a://staging/brasileirao/*/matches.json")
        assert out is None

    def test_reraises_unrelated_runtime_error(self, patched_functions):
        spark = MagicMock()
        spark.read.schema.return_value.option.return_value.json.side_effect = RuntimeError("JVM gateway crashed")
        with pytest.raises(RuntimeError, match="JVM gateway"):
            _read_staging_with_ingestion_date(spark, "s3a://staging/brasileirao/*/matches.json")

    def test_adds_ingestion_date_column(self, patched_functions):
        spark = MagicMock()
        raw_df = MagicMock()
        spark.read.schema.return_value.option.return_value.json.return_value = raw_df

        out = _read_staging_with_ingestion_date(spark, "s3a://staging/brasileirao/*/matches.json")

        # withColumn must have been called with "ingestion_date"
        raw_df.withColumn.assert_called_once()
        call_args = raw_df.withColumn.call_args
        assert call_args.args[0] == "ingestion_date"
        # Returned the DataFrame with the new column
        assert out is raw_df.withColumn.return_value


# ──────────────────────────────────────────────────────────────────────────
# _run_ingest — orchestration
# ──────────────────────────────────────────────────────────────────────────


class TestRunIngest:
    @patch("src.bronze.ingest_brasileirao._read_staging_with_ingestion_date", return_value=None)
    def test_short_circuits_when_glob_empty(self, _read, patched_functions):
        spark = MagicMock()
        _run_ingest(spark, "2026-05-25")
        # Should not attempt namespace / table creation.
        spark.sql.assert_not_called()

    @patch("src.bronze.ingest_brasileirao.check_row_count")
    @patch("src.bronze.ingest_brasileirao.log_quality_summary")
    @patch("src.bronze.ingest_brasileirao._apply_watermark_filter")
    @patch("src.bronze.ingest_brasileirao._read_staging_with_ingestion_date")
    def test_short_circuits_when_filtered_empty(self, mock_read, mock_filter, _summary, _check, patched_functions):
        df_empty, _ = _make_mock_df(empty=True)
        mock_read.return_value = df_empty
        mock_filter.return_value = df_empty

        spark = MagicMock()
        _run_ingest(spark, "2026-05-25")

        # No CREATE NAMESPACE / write when filter result is empty.
        spark.sql.assert_not_called()

    @patch("src.bronze.ingest_brasileirao.check_row_count")
    @patch("src.bronze.ingest_brasileirao.log_quality_summary")
    @patch("src.bronze.ingest_brasileirao._apply_watermark_filter")
    @patch("src.bronze.ingest_brasileirao._read_staging_with_ingestion_date")
    def test_creates_table_on_first_run(self, mock_read, mock_filter, _summary, _check, patched_functions):
        df, writer = _make_mock_df(empty=False)
        mock_read.return_value = df
        mock_filter.return_value = df
        spark = _make_mock_spark(table_exists=False, df=df)

        _run_ingest(spark, "2026-05-25")

        writer.create.assert_called_once()
        writer.overwritePartitions.assert_not_called()

    @patch("src.bronze.ingest_brasileirao.check_row_count")
    @patch("src.bronze.ingest_brasileirao.log_quality_summary")
    @patch("src.bronze.ingest_brasileirao._apply_watermark_filter")
    @patch("src.bronze.ingest_brasileirao._read_staging_with_ingestion_date")
    def test_overwrites_partitions_when_table_exists(self, mock_read, mock_filter, _summary, _check, patched_functions):
        df, writer = _make_mock_df(empty=False)
        mock_read.return_value = df
        mock_filter.return_value = df
        spark = _make_mock_spark(table_exists=True, df=df)

        _run_ingest(spark, "2026-05-25")

        writer.overwritePartitions.assert_called_once()
        writer.create.assert_not_called()

    @patch("src.bronze.ingest_brasileirao.check_row_count")
    @patch("src.bronze.ingest_brasileirao.log_quality_summary")
    @patch("src.bronze.ingest_brasileirao._apply_watermark_filter")
    @patch("src.bronze.ingest_brasileirao._read_staging_with_ingestion_date")
    def test_uses_format_version_2(self, mock_read, mock_filter, _summary, _check, patched_functions):
        df, writer = _make_mock_df(empty=False)
        mock_read.return_value = df
        mock_filter.return_value = df
        spark = _make_mock_spark(table_exists=True, df=df)

        _run_ingest(spark, "2026-05-25")

        writer.tableProperty.assert_any_call("format-version", "2")
        writer.tableProperty.assert_any_call("gc.enabled", "true")

    @patch("src.bronze.ingest_brasileirao.check_row_count")
    @patch("src.bronze.ingest_brasileirao.log_quality_summary")
    @patch("src.bronze.ingest_brasileirao._apply_watermark_filter")
    @patch("src.bronze.ingest_brasileirao._read_staging_with_ingestion_date")
    def test_creates_namespace_idempotently(self, mock_read, mock_filter, _summary, _check, patched_functions):
        df, _writer = _make_mock_df(empty=False)
        mock_read.return_value = df
        mock_filter.return_value = df
        spark = _make_mock_spark(table_exists=True, df=df)

        _run_ingest(spark, "2026-05-25")

        spark.sql.assert_any_call("CREATE NAMESPACE IF NOT EXISTS nessie.bronze")

    @patch("src.bronze.ingest_brasileirao.check_row_count")
    @patch("src.bronze.ingest_brasileirao.log_quality_summary")
    @patch("src.bronze.ingest_brasileirao._apply_watermark_filter")
    @patch("src.bronze.ingest_brasileirao._read_staging_with_ingestion_date")
    def test_caches_and_unpersists(self, mock_read, mock_filter, _summary, _check, patched_functions):
        df, _writer = _make_mock_df(empty=False)
        mock_read.return_value = df
        mock_filter.return_value = df
        spark = _make_mock_spark(table_exists=True, df=df)

        _run_ingest(spark, "2026-05-25")

        df.cache.assert_called_once()
        df.unpersist.assert_called_once()


# ──────────────────────────────────────────────────────────────────────────
# ingest entrypoint
# ──────────────────────────────────────────────────────────────────────────


class TestIngestEntrypoint:
    @patch("src.bronze.ingest_brasileirao._run_ingest")
    @patch("src.bronze.ingest_brasileirao.create_spark_session")
    def test_stops_spark_on_failure(self, mock_create, mock_run):
        mock_spark = MagicMock()
        mock_create.return_value = mock_spark
        mock_run.side_effect = RuntimeError("boom")

        with pytest.raises(RuntimeError, match="boom"):
            ingest("2026-05-25")

        mock_spark.stop.assert_called_once()

    @patch("src.bronze.ingest_brasileirao._run_ingest")
    @patch("src.bronze.ingest_brasileirao.create_spark_session")
    def test_happy_path_stops_spark(self, mock_create, mock_run):
        mock_spark = MagicMock()
        mock_create.return_value = mock_spark

        ingest("2026-05-25")

        mock_run.assert_called_once()
        mock_spark.stop.assert_called_once()

    @patch("src.bronze.ingest_brasileirao._run_ingest")
    @patch("src.bronze.ingest_brasileirao.create_spark_session")
    def test_full_refresh_passed_through(self, mock_create, mock_run):
        mock_create.return_value = MagicMock()
        ingest("2026-05-25", full_refresh=True)
        _, kwargs = mock_run.call_args
        assert kwargs.get("full_refresh") is True


class TestModuleConstants:
    def test_module_logger_named_after_module(self):
        assert ingest_brasileirao.logger.name == "src.bronze.ingest_brasileirao"

    def test_table_name(self):
        assert BRASILEIRAO_TABLE == "nessie.bronze.brasileirao"
