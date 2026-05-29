"""Unit tests for Silver layer — brewery transformations."""

from unittest.mock import MagicMock, patch

import pytest

from src.silver.transform_breweries import (
    MAX_SOURCE_SHRINK_RATIO,
    REASON_NULL_ID,
    SourceShrinkError,
    _apply_native_transformations,
    _assert_source_not_shrinking,
    _execute_merge,
    _quarantine_invalid_records,
    _run_transform,
    transform,
)


class TestNormalizeUnicode:
    """Tests for the native unicode normalization in Silver layer."""

    def test_removes_diacritics(self, spark):
        """Should remove accent marks from characters using Spark translate."""
        from pyspark.sql import Row

        # Test standard accented strings
        test_rows = [
            Row(id="1", state="Kärnten", ingested_at="2026-05-23 12:00:00"),
            Row(id="2", state="Niederösterreich", ingested_at="2026-05-23 12:00:00"),
            Row(id="3", state="São Paulo", ingested_at="2026-05-23 12:00:00"),
            Row(id="4", state="Zürich", ingested_at="2026-05-23 12:00:00"),
            Row(id="5", state="Päijät-Häme", ingested_at="2026-05-23 12:00:00"),
        ]
        df = spark.createDataFrame(test_rows)
        transformed_df = _apply_native_transformations(df)
        results = {row.id: row.state for row in transformed_df.collect()}

        assert results["1"] == "Karnten"
        assert results["2"] == "Niederosterreich"
        assert results["3"] == "Sao Paulo"
        assert results["4"] == "Zurich"
        assert results["5"] == "Paijat-Hame"

    def test_preserves_plain_ascii(self, spark):
        """Should not modify plain ASCII strings."""
        from pyspark.sql import Row

        test_rows = [
            Row(id="1", state="Oklahoma", ingested_at="2026-05-23 12:00:00"),
            Row(id="2", state="New York", ingested_at="2026-05-23 12:00:00"),
        ]
        df = spark.createDataFrame(test_rows)
        transformed_df = _apply_native_transformations(df)
        results = {row.id: row.state for row in transformed_df.collect()}

        assert results["1"] == "Oklahoma"
        assert results["2"] == "New York"

    def test_handles_none(self, spark):
        """Should replace None with sentinel value __UNKNOWN__."""
        from pyspark.sql.types import StringType, StructField, StructType

        schema = StructType(
            [
                StructField("id", StringType(), True),
                StructField("state", StringType(), True),
                StructField("ingested_at", StringType(), True),
            ]
        )
        test_rows = [("1", None, "2026-05-23 12:00:00")]
        df = spark.createDataFrame(test_rows, schema=schema)
        transformed_df = _apply_native_transformations(df)
        results = {row.id: row.state for row in transformed_df.collect()}

        assert results["1"] == "__UNKNOWN__"


class TestSilverTransformations:
    """Tests for Silver layer DataFrame transformations using PySpark."""

    def test_null_state_replacement(self, spark, sample_df):
        """Should replace null state values with __UNKNOWN__."""
        from pyspark.sql import functions as F

        df = sample_df.withColumn(
            "state",
            F.when(F.col("state").isNull(), F.lit("__UNKNOWN__")).otherwise(F.col("state")),
        )

        null_states = df.filter(F.col("state").isNull()).count()
        unknown_states = df.filter(F.col("state") == "__UNKNOWN__").count()

        assert null_states == 0
        assert unknown_states == 1  # id-003 has null state

    def test_deduplication_by_id(self, spark, sample_df):
        """Should keep only one record per id after dedup."""
        from pyspark.sql import functions as F
        from pyspark.sql.window import Window

        # Create duplicates
        df_with_dupes = sample_df.union(sample_df.filter(F.col("id") == "id-001"))

        initial_count = df_with_dupes.count()
        assert initial_count == 6  # 5 original + 1 duplicate

        # Deduplicate
        df_with_dupes = df_with_dupes.withColumn("ingestion_date", F.lit("2026-04-29"))
        window = Window.partitionBy("id").orderBy(F.col("ingestion_date").desc())
        df_deduped = (
            df_with_dupes.withColumn("_row_num", F.row_number().over(window))
            .filter(F.col("_row_num") == 1)
            .drop("_row_num")
        )

        assert df_deduped.count() == 5

    def test_ingestion_date_added(self, spark, sample_df):
        """Should add ingestion_date column."""
        from pyspark.sql import functions as F

        df = sample_df.withColumn("ingestion_date", F.lit("2026-04-29"))

        assert "ingestion_date" in df.columns
        dates = [row.ingestion_date for row in df.select("ingestion_date").collect()]
        assert all(d == "2026-04-29" for d in dates)

    def test_unicode_normalization_on_state(self, spark, sample_df):
        """Should normalize unicode characters in the state column."""
        from pyspark.sql import functions as F

        # _apply_native_transformations expects an ingested_at column to order window function
        df_with_ingested_at = sample_df.withColumn("ingested_at", F.current_timestamp())
        df = _apply_native_transformations(df_with_ingested_at)

        karnten_rows = df.filter(F.col("state") == "Karnten").count()
        assert karnten_rows == 1

    def test_schema_has_expected_columns(self, sample_df):
        """Should contain all 16 expected columns."""
        expected = {
            "id",
            "name",
            "brewery_type",
            "address_1",
            "address_2",
            "address_3",
            "city",
            "state_province",
            "postal_code",
            "country",
            "longitude",
            "latitude",
            "phone",
            "website_url",
            "state",
            "street",
        }
        assert set(sample_df.columns) == expected


class TestExecuteMerge:
    """Tests for the Silver MERGE INTO orchestration."""

    def test_creates_namespace(self):
        spark = MagicMock()
        spark.catalog.tableExists.return_value = True

        _execute_merge(spark)

        spark.sql.assert_any_call("CREATE NAMESPACE IF NOT EXISTS nessie.silver")

    def test_creates_table_on_first_run(self):
        spark = MagicMock()
        spark.catalog.tableExists.return_value = False

        _execute_merge(spark)

        ddl_calls = [args[0] for args, _ in spark.sql.call_args_list]
        create_table = next((c for c in ddl_calls if "CREATE TABLE" in c), None)
        assert create_table is not None, "first-run path must CREATE TABLE"
        assert "format-version" in create_table
        assert "'2'" in create_table

    def test_skips_create_when_table_exists(self):
        spark = MagicMock()
        spark.catalog.tableExists.return_value = True

        _execute_merge(spark)

        ddl_calls = [args[0] for args, _ in spark.sql.call_args_list]
        assert not any("CREATE TABLE" in c for c in ddl_calls), "should not CREATE TABLE when it already exists"

    def test_issues_merge_with_soft_delete_clauses(self):
        spark = MagicMock()
        spark.catalog.tableExists.return_value = True

        _execute_merge(spark)

        ddl_calls = [args[0] for args, _ in spark.sql.call_args_list]
        merge_sql = next((c for c in ddl_calls if "MERGE INTO" in c), None)
        assert merge_sql is not None, "MERGE must be issued"
        assert "WHEN MATCHED" in merge_sql
        assert "WHEN NOT MATCHED" in merge_sql
        assert "WHEN NOT MATCHED BY SOURCE" in merge_sql
        assert "is_active = false" in merge_sql
        assert "is_active = true" in merge_sql


class TestSourceShrinkGuard:
    """Tests for the relative shrink guard added in P1.17."""

    def _spark_with_active_count(self, active_count: int):
        spark = MagicMock()
        spark.catalog.tableExists.return_value = True
        result_row = MagicMock()
        result_row.__getitem__.side_effect = lambda key: active_count if key == "c" else None
        spark.sql.return_value.collect.return_value = [result_row]
        return spark

    def _df_with_count(self, count: int):
        df = MagicMock()
        df.count.return_value = count
        return df

    def test_aborts_when_drop_exceeds_threshold(self):
        spark = self._spark_with_active_count(active_count=1000)
        # 50% drop is well past the 20% threshold
        df = self._df_with_count(500)

        with pytest.raises(SourceShrinkError, match="dropped"):
            _assert_source_not_shrinking(spark, df)

    def test_passes_when_drop_below_threshold(self):
        spark = self._spark_with_active_count(active_count=1000)
        # 10% drop — below the 20% threshold
        df = self._df_with_count(900)

        _assert_source_not_shrinking(spark, df)  # must not raise

    def test_passes_when_source_grows(self):
        spark = self._spark_with_active_count(active_count=1000)
        df = self._df_with_count(1200)

        _assert_source_not_shrinking(spark, df)

    def test_skips_when_silver_table_missing(self):
        spark = MagicMock()
        spark.catalog.tableExists.return_value = False
        df = self._df_with_count(0)

        _assert_source_not_shrinking(spark, df)

        # spark.sql must NOT be queried for active count when table is absent
        spark.sql.assert_not_called()

    def test_skips_when_silver_has_zero_active_rows(self):
        spark = self._spark_with_active_count(active_count=0)
        df = self._df_with_count(0)

        # No raise, even though both are 0. First-population case.
        _assert_source_not_shrinking(spark, df)

    def test_threshold_constant_is_20_percent(self):
        """Locks in the agreed-on default so accidental changes are caught."""
        assert MAX_SOURCE_SHRINK_RATIO == 0.20


class TestTransformEntrypoint:
    """Tests for the top-level transform() function."""

    @patch("src.silver.transform_breweries._run_transform")
    @patch("src.silver.transform_breweries.create_spark_session")
    def test_stops_spark_on_failure(self, mock_create, mock_run):
        mock_spark = MagicMock()
        mock_create.return_value = mock_spark
        mock_run.side_effect = RuntimeError("kaboom")

        with pytest.raises(RuntimeError, match="kaboom"):
            transform("2026-04-29")

        mock_spark.stop.assert_called_once()

    @patch("src.silver.transform_breweries._run_transform")
    @patch("src.silver.transform_breweries.create_spark_session")
    def test_happy_path(self, mock_create, mock_run):
        mock_spark = MagicMock()
        mock_create.return_value = mock_spark

        transform("2026-04-29")

        mock_run.assert_called_once_with(mock_spark, "2026-04-29")
        mock_spark.stop.assert_called_once()


class TestRunTransformEmptySource:
    """If the Bronze partition is empty, _run_transform returns early."""

    @patch("src.silver.transform_breweries.F")
    def test_empty_source_returns_without_merge(self, _mock_f):
        """Empty Bronze for the date short-circuits — no MERGE issued."""
        spark = MagicMock()
        bronze_df = MagicMock()
        bronze_df.isEmpty.return_value = True
        spark.table.return_value.filter.return_value = bronze_df

        _run_transform(spark, "2026-04-29")

        # No MERGE was issued
        merge_calls = [args[0] for args, _ in spark.sql.call_args_list if "MERGE" in str(args[0])]
        assert merge_calls == []


class TestQuarantine:
    """The quarantine sink for rows that would fail Silver quality rules."""

    @staticmethod
    def _make_chained_df():
        """Return a MagicMock whose every chained method returns itself.

        This stand-in is good enough for the `select(...).withColumn(...)`
        chain in `_quarantine_invalid_records`, because the call shape is
        all we assert.
        """
        df = MagicMock(name="DataFrame")
        df.select.return_value = df
        df.withColumn.return_value = df
        df.count.return_value = 3
        return df

    @patch("src.silver.transform_breweries.F")
    def test_creates_table_on_first_run(self, _mock_f):
        spark = MagicMock()
        spark.catalog.tableExists.return_value = False
        bad_df = self._make_chained_df()

        _quarantine_invalid_records(spark, bad_df, REASON_NULL_ID, "2026-04-29")

        ddl_calls = [args[0] for args, _ in spark.sql.call_args_list]
        assert any("CREATE NAMESPACE IF NOT EXISTS nessie.silver" in c for c in ddl_calls)
        create_table = next((c for c in ddl_calls if "CREATE TABLE" in c), None)
        assert create_table is not None
        assert "breweries_quarantine" in create_table
        assert "PARTITIONED BY (quarantine_date)" in create_table
        assert "format-version" in create_table

    @patch("src.silver.transform_breweries.F")
    def test_skips_create_when_table_exists(self, _mock_f):
        spark = MagicMock()
        spark.catalog.tableExists.return_value = True
        bad_df = self._make_chained_df()

        _quarantine_invalid_records(spark, bad_df, REASON_NULL_ID, "2026-04-29")

        ddl_calls = [args[0] for args, _ in spark.sql.call_args_list]
        assert not any("CREATE TABLE" in c for c in ddl_calls)

    @patch("src.silver.transform_breweries.F")
    def test_appends_with_writeto(self, _mock_f):
        """Quarantine is append-only — we keep history, never overwrite."""
        spark = MagicMock()
        spark.catalog.tableExists.return_value = True
        bad_df = self._make_chained_df()

        _quarantine_invalid_records(spark, bad_df, REASON_NULL_ID, "2026-04-29")

        bad_df.writeTo.assert_called_with("nessie.silver.breweries_quarantine")
        bad_df.writeTo.return_value.append.assert_called_once()

    @patch("src.silver.transform_breweries.F")
    def test_decorates_with_reason_and_quarantine_columns(self, mock_f):
        spark = MagicMock()
        spark.catalog.tableExists.return_value = True
        bad_df = self._make_chained_df()

        _quarantine_invalid_records(spark, bad_df, REASON_NULL_ID, "2026-04-29")

        # The three .withColumn calls should add reason, quarantined_at, quarantine_date.
        column_names = [call.args[0] for call in bad_df.withColumn.call_args_list]
        assert column_names == ["quarantine_reason", "quarantined_at", "quarantine_date"]
        # Reason was passed as a literal — capture by checking F.lit was called.
        mock_f.lit.assert_any_call(REASON_NULL_ID)
        mock_f.lit.assert_any_call("2026-04-29")

    def test_reason_constant_value(self):
        """Reason codes are part of the public contract; freeze the value."""
        assert REASON_NULL_ID == "NULL_ID"


class TestRunTransformQuarantineFlow:
    """Top-level flow tests for the split that feeds the quarantine sink."""

    @patch("src.silver.transform_breweries._quarantine_invalid_records")
    @patch("src.silver.transform_breweries.F")
    def test_quarantines_when_bad_rows_present(self, _mock_f, mock_quarantine):
        spark = MagicMock()
        spark.catalog.tableExists.return_value = True

        bronze_df = MagicMock(name="BronzeDF")
        bronze_df.isEmpty.return_value = False
        bad_df = MagicMock(name="BadDF")
        bad_df.count.return_value = 4
        good_df = MagicMock(name="GoodDF")
        good_df.isEmpty.return_value = True  # short-circuit MERGE for this test

        bronze_df.filter.side_effect = [bad_df, good_df]
        spark.table.return_value.filter.return_value = bronze_df

        _run_transform(spark, "2026-04-29")

        mock_quarantine.assert_called_once_with(spark, bad_df, REASON_NULL_ID, "2026-04-29")

    @patch("src.silver.transform_breweries._quarantine_invalid_records")
    @patch("src.silver.transform_breweries.F")
    def test_skips_quarantine_when_no_bad_rows(self, _mock_f, mock_quarantine):
        spark = MagicMock()
        spark.catalog.tableExists.return_value = True

        bronze_df = MagicMock(name="BronzeDF")
        bronze_df.isEmpty.return_value = False
        bad_df = MagicMock(name="BadDF")
        bad_df.count.return_value = 0
        good_df = MagicMock(name="GoodDF")
        good_df.isEmpty.return_value = True  # short-circuit MERGE

        bronze_df.filter.side_effect = [bad_df, good_df]
        spark.table.return_value.filter.return_value = bronze_df

        _run_transform(spark, "2026-04-29")

        mock_quarantine.assert_not_called()

    @patch("src.silver.transform_breweries._quarantine_invalid_records")
    @patch("src.silver.transform_breweries.F")
    def test_short_circuits_merge_when_only_bad_rows(self, _mock_f, mock_quarantine):
        """If every row is bad, we quarantine them but skip MERGE entirely."""
        spark = MagicMock()
        spark.catalog.tableExists.return_value = True

        bronze_df = MagicMock(name="BronzeDF")
        bronze_df.isEmpty.return_value = False
        bad_df = MagicMock(name="BadDF")
        bad_df.count.return_value = 5
        good_df = MagicMock(name="GoodDF")
        good_df.isEmpty.return_value = True

        bronze_df.filter.side_effect = [bad_df, good_df]
        spark.table.return_value.filter.return_value = bronze_df

        _run_transform(spark, "2026-04-29")

        mock_quarantine.assert_called_once()
        # No MERGE was issued
        merge_calls = [args[0] for args, _ in spark.sql.call_args_list if "MERGE" in str(args[0])]
        assert merge_calls == []
