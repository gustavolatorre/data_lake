"""Unit tests for data quality check functions."""

import pytest

from src.utils.data_quality import check_null_counts, check_row_count, check_schema, log_quality_summary


class TestCheckNullCounts:
    """Tests for the check_null_counts function."""

    def test_detects_nulls(self, sample_df):
        """Should detect null values in specified columns."""
        results = check_null_counts(sample_df, ["state", "phone"])

        assert results["state"] == 1  # id-003 has null state
        assert results["phone"] >= 1  # some breweries have null phone

    def test_no_nulls_in_id(self, sample_df):
        """Should report zero nulls for the id column."""
        results = check_null_counts(sample_df, ["id"])
        assert results["id"] == 0

    def test_fail_on_nulls_raises(self, sample_df):
        """Should raise ValueError when fail_on_nulls is True and nulls exist."""
        with pytest.raises(ValueError, match="null values"):
            check_null_counts(sample_df, ["state"], fail_on_nulls=True)

    def test_skips_missing_columns(self, sample_df):
        """Should skip columns that don't exist in the DataFrame."""
        results = check_null_counts(sample_df, ["nonexistent_column"])
        assert "nonexistent_column" not in results


class TestCheckSchema:
    """Tests for the check_schema function."""

    def test_valid_schema_passes(self, sample_df):
        """Should return empty set when all expected columns are present."""
        expected = {"id", "name", "brewery_type"}
        missing = check_schema(sample_df, expected)
        assert missing == set()

    def test_missing_columns_raises(self, sample_df):
        """Should raise ValueError when expected columns are missing."""
        expected = {"id", "name", "nonexistent_column"}
        with pytest.raises(ValueError, match="Missing columns"):
            check_schema(sample_df, expected)


class TestCheckRowCount:
    """Tests for the check_row_count function."""

    def test_sufficient_rows_passes(self, sample_df):
        """Should return count when DataFrame meets minimum."""
        count = check_row_count(sample_df, min_rows=1)
        assert count == 5

    def test_empty_df_raises(self, empty_df):
        """Should raise ValueError when DataFrame is empty."""
        with pytest.raises(ValueError, match="expected at least"):
            check_row_count(empty_df, min_rows=1)

    def test_threshold_exceeded_raises(self, sample_df):
        """Should raise ValueError when count is below min_rows."""
        with pytest.raises(ValueError, match="expected at least"):
            check_row_count(sample_df, min_rows=100)


class TestLogQualitySummary:
    """Tests for the log_quality_summary function."""

    def test_returns_summary_dict(self, sample_df):
        """Should return a dict with row_count and column_count."""
        summary = log_quality_summary(sample_df, "test")

        assert summary["row_count"] == 5
        assert summary["column_count"] == 16

    def test_includes_null_counts_when_specified(self, sample_df):
        """Should include null_counts when critical_columns are provided."""
        summary = log_quality_summary(sample_df, "test", critical_columns=["id", "state"])

        assert "null_counts" in summary
        assert summary["null_counts"]["id"] == 0
        assert summary["null_counts"]["state"] == 1
