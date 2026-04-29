"""Unit tests for Silver layer — brewery transformations."""

import pytest

from src.silver.transform_breweries import normalize_unicode


class TestNormalizeUnicode:
    """Tests for the normalize_unicode function."""

    def test_removes_diacritics(self):
        """Should remove accent marks from characters."""
        assert normalize_unicode("Kärnten") == "Karnten"
        assert normalize_unicode("Niederösterreich") == "Niederosterreich"

    def test_preserves_plain_ascii(self):
        """Should not modify plain ASCII strings."""
        assert normalize_unicode("Oklahoma") == "Oklahoma"
        assert normalize_unicode("New York") == "New York"

    def test_handles_none(self):
        """Should return None for None input."""
        assert normalize_unicode(None) is None

    def test_handles_empty_string(self):
        """Should return empty string for empty input."""
        assert normalize_unicode("") == ""

    def test_handles_mixed_accents(self):
        """Should handle strings with multiple accented characters."""
        assert normalize_unicode("São Paulo") == "Sao Paulo"
        assert normalize_unicode("Zürich") == "Zurich"
        assert normalize_unicode("Päijät-Häme") == "Paijat-Hame"


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
        from pyspark.sql.types import StringType

        normalize_udf = F.udf(normalize_unicode, StringType())
        df = sample_df.withColumn("state", normalize_udf(F.col("state")))

        karnten_rows = df.filter(F.col("state") == "Karnten").count()
        assert karnten_rows == 1

    def test_schema_has_expected_columns(self, sample_df):
        """Should contain all 16 expected columns."""
        expected = {
            "id", "name", "brewery_type", "address_1", "address_2",
            "address_3", "city", "state_province", "postal_code", "country",
            "longitude", "latitude", "phone", "website_url", "state", "street",
        }
        assert set(sample_df.columns) == expected
