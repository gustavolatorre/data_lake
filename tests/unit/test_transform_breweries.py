"""Unit tests for Silver layer — brewery transformations."""

from src.silver.transform_breweries import _apply_native_transformations


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
