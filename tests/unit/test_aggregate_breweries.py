"""Unit tests for Gold layer — brewery aggregation."""

from pyspark.sql import functions as F


class TestAggregation:
    """Tests for the Gold aggregation logic."""

    def test_aggregation_counts(self, spark, sample_df):
        """Should produce correct counts grouped by type and state."""
        df_agg = (
            sample_df.groupBy("brewery_type", "state")
            .agg(F.count("*").alias("quantity"))
            .orderBy("state", "brewery_type")
        )

        # Verify we get the expected number of groups
        assert df_agg.count() > 0

        # Check specific known aggregation: Oklahoma has 1 micro brewery
        oklahoma_micro = df_agg.filter(
            (F.col("state") == "Oklahoma") & (F.col("brewery_type") == "micro")
        ).first()
        assert oklahoma_micro is not None
        assert oklahoma_micro["quantity"] == 1

    def test_aggregation_columns(self, spark, sample_df):
        """Should produce exactly 3 columns: brewery_type, state, quantity."""
        df_agg = (
            sample_df.groupBy("brewery_type", "state")
            .agg(F.count("*").alias("quantity"))
        )

        assert set(df_agg.columns) == {"brewery_type", "state", "quantity"}

    def test_aggregation_no_nulls_in_quantity(self, spark, sample_df):
        """Should not have null values in the quantity column."""
        df_agg = (
            sample_df.groupBy("brewery_type", "state")
            .agg(F.count("*").alias("quantity"))
        )

        null_count = df_agg.filter(F.col("quantity").isNull()).count()
        assert null_count == 0

    def test_aggregation_all_quantities_positive(self, spark, sample_df):
        """All quantities should be greater than 0."""
        df_agg = (
            sample_df.groupBy("brewery_type", "state")
            .agg(F.count("*").alias("quantity"))
        )

        negative = df_agg.filter(F.col("quantity") <= 0).count()
        assert negative == 0

    def test_aggregation_handles_null_state(self, spark, sample_df):
        """Should include null state records in aggregation."""
        df_agg = (
            sample_df.groupBy("brewery_type", "state")
            .agg(F.count("*").alias("quantity"))
        )

        # id-003 has null state, should still appear as a group
        null_state_groups = df_agg.filter(F.col("state").isNull()).count()
        assert null_state_groups == 1  # nano type with null state
