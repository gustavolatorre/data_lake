"""Unit tests for ``src.maintenance.iceberg_maintenance``."""

from __future__ import annotations

import re
from unittest.mock import MagicMock, patch

import pytest

from src.maintenance.iceberg_maintenance import (
    MAINTAINED_TABLES,
    _expire_snapshots,
    _parse_args,
    _remove_orphan_files,
    _rewrite_data_files,
    run_maintenance,
)


class TestArgParser:
    def test_defaults(self):
        args = _parse_args([])
        assert args.retention_days == 30
        assert args.min_snapshots == 5

    def test_custom_retention(self):
        args = _parse_args(["--retention-days", "7", "--min-snapshots", "10"])
        assert args.retention_days == 7
        assert args.min_snapshots == 10


class TestMaintenanceProcedures:
    def test_rewrite_calls_iceberg_procedure(self):
        spark = MagicMock()
        _rewrite_data_files(spark, "nessie.bronze.breweries")
        spark.sql.assert_called_once_with("CALL nessie.system.rewrite_data_files(table => 'nessie.bronze.breweries')")

    def test_remove_orphan_calls_iceberg_procedure(self):
        spark = MagicMock()
        _remove_orphan_files(spark, "nessie.silver.breweries")
        spark.sql.assert_called_once_with("CALL nessie.system.remove_orphan_files(table => 'nessie.silver.breweries')")

    def test_expire_snapshots_includes_retention(self):
        spark = MagicMock()
        _expire_snapshots(spark, "nessie.bronze.breweries", retention_days=14, min_snapshots=3)

        spark.sql.assert_called_once()
        sql = spark.sql.call_args.args[0]
        assert "expire_snapshots" in sql
        assert "table => 'nessie.bronze.breweries'" in sql
        assert "retain_last => 3" in sql
        # timestamp threshold should be in YYYY-MM-DD HH:MM:SS.ffffff format
        assert re.search(r"TIMESTAMP '\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d+'", sql)


class TestRunMaintenance:
    @patch("src.maintenance.iceberg_maintenance.create_spark_session")
    def test_processes_every_configured_table(self, mock_create):
        mock_spark = MagicMock()
        mock_create.return_value = mock_spark

        run_maintenance(retention_days=30, min_snapshots=5)

        # 3 procedures × number of tables
        expected_calls = 3 * len(MAINTAINED_TABLES)
        assert mock_spark.sql.call_count == expected_calls
        mock_spark.stop.assert_called_once()

    @patch("src.maintenance.iceberg_maintenance.create_spark_session")
    def test_stops_spark_on_failure(self, mock_create):
        mock_spark = MagicMock()
        mock_spark.sql.side_effect = RuntimeError("compaction crashed")
        mock_create.return_value = mock_spark

        with pytest.raises(RuntimeError, match="compaction crashed"):
            run_maintenance(retention_days=30, min_snapshots=5)

        mock_spark.stop.assert_called_once()


class TestModuleConstants:
    def test_maintained_tables_covers_bronze_silver_and_quarantine(self):
        assert "nessie.bronze.breweries" in MAINTAINED_TABLES
        assert "nessie.silver.breweries" in MAINTAINED_TABLES
        # P3.8 — the quarantine sink is append-only so small-file accumulation
        # is a real risk; it must be on the maintenance roster.
        assert "nessie.silver.breweries_quarantine" in MAINTAINED_TABLES
