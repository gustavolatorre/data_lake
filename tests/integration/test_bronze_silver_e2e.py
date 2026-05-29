"""End-to-end integration tests: staging JSON → Bronze → Silver.

Each test drives real Bronze/Silver functions against a local Iceberg
warehouse (no Nessie, no MinIO). The goal is to catch regressions in
production code paths — particularly the contracts that unit-test mocks
can't catch: hidden partitioning actually pruning, ``overwritePartitions``
really being idempotent, ``MERGE INTO`` actually flipping ``is_active``
on soft-deletes, the quarantine sink actually receiving NULL-id rows.

Why ``_run_ingest`` / ``_run_transform`` and not the public ``ingest`` /
``transform`` entry points: the publics create their own SparkSession via
``create_spark_session`` (which wants Nessie URIs + S3A creds). The
internals take a pre-built session — exactly what the fixture provides.
"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

import pytest

from src.bronze.ingest_breweries import _run_ingest
from src.silver.transform_breweries import _run_transform

if TYPE_CHECKING:
    from pathlib import Path

    from pyspark.sql import SparkSession

EXECUTION_DATE = "2026-01-15"

# Minimal fixture set covering the dimensions the pipeline really cares about:
# - 2 valid rows (id present)
# - 1 quarantine candidate (id is NULL)
# - distinct states for partitioning behaviour
_RAW_ROWS: list[dict] = [
    {
        "id": "abc-001",
        "name": "Test Brewing Co",
        "brewery_type": "micro",
        "address_1": "123 Hop St",
        "city": "Portland",
        "state_province": "Oregon",
        "postal_code": "97201",
        "country": "United States",
        "longitude": -122.6,
        "latitude": 45.5,
        "phone": "5031234567",
        "website_url": "http://test.example.com",
        "state": "Oregon",
        "street": "123 Hop St",
    },
    {
        "id": "abc-002",
        "name": "Karnten Brauerei",
        "brewery_type": "brewpub",
        "address_1": "Hauptstr 1",
        "city": "Klagenfurt",
        "state_province": "Karnten",
        "postal_code": "9020",
        "country": "Austria",
        "longitude": 14.3,
        "latitude": 46.6,
        "phone": None,
        "website_url": None,
        "state": "Karnten",
        "street": "Hauptstr 1",
    },
    {
        # Bad row: NULL id — must be diverted to quarantine, not aborted.
        "id": None,
        "name": "Null ID Brewery",
        "brewery_type": "micro",
        "address_1": "999 Broken Pk",
        "city": "Nowhere",
        "state_province": "Unknown",
        "postal_code": "00000",
        "country": "United States",
        "longitude": None,
        "latitude": None,
        "phone": None,
        "website_url": None,
        "state": "Unknown",
        "street": "999 Broken Pk",
    },
]


def _write_staging(staging_dir: Path, execution_date: str, rows: list[dict]) -> str:
    """Persist ``rows`` as the day's staging JSON file, return the base URI.

    Mirrors what the production fetcher writes to MinIO: a single JSON file
    per execution_date, multiline-true. Returns the ``file://`` base
    that the integration tests feed into ``_run_ingest``.
    """
    date_dir = staging_dir / execution_date
    date_dir.mkdir(parents=True, exist_ok=True)
    (date_dir / "breweries.json").write_text(json.dumps(rows), encoding="utf-8")
    # ``_run_ingest`` appends ``/<execution_date>/`` to whatever base we pass.
    return f"file://{staging_dir.as_posix()}"


def test_bronze_writes_partitioned_iceberg_table(spark: SparkSession, staging_dir: Path) -> None:
    """Happy path: ingest creates the Bronze table with metadata columns."""
    staging_base = _write_staging(staging_dir, EXECUTION_DATE, _RAW_ROWS[:2])

    _run_ingest(spark, EXECUTION_DATE, staging_path_base=staging_base)

    rows = spark.sql("SELECT id, ingestion_date FROM nessie.bronze.breweries ORDER BY id").collect()
    assert [r["id"] for r in rows] == ["abc-001", "abc-002"]
    assert all(r["ingestion_date"] == EXECUTION_DATE for r in rows)
    # ``ingested_at`` and ``ingestion_ts`` are populated by the ingest step.
    enriched = spark.sql("SELECT ingested_at, ingestion_ts FROM nessie.bronze.breweries").collect()
    assert all(r["ingested_at"] is not None for r in enriched)
    assert all(r["ingestion_ts"] is not None for r in enriched)


def test_bronze_is_idempotent_via_overwritepartitions(spark: SparkSession, staging_dir: Path) -> None:
    """Re-running on the same date replaces — never duplicates — the partition.

    This is the contract P0.5 added (idempotency for safe retries) and that
    P2.3 reinforced (hidden partitioning by ``days(ingestion_ts)``). A unit
    test can only check that ``overwritePartitions`` was called; only a real
    write proves the partition state is correct.
    """
    staging_base = _write_staging(staging_dir, EXECUTION_DATE, _RAW_ROWS[:2])

    _run_ingest(spark, EXECUTION_DATE, staging_path_base=staging_base)
    _run_ingest(spark, EXECUTION_DATE, staging_path_base=staging_base)

    count = spark.sql("SELECT COUNT(*) AS c FROM nessie.bronze.breweries").collect()[0]["c"]
    assert count == 2, "second run must overwrite, not append"


def test_silver_merge_upserts_and_soft_deletes(spark: SparkSession, staging_dir: Path) -> None:
    """Silver MERGE flips ``is_active`` on rows the source no longer sees.

    Pipeline: day 1 staging has 2 rows → bronze → silver merges them with
    ``is_active=true``. Day 2 staging only has the first row → bronze
    overwrites the partition → silver MERGE should keep abc-001 active but
    flip abc-002 to ``is_active=false`` via ``WHEN NOT MATCHED BY SOURCE``.
    Without a real Iceberg + MERGE this contract isn't exercised at all.
    """
    day_one = "2026-01-15"
    day_two = "2026-01-16"

    base = _write_staging(staging_dir, day_one, _RAW_ROWS[:2])
    _run_ingest(spark, day_one, staging_path_base=base)
    _run_transform(spark, day_one)

    base = _write_staging(staging_dir, day_two, _RAW_ROWS[:1])  # only abc-001
    _run_ingest(spark, day_two, staging_path_base=base)
    _run_transform(spark, day_two)

    silver = spark.sql("SELECT id, is_active FROM nessie.silver.breweries ORDER BY id").collect()
    by_id = {r["id"]: r["is_active"] for r in silver}
    assert by_id == {"abc-001": True, "abc-002": False}


def test_quarantine_receives_null_id_rows(spark: SparkSession, staging_dir: Path) -> None:
    """NULL-id rows land in the quarantine sink, not the main Silver table.

    This is the P3.8 contract: the Silver MERGE keys on ``id``, so NULL
    breaks the upsert. Diverting to quarantine keeps the rest of the day's
    data flowing while preserving the bad row for investigation.
    """
    base = _write_staging(staging_dir, EXECUTION_DATE, _RAW_ROWS)  # includes NULL-id row
    _run_ingest(spark, EXECUTION_DATE, staging_path_base=base)
    _run_transform(spark, EXECUTION_DATE)

    silver_count = spark.sql("SELECT COUNT(*) AS c FROM nessie.silver.breweries").collect()[0]["c"]
    quarantine_count = spark.sql("SELECT COUNT(*) AS c FROM nessie.silver.breweries_quarantine").collect()[0]["c"]

    assert silver_count == 2  # 2 valid rows
    assert quarantine_count == 1  # 1 NULL-id row diverted

    reason = spark.sql("SELECT quarantine_reason FROM nessie.silver.breweries_quarantine LIMIT 1").collect()[0][
        "quarantine_reason"
    ]
    assert reason == "NULL_ID"


def test_silver_aborts_on_source_shrink(spark: SparkSession, staging_dir: Path) -> None:
    """Shrink guard fires when today's source is much smaller than yesterday's Silver.

    Without this guard, a partial fetch (pagination interrupted) would
    silently soft-delete half the table via ``WHEN NOT MATCHED BY SOURCE``.
    Threshold is 20% — feed a 100-row baseline, then a 1-row day, expect
    ``SourceShrinkError``.
    """
    from src.silver.transform_breweries import SourceShrinkError

    big_day = "2026-01-15"
    small_day = "2026-01-16"

    # 100 rows on day 1 to give the guard a baseline.
    many_rows = [{**_RAW_ROWS[0], "id": f"bulk-{i:03d}", "name": f"Brew #{i}"} for i in range(100)]
    base = _write_staging(staging_dir, big_day, many_rows)
    _run_ingest(spark, big_day, staging_path_base=base)
    _run_transform(spark, big_day)

    # 1 row on day 2 → 99% shrink → must abort.
    base = _write_staging(staging_dir, small_day, _RAW_ROWS[:1])
    _run_ingest(spark, small_day, staging_path_base=base)
    with pytest.raises(SourceShrinkError):
        _run_transform(spark, small_day)
