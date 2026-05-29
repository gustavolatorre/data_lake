"""Fixtures for integration tests.

Integration tests exercise the **real** Bronze and Silver code paths against
an Iceberg-on-local-filesystem warehouse, with no Nessie or MinIO involved.
A Hadoop catalog also called ``nessie`` lets the production SQL strings
(``nessie.bronze.breweries`` etc.) run unmodified — the production code
doesn't care that the catalog is backed by ``/tmp/...`` instead of a
Nessie service. Spark is configured via ``--packages`` so CI fetches the
Iceberg runtime JAR on the first run.

These fixtures are kept out of ``tests/conftest.py`` on purpose: the
existing unit-test fixture is a plain SparkSession without Iceberg
extensions, and we don't want every unit test to pay the cost of loading
the runtime + downloading the JAR.
"""

from __future__ import annotations

import shutil
from typing import TYPE_CHECKING

import pytest
from pyspark.sql import SparkSession

if TYPE_CHECKING:
    from pathlib import Path

# Pin the Iceberg + Spark runtime coordinates that match the prod image
# (``docker/Dockerfile.spark`` bundles the same JAR locally; CI fetches it
# from Maven Central on first use, then the Ivy cache makes subsequent runs
# fast).
_ICEBERG_PACKAGE = "org.apache.iceberg:iceberg-spark-runtime-4.0_2.13:1.11.0"


@pytest.fixture(scope="session")
def iceberg_warehouse(tmp_path_factory) -> Path:
    """Per-session local Iceberg warehouse root.

    Returned as a ``Path`` and re-used across every integration test so we
    don't pay Spark startup cost more than once. Cleared at session end.
    """
    root = tmp_path_factory.mktemp("warehouse")
    yield root
    # Best-effort teardown; failures here shouldn't mask test errors.
    shutil.rmtree(root, ignore_errors=True)


@pytest.fixture(scope="session")
def spark(iceberg_warehouse: Path) -> SparkSession:
    """SparkSession wired for Iceberg with a local catalog named ``nessie``.

    The catalog name **must** be ``nessie`` so the production SQL
    (``nessie.bronze.breweries`` etc.) runs unchanged. ``type=hadoop``
    means Spark talks to the local filesystem instead of a Nessie REST
    service.
    """
    session = (
        SparkSession.builder.master("local[2]")
        .appName("data_lake_integration_tests")
        .config("spark.jars.packages", _ICEBERG_PACKAGE)
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config("spark.sql.catalog.nessie", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.nessie.type", "hadoop")
        .config("spark.sql.catalog.nessie.warehouse", str(iceberg_warehouse))
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.driver.host", "127.0.0.1")
        .getOrCreate()
    )
    yield session
    session.stop()


@pytest.fixture(autouse=True)
def reset_catalog(spark: SparkSession) -> None:
    """Drop bronze + silver namespaces between tests for isolation.

    Each test gets a clean slate. Without this, table state from one test
    leaks into the next (Iceberg's ``overwritePartitions`` + ``MERGE``
    are intentionally stateful).
    """
    yield
    for namespace in ("bronze", "silver"):
        try:
            spark.sql(f"DROP TABLE IF EXISTS nessie.{namespace}.breweries PURGE")
            spark.sql(f"DROP TABLE IF EXISTS nessie.{namespace}.breweries_quarantine PURGE")
            spark.sql(f"DROP NAMESPACE IF EXISTS nessie.{namespace}")
        except Exception:
            # Best-effort — quarantine table may not exist in every test
            pass


@pytest.fixture
def staging_dir(tmp_path: Path) -> Path:
    """Per-test staging root — analog of MinIO's ``s3a://staging``.

    Returned as a Path that integration tests join with
    ``breweries/<execution_date>/<file>.json``. Backed by pytest's
    ``tmp_path``, so each test gets isolated state without manual cleanup.
    """
    root = tmp_path / "staging" / "breweries"
    root.mkdir(parents=True)
    return root
