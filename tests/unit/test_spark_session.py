"""Unit tests for ``src.utils.spark_session.create_spark_session``.

The session is built with a long fluent ``.config(...)`` chain on
``SparkSession.builder``. We mock the builder so we can capture every config
the production code attempts to set, without spinning up a real JVM.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from src.utils.spark_session import create_spark_session


@pytest.fixture
def builder():
    """Return a MagicMock that pretends to be SparkSession.builder, capturing configs."""
    mock_session = MagicMock(name="SparkSession")
    builder_mock = MagicMock(name="builder")
    # Every chained call returns the same builder so we can collect configs.
    builder_mock.appName.return_value = builder_mock
    builder_mock.config.return_value = builder_mock
    builder_mock.getOrCreate.return_value = mock_session
    return builder_mock, mock_session


@pytest.fixture
def mock_settings():
    return MagicMock(
        minio_endpoint="minio:9000",
        minio_root_user="ak",
        minio_root_password="sk",
        nessie_uri="http://nessie:19120/api/v2",
    )


def _collected_configs(builder_mock) -> dict[str, str]:
    """Extract the kwargs/args from every .config() call on the builder."""
    configs: dict[str, str] = {}
    for call in builder_mock.config.call_args_list:
        args, kwargs = call
        if len(args) == 2:
            configs[args[0]] = args[1]
        elif kwargs:
            # config(key=..., value=...) form (defensive)
            configs[kwargs.get("key", "")] = kwargs.get("value", "")
    return configs


@patch("src.utils.spark_session.SparkSession")
@patch("src.utils.spark_session.get_settings")
def test_sets_application_name(mock_get_settings, mock_spark_cls, builder, mock_settings):
    builder_mock, _ = builder
    mock_spark_cls.builder = builder_mock
    mock_get_settings.return_value = mock_settings

    create_spark_session("MyApp")

    builder_mock.appName.assert_called_once_with("MyApp")


@patch("src.utils.spark_session.SparkSession")
@patch("src.utils.spark_session.get_settings")
def test_configures_iceberg_extension(mock_get_settings, mock_spark_cls, builder, mock_settings):
    builder_mock, _ = builder
    mock_spark_cls.builder = builder_mock
    mock_get_settings.return_value = mock_settings

    create_spark_session("test")

    configs = _collected_configs(builder_mock)
    assert (
        configs.get("spark.sql.extensions")
        == "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
    )


@patch("src.utils.spark_session.SparkSession")
@patch("src.utils.spark_session.get_settings")
def test_configures_nessie_catalog(mock_get_settings, mock_spark_cls, builder, mock_settings):
    builder_mock, _ = builder
    mock_spark_cls.builder = builder_mock
    mock_get_settings.return_value = mock_settings

    create_spark_session("test")

    configs = _collected_configs(builder_mock)
    assert configs.get("spark.sql.catalog.nessie") == "org.apache.iceberg.spark.SparkCatalog"
    assert (
        configs.get("spark.sql.catalog.nessie.catalog-impl")
        == "org.apache.iceberg.nessie.NessieCatalog"
    )
    assert configs.get("spark.sql.catalog.nessie.uri") == "http://nessie:19120/api/v2"
    assert configs.get("spark.sql.catalog.nessie.ref") == "main"
    assert configs.get("spark.sql.catalog.nessie.warehouse") == "s3a://warehouse/"


@patch("src.utils.spark_session.SparkSession")
@patch("src.utils.spark_session.get_settings")
def test_configures_s3a_credentials_from_settings(
    mock_get_settings, mock_spark_cls, builder, mock_settings
):
    builder_mock, _ = builder
    mock_spark_cls.builder = builder_mock
    mock_get_settings.return_value = mock_settings

    create_spark_session("test")

    configs = _collected_configs(builder_mock)
    assert configs.get("spark.hadoop.fs.s3a.endpoint") == "http://minio:9000"
    assert configs.get("spark.hadoop.fs.s3a.access.key") == "ak"
    assert configs.get("spark.hadoop.fs.s3a.secret.key") == "sk"
    assert configs.get("spark.hadoop.fs.s3a.path.style.access") == "true"


@patch("src.utils.spark_session.SparkSession")
@patch("src.utils.spark_session.get_settings")
def test_returns_get_or_create_result(mock_get_settings, mock_spark_cls, builder, mock_settings):
    builder_mock, session_mock = builder
    mock_spark_cls.builder = builder_mock
    mock_get_settings.return_value = mock_settings

    result = create_spark_session("test")

    assert result is session_mock
    builder_mock.getOrCreate.assert_called_once()
