"""Unit tests for ``src.utils.minio_client``."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
from minio.error import S3Error

from src.utils.minio_client import create_minio_client, ensure_bucket_exists


@patch("src.utils.minio_client.get_settings")
@patch("src.utils.minio_client.Minio")
def test_create_minio_client_uses_settings(mock_minio_cls, mock_get_settings):
    """create_minio_client must wire Settings into the Minio constructor."""
    mock_get_settings.return_value = MagicMock(
        minio_endpoint="example:9000",
        minio_root_user="alice",
        minio_root_password="s3cret",
        minio_secure=True,
    )

    create_minio_client()

    mock_minio_cls.assert_called_once_with(
        "example:9000",
        access_key="alice",
        secret_key="s3cret",
        secure=True,
    )


@patch("src.utils.minio_client.get_settings")
@patch("src.utils.minio_client.Minio")
def test_create_minio_client_defaults_to_insecure(mock_minio_cls, mock_get_settings):
    """A False minio_secure setting must propagate as secure=False."""
    mock_get_settings.return_value = MagicMock(
        minio_endpoint="local:9000",
        minio_root_user="u",
        minio_root_password="p",
        minio_secure=False,
    )

    create_minio_client()

    _, kwargs = mock_minio_cls.call_args
    assert kwargs["secure"] is False


def test_ensure_bucket_exists_creates_when_missing():
    """ensure_bucket_exists must call make_bucket when the bucket is absent."""
    client = MagicMock()
    client.bucket_exists.return_value = False

    ensure_bucket_exists(client, "my-bucket")

    client.bucket_exists.assert_called_once_with("my-bucket")
    client.make_bucket.assert_called_once_with("my-bucket")


def test_ensure_bucket_exists_skips_when_present():
    """ensure_bucket_exists must NOT call make_bucket when the bucket exists."""
    client = MagicMock()
    client.bucket_exists.return_value = True

    ensure_bucket_exists(client, "already-here")

    client.bucket_exists.assert_called_once_with("already-here")
    client.make_bucket.assert_not_called()


def test_ensure_bucket_exists_reraises_s3error():
    """Any S3Error during bucket existence/creation must propagate to the caller."""
    client = MagicMock()
    client.bucket_exists.side_effect = S3Error(
        code="AccessDenied",
        message="nope",
        resource="my-bucket",
        request_id="rid",
        host_id="hid",
        response=MagicMock(status=403),
    )

    with pytest.raises(S3Error):
        ensure_bucket_exists(client, "my-bucket")
