"""MinIO client factory for S3-compatible object storage operations.

Provides a centralized client factory and helper functions for interacting
with MinIO buckets (bronze, silver, gold layers).
"""

import logging

from minio import Minio, S3Error

from src.config.settings import get_settings

logger = logging.getLogger(__name__)


def create_minio_client() -> Minio:
    """Create a MinIO client using application settings.

    Returns:
        Minio: Configured MinIO client instance.

    Raises:
        Exception: If the client cannot connect to MinIO.
    """
    settings = get_settings()

    client = Minio(
        settings.minio_endpoint,
        access_key=settings.minio_root_user,
        secret_key=settings.minio_root_password,
        secure=settings.minio_secure,
    )

    logger.info(
        "MinIO client created for endpoint '%s' (TLS=%s)",
        settings.minio_endpoint,
        settings.minio_secure,
    )
    return client


def ensure_bucket_exists(client: Minio, bucket_name: str) -> None:
    """Ensure a MinIO bucket exists, creating it if necessary.

    Args:
        client: MinIO client instance.
        bucket_name: Name of the bucket to verify/create.

    Raises:
        S3Error: If bucket creation fails due to permissions or connectivity.
    """
    try:
        if not client.bucket_exists(bucket_name):
            client.make_bucket(bucket_name)
            logger.info("Bucket '%s' created", bucket_name)
        else:
            logger.debug("Bucket '%s' already exists", bucket_name)
    except S3Error:
        logger.exception("Failed to verify/create bucket '%s'", bucket_name)
        raise
