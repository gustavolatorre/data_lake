"""SparkSession factory with Iceberg + Nessie + S3A configuration.

Provides a single factory function to create a fully configured SparkSession
that integrates with Apache Iceberg tables, Nessie REST catalog, and MinIO
(via S3A filesystem). Eliminates duplicated Spark configuration across scripts.
"""

import logging

from pyspark.sql import SparkSession

from src.config.settings import get_settings

logger = logging.getLogger(__name__)


def create_spark_session(app_name: str) -> SparkSession:
    """Create a SparkSession configured for Iceberg + Nessie + MinIO.

    The session is configured with:
    - Iceberg Spark extensions for DDL/DML support
    - Nessie REST catalog (type=rest) for Git-like table versioning
    - S3A filesystem for MinIO object storage access
    - KryoSerializer for optimized serialization

    Args:
        app_name: Name of the Spark application (visible in Spark UI).

    Returns:
        SparkSession: A fully configured Spark session.

    Raises:
        Exception: If SparkSession creation fails due to misconfiguration.
    """
    settings = get_settings()

    logger.info("Creating SparkSession '%s' with Iceberg + Nessie catalog", app_name)

    session = (
        SparkSession.builder.appName(app_name)
        # Iceberg extensions
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        # Nessie catalog (native NessieCatalog — Nessie 0.79 does not support REST)
        .config(
            "spark.sql.catalog.nessie",
            "org.apache.iceberg.spark.SparkCatalog",
        )
        .config(
            "spark.sql.catalog.nessie.catalog-impl",
            "org.apache.iceberg.nessie.NessieCatalog",
        )
        .config("spark.sql.catalog.nessie.uri", settings.nessie_uri)
        .config("spark.sql.catalog.nessie.ref", "main")
        .config(
            "spark.sql.catalog.nessie.io-impl",
            "org.apache.iceberg.hadoop.HadoopFileIO",
        )
        .config("spark.sql.catalog.nessie.warehouse", "s3a://warehouse/")
        # S3A filesystem (MinIO)
        .config(
            "spark.hadoop.fs.s3a.endpoint",
            f"http://{settings.minio_endpoint}",
        )
        .config("spark.hadoop.fs.s3a.access.key", settings.minio_root_user)
        .config("spark.hadoop.fs.s3a.secret.key", settings.minio_root_password)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config(
            "spark.hadoop.fs.s3a.impl",
            "org.apache.hadoop.fs.s3a.S3AFileSystem",
        )
        .getOrCreate()
    )

    logger.info("SparkSession '%s' created successfully", app_name)
    return session
