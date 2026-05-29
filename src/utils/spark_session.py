"""SparkSession factory with Iceberg + Nessie + S3A configuration.

Provides a single factory function to create a fully configured SparkSession
that integrates with Apache Iceberg tables, Nessie REST catalog, and MinIO
(via S3A filesystem). Eliminates duplicated Spark configuration across scripts.

Also wires the OpenLineage Spark listener (P3.6). The listener is always
loaded (JAR bundled in the image) but only transmits when
``OPENLINEAGE_URL`` is set — otherwise it just logs to stderr.
"""

import logging

from pyspark.sql import SparkSession

from src.config.settings import Settings, get_settings

logger = logging.getLogger(__name__)

_OPENLINEAGE_LISTENER_CLASS = "io.openlineage.spark.agent.OpenLineageSparkListener"


def create_spark_session(app_name: str) -> SparkSession:
    """Create a SparkSession configured for Iceberg + Nessie + MinIO.

    The session is configured with:
    - Iceberg Spark extensions for DDL/DML support
    - Nessie REST catalog (type=rest) for Git-like table versioning
    - S3A filesystem for MinIO object storage access
    - KryoSerializer for optimized serialization
    - OpenLineage listener (always loaded; transmits when configured)

    Args:
        app_name: Name of the Spark application (visible in Spark UI).

    Returns:
        SparkSession: A fully configured Spark session.

    Raises:
        Exception: If SparkSession creation fails due to misconfiguration.
    """
    settings = get_settings()

    logger.info("Creating SparkSession '%s' with Iceberg + Nessie catalog", app_name)

    builder = (
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
    )
    builder = _apply_openlineage_config(builder, settings, app_name)
    session = builder.getOrCreate()

    logger.info("SparkSession '%s' created successfully", app_name)
    return session


def _apply_openlineage_config(builder, settings: Settings, app_name: str):
    """Attach OpenLineage Spark listener configs to the builder.

    The listener JAR (``openlineage-spark_2.13``) is bundled in the Spark
    image. We always register the listener class — it's harmless without
    transport, just logs locally — but we only set ``spark.openlineage.*``
    transport config when ``settings.openlineage_url`` is non-empty.

    Args:
        builder: SparkSession.Builder being configured.
        settings: Loaded application settings.
        app_name: Spark application name; used as the lineage job name so
            multiple SparkSubmits from the same Airflow DAG are
            distinguishable in the lineage graph.

    Returns:
        The builder, mutated in place (chain-friendly).
    """
    builder = builder.config("spark.extraListeners", _OPENLINEAGE_LISTENER_CLASS).config(
        "spark.openlineage.namespace", settings.openlineage_namespace
    )

    if not settings.openlineage_url:
        logger.info(
            "OpenLineage listener loaded for '%s' (transport disabled — set OPENLINEAGE_URL to enable)",
            app_name,
        )
        return builder

    builder = (
        builder.config("spark.openlineage.transport.type", "http")
        .config("spark.openlineage.transport.url", settings.openlineage_url)
        # Identify the job in lineage graphs by the same name Airflow uses.
        .config("spark.openlineage.appName", app_name)
    )
    logger.info(
        "OpenLineage listener will emit to %s (namespace=%s)",
        settings.openlineage_url,
        settings.openlineage_namespace,
    )
    return builder
