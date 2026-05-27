"""Centralized application settings using pydantic-settings.

All configuration is loaded from environment variables or a `.env` file.
This module provides a single source of truth for all service endpoints,
credentials, and tunable parameters across the application.
"""

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Application settings loaded from environment variables.

    Attributes:
        minio_endpoint: MinIO server address (host:port).
        minio_root_user: MinIO access key.
        minio_root_password: MinIO secret key.
        nessie_uri: Nessie Iceberg REST catalog endpoint.
        spark_master: Spark master URL for job submission.
        spark_driver_memory: Memory allocated to the Spark driver.
        spark_executor_memory: Memory allocated to each Spark executor.
        api_base_url: OpenBreweryDB API base URL.
        api_per_page: Number of records per API page.
        api_timeout_seconds: HTTP request timeout in seconds.
    """

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # MinIO
    minio_endpoint: str = "minio:9000"
    minio_root_user: str
    minio_root_password: str
    # TLS toggle for the MinIO client. Default False for local docker-compose
    # (HTTP). Set MINIO_SECURE=true in staging/prod environments where MinIO
    # (or the upstream S3 service) is fronted by HTTPS.
    minio_secure: bool = False

    # Nessie
    nessie_uri: str = "http://nessie:19120/api/v2"

    # Spark
    spark_master: str = "spark://spark-master:7077"
    spark_driver_memory: str = "2g"
    spark_executor_memory: str = "2g"

    # API
    api_base_url: str = "https://api.openbrewerydb.org/v1/breweries"
    api_per_page: int = 50
    api_timeout_seconds: int = 15


def get_settings() -> Settings:
    """Return a cached Settings instance.

    Returns:
        Settings: Application configuration object.
    """
    # pydantic-settings populates required fields from env vars / .env at runtime
    return Settings()  # type: ignore[call-arg]
