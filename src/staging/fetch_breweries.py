"""Bronze layer — fetch brewery data from OpenBreweryDB API and upload to MinIO.

This module handles paginated API ingestion with retry logic, storing raw JSON
files in the MinIO bronze bucket organized by execution date:
    bronze/breweries/{execution_date}/breweries_page_{N}.json
"""

import io
import json
import logging

import requests
from minio import Minio
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from src.config.settings import get_settings
from src.utils.minio_client import create_minio_client, ensure_bucket_exists

logger = logging.getLogger(__name__)

STAGING_BUCKET = "staging"

# Retry policy for the OpenBreweryDB API: 3 attempts with exponential backoff
# (1s, 2s, 4s) on transient errors. Applied per-request — does NOT replay the
# entire pagination loop. Airflow still owns task-level retries (3x) for
# anything this doesn't recover.
_RETRY_POLICY = Retry(
    total=3,
    backoff_factor=1,
    status_forcelist=[429, 500, 502, 503, 504],
    allowed_methods=["GET"],
    raise_on_status=False,
)


def _build_session() -> requests.Session:
    """Build a requests.Session with the standard retry policy mounted."""
    session = requests.Session()
    adapter = HTTPAdapter(max_retries=_RETRY_POLICY)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


def fetch_and_upload(execution_date: str) -> int:
    """Fetch all brewery pages from the API and upload to MinIO staging bucket.

    Paginates through the OpenBreweryDB API, uploading each page as a separate
    JSON file. Stops when the API returns an empty list.

    Args:
        execution_date: Date string (YYYY-MM-DD) used for partitioning
            files in the staging bucket. Should come from Airflow's ``ds``.

    Returns:
        Total number of records fetched across all pages.

    Raises:
        requests.exceptions.HTTPError: If the API returns a non-2xx status.
        requests.exceptions.Timeout: If the API does not respond in time.
        ValueError: If the API response is not a valid JSON list.
    """
    settings = get_settings()
    client = create_minio_client()
    ensure_bucket_exists(client, STAGING_BUCKET)
    http_session = _build_session()

    total_records = 0
    page = 1

    logger.info("Starting brewery data fetch for date=%s", execution_date)

    while True:
        data = _fetch_page(
            session=http_session,
            base_url=settings.api_base_url,
            page=page,
            per_page=settings.api_per_page,
            timeout=settings.api_timeout_seconds,
        )

        if not data:
            logger.info("All pages processed. Total records: %d", total_records)
            break

        object_name = f"breweries/{execution_date}/breweries_page_{page}.json"
        _upload_json(client, STAGING_BUCKET, object_name, data)

        total_records += len(data)
        logger.info("Page %d uploaded (%d records) → %s", page, len(data), object_name)
        page += 1

    return total_records


def _fetch_page(
    session: requests.Session,
    base_url: str,
    page: int,
    per_page: int,
    timeout: int,
) -> list[dict]:
    """Fetch a single page from the OpenBreweryDB API.

    Args:
        session: Pre-configured requests.Session with retry policy.
        base_url: API base URL.
        page: Page number to fetch.
        per_page: Number of records per page.
        timeout: HTTP request timeout in seconds.

    Returns:
        List of brewery dictionaries, or empty list if no more data.

    Raises:
        requests.exceptions.HTTPError: On non-2xx response.
        requests.exceptions.Timeout: On request timeout.
        ValueError: If response is not a JSON list.
    """
    params: dict[str, str | int] = {"page": page, "per_page": per_page, "sort": "name,asc"}

    response = session.get(base_url, params=params, timeout=timeout)
    response.raise_for_status()

    data = response.json()

    if not isinstance(data, list):
        msg = f"Invalid API response on page {page}: expected list, got {type(data).__name__}"
        raise ValueError(msg)

    return data


def _upload_json(
    client: Minio,
    bucket: str,
    object_name: str,
    data: list[dict],
) -> None:
    """Upload JSON data to MinIO.

    Args:
        client: MinIO client instance.
        bucket: Target bucket name.
        object_name: Object key (path) in the bucket.
        data: List of dictionaries to serialize as JSON.
    """
    json_bytes = json.dumps(data, ensure_ascii=False, indent=2).encode("utf-8")
    file_obj = io.BytesIO(json_bytes)

    client.put_object(
        bucket_name=bucket,
        object_name=object_name,
        data=file_obj,
        length=len(json_bytes),
        content_type="application/json",
    )
