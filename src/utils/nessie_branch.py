"""Thin wrapper over the Nessie REST API for branch lifecycle (P3.1).

Each Bronze/Silver run creates an **isolated branch** off ``main``, writes
to it, and merges back only after the quality gates have passed. If a step
fails the branch is dropped, leaving ``main`` untouched. This is the
canonical "transactional ETL" pattern Nessie was built for; on-disk it
costs nothing (branches are just metadata pointers).

We hit the REST API directly instead of going through nessie-spark-extensions
because that JAR was deliberately removed from the Spark image (its
NessieCatalog is already bundled in the Iceberg runtime).

API reference: https://projectnessie.org/nessie-latest/api/
We use the v2 endpoints (the Nessie 0.79+ default).
"""

from __future__ import annotations

import logging
import re

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from src.config.settings import get_settings

logger = logging.getLogger(__name__)

# Branch name sanity. Nessie accepts most strings but we keep ours strict
# so they're safe in URLs without %-encoding, and so they line up with the
# convention dbt + Airflow already use for asset URIs.
_BRANCH_NAME_RE = re.compile(r"^[A-Za-z0-9_.\-]+$")

# How long we wait for a single Nessie HTTP call. The API is meant to be
# fast (in-memory + Postgres-backed by default); seconds-long latency means
# something is wrong and we'd rather fail than hang the DAG.
_HTTP_TIMEOUT_SECONDS = 15

# Retry on the kinds of transient errors that show up when the Nessie
# pod is rolling. Same policy as the staging fetcher.
_RETRY_POLICY = Retry(
    total=3,
    backoff_factor=1,
    status_forcelist=[429, 500, 502, 503, 504],
    allowed_methods=["GET", "POST", "DELETE"],
    raise_on_status=False,
)


class NessieAPIError(RuntimeError):
    """Raised when a Nessie HTTP call returns a non-2xx response."""


def _session() -> requests.Session:
    """A retrying ``requests.Session`` aimed at the configured Nessie URI."""
    s = requests.Session()
    adapter = HTTPAdapter(max_retries=_RETRY_POLICY)
    s.mount("http://", adapter)
    s.mount("https://", adapter)
    return s


def _base_url() -> str:
    """The catalog base URL — ``settings.nessie_uri`` already includes ``/api/v2``."""
    return get_settings().nessie_uri.rstrip("/")


def _validate_branch_name(name: str) -> None:
    if not _BRANCH_NAME_RE.fullmatch(name):
        msg = (
            f"Invalid Nessie branch name {name!r}: must match {_BRANCH_NAME_RE.pattern}. "
            "Use letters, digits, dot, dash or underscore — no '/' or other URL-special chars."
        )
        raise ValueError(msg)


def build_branch_name(dag_id: str, execution_date: str) -> str:
    """Construct a deterministic branch name for a DAG run.

    Args:
        dag_id: Airflow DAG id (already a valid identifier).
        execution_date: ``YYYY-MM-DD``.

    Returns:
        ``etl_<dag_id>_<execution_date_with_underscores>`` — safe to use as
        a Nessie branch name and as a Spark catalog ref.
    """
    safe_date = execution_date.replace("-", "_")
    return f"etl_{dag_id}_{safe_date}"


def create_branch(name: str, *, source_ref: str = "main") -> None:
    """Create ``name`` off ``source_ref``.

    Idempotent: if the branch already exists the call is a no-op + logs a
    warning. Useful when an Airflow task retries after a partial failure.

    Raises:
        NessieAPIError: On any other non-2xx response.
        ValueError: On a malformed branch name.
    """
    _validate_branch_name(name)

    if branch_exists(name):
        logger.warning("Nessie branch '%s' already exists — reusing it", name)
        return

    # v2 spec: POST /trees with body { "name", "type": "BRANCH", "sourceRefName" }
    # We hit GET on source to grab its hash, then POST with that hash so the
    # branch points at exactly the same commit we observed.
    src_hash = _ref_hash(source_ref)

    url = f"{_base_url()}/trees"
    payload = {"name": name, "type": "BRANCH", "hash": src_hash, "sourceRefName": source_ref}
    with _session() as s:
        resp = s.post(url, json=payload, timeout=_HTTP_TIMEOUT_SECONDS)
    _raise_for_status(resp, f"create branch '{name}' from '{source_ref}'")
    logger.info("Created Nessie branch '%s' off '%s' (hash=%s)", name, source_ref, src_hash[:8])


def drop_branch(name: str) -> None:
    """Delete ``name``. No-op + warn if it does not exist."""
    _validate_branch_name(name)

    if not branch_exists(name):
        logger.warning("Nessie branch '%s' does not exist — nothing to drop", name)
        return

    # v2 spec: DELETE /trees/{ref-key} with expected hash header.
    branch_hash = _ref_hash(name)
    url = f"{_base_url()}/trees/{name}@{branch_hash}"
    with _session() as s:
        resp = s.delete(url, timeout=_HTTP_TIMEOUT_SECONDS)
    _raise_for_status(resp, f"drop branch '{name}'")
    logger.info("Dropped Nessie branch '%s'", name)


def merge_branch(source: str, *, target: str = "main") -> None:
    """Merge ``source`` into ``target``.

    On success ``target`` advances; on conflict the API returns 409 and we
    raise :class:`NessieAPIError` — the caller is responsible for deciding
    whether to fail the run or rebase.
    """
    _validate_branch_name(source)
    _validate_branch_name(target)

    src_hash = _ref_hash(source)
    target_hash = _ref_hash(target)

    # v2 spec: POST /trees/{target}/merge
    url = f"{_base_url()}/trees/{target}@{target_hash}/merge"
    payload = {
        "fromRefName": source,
        "fromHash": src_hash,
        "defaultKeyMergeMode": "NORMAL",
    }
    with _session() as s:
        resp = s.post(url, json=payload, timeout=_HTTP_TIMEOUT_SECONDS)
    _raise_for_status(resp, f"merge '{source}' into '{target}'")
    logger.info("Merged Nessie branch '%s' into '%s'", source, target)


def branch_exists(name: str) -> bool:
    """``True`` when ``name`` resolves to a BRANCH ref on the server."""
    _validate_branch_name(name)
    url = f"{_base_url()}/trees/{name}"
    with _session() as s:
        resp = s.get(url, timeout=_HTTP_TIMEOUT_SECONDS)
    if resp.status_code == 404:
        return False
    _raise_for_status(resp, f"check branch '{name}'")
    data = resp.json()
    # v2 response wraps the reference under "reference".
    ref = data.get("reference") or data
    return ref.get("type") == "BRANCH"


def _ref_hash(name: str) -> str:
    """Resolve ``name`` to its current commit hash."""
    url = f"{_base_url()}/trees/{name}"
    with _session() as s:
        resp = s.get(url, timeout=_HTTP_TIMEOUT_SECONDS)
    _raise_for_status(resp, f"resolve hash for '{name}'")
    data = resp.json()
    ref = data.get("reference") or data
    h = ref.get("hash")
    if not h:
        msg = f"Nessie response for '{name}' missing 'hash' field: {data!r}"
        raise NessieAPIError(msg)
    return h


def _raise_for_status(resp: requests.Response, what: str) -> None:
    """Translate an HTTP failure into a NessieAPIError with context."""
    if 200 <= resp.status_code < 300:
        return

    body: object
    try:
        body = resp.json()
    except ValueError:
        body = resp.text

    msg = f"Nessie API call failed [{what}]: status={resp.status_code} body={body!r}"
    logger.error(msg)
    raise NessieAPIError(msg)
