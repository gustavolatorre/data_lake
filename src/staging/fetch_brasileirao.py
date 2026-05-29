"""Staging layer — fetch Brasileirão Série A data from GE (Globo) internal API.

This module fetches match data from the GE's internal JSON endpoint
(the same one consumed by the browser), extracts match information
(home/away teams, score, date, stadium, broadcast), and stores raw JSON
files in the MinIO staging bucket, partitioned by date.

Idempotency contract:
- First run (MinIO empty): fetch ALL rounds (1-38), partition by match date,
  and upload one file per date (brasileirao/YYYY-MM-DD/matches.json).
  Only matches with date <= execution_date (D-1) are included.
- Incremental (data exists): fetch ALL rounds, filter matches for
  execution_date only, and overwrite the file for that date.
- Re-runs produce identical output (overwrite, never append).
"""

import io
import json
import logging

import requests
from minio import Minio
from minio.error import S3Error

from src.utils.minio_client import create_minio_client, ensure_bucket_exists

logger = logging.getLogger(__name__)

STAGING_BUCKET = "staging"
BASE_PREFIX = "brasileirao"

# GE internal API endpoint pattern for Brasileirão 2026
GE_API_BASE = (
    "https://api.globoesporte.globo.com/tabela/"
    "d1a37fa4-e948-43a6-ba53-ab24ab3a45b1/"
    "fase/fase-unica-campeonato-brasileiro-2026/"
    "rodada/{rodada}/jogos/"
)
TOTAL_ROUNDS = 38
REQUEST_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/125.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json",
}


def _check_is_first_run(client: Minio, bucket: str) -> bool:
    """Check if the brasileirao prefix is empty in MinIO (first run)."""
    try:
        for _ in client.list_objects(bucket, prefix=BASE_PREFIX + "/", recursive=True):
            return False  # Found at least one object — not first run
        return True  # No objects found — first run
    except S3Error:
        return True


def _fetch_all_matches() -> list[dict]:
    """Fetch matches from all 38 rounds of the GE internal API.

    Returns a flat list of normalized match dicts.
    """
    all_matches = []

    for rodada in range(1, TOTAL_ROUNDS + 1):
        url = GE_API_BASE.format(rodada=rodada)
        try:
            resp = requests.get(url, headers=REQUEST_HEADERS, timeout=15)
            resp.raise_for_status()
            jogos = resp.json()
        except requests.RequestException as e:
            logger.warning("Failed to fetch round %d: %s", rodada, e)
            continue

        for jogo in jogos:
            match = _normalize_match(jogo, rodada)
            if match:
                all_matches.append(match)

        logger.debug("Round %d: fetched %d matches", rodada, len(jogos))

    logger.info("Total matches fetched from GE API: %d", len(all_matches))
    return all_matches


def _normalize_match(jogo: dict, rodada: int) -> dict | None:
    """Normalize a single match JSON from the GE API into our standard schema.

    Preserves home/away order exactly as returned by the source (mandante first).
    """
    try:
        data_str = jogo.get("data_realizacao", "")
        if not data_str:
            return None

        # data_realizacao is like "2026-01-28T19:00"
        match_date = data_str[:10]  # "2026-01-28"

        equipes = jogo.get("equipes", {})
        mandante = equipes.get("mandante", {})
        visitante = equipes.get("visitante", {})

        sede = jogo.get("sede") or {}
        transmissao = jogo.get("transmissao") or {}
        broadcast_info = transmissao.get("broadcast") or {}

        # Determine broadcast channel from label/url
        broadcast_label = broadcast_info.get("label", "")
        broadcast_url = transmissao.get("url", "")

        return {
            "matchweek": rodada,
            "home_team": mandante.get("nome_popular", "Unknown"),
            "home_team_code": mandante.get("sigla", ""),
            "away_team": visitante.get("nome_popular", "Unknown"),
            "away_team_code": visitante.get("sigla", ""),
            "score_home": jogo.get("placar_oficial_mandante"),
            "score_away": jogo.get("placar_oficial_visitante"),
            "date": match_date,
            "kickoff_time": jogo.get("hora_realizacao", ""),
            "stadium": sede.get("nome_popular", "Unknown"),
            "broadcast": broadcast_label,
            "match_url": broadcast_url,
            "match_started": jogo.get("jogo_ja_comecou", False),
            "source": "ge.globo.com",
            "ge_match_id": jogo.get("id"),
        }
    except (KeyError, TypeError) as e:
        logger.warning("Failed to normalize match: %s", e)
        return None


def fetch_and_upload(execution_date_str: str) -> int:
    """Fetch Brasileirão matches and upload to MinIO staging bucket.

    Logic:
    - If first run (no data in MinIO): Fetch all matches up to D-1 (execution_date)
      and partition them by date (e.g. brasileirao/YYYY-MM-DD/matches.json).
    - If incremental (data exists): Fetch matches only for D-1 (execution_date)
      and upload to its respective date folder (overwriting if it already exists).

    Args:
        execution_date_str: Date string (YYYY-MM-DD) from Airflow's ``ds``.

    Returns:
        Total number of records fetched and uploaded.
    """
    client = create_minio_client()
    ensure_bucket_exists(client, STAGING_BUCKET)

    is_first_run = _check_is_first_run(client, STAGING_BUCKET)

    logger.info(
        "Starting Brasileirão ingestion for date=%s (First Run: %s)",
        execution_date_str,
        is_first_run,
    )

    # Fetch all matches from the GE API (all 38 rounds)
    all_matches = _fetch_all_matches()

    if not all_matches:
        logger.warning("No matches returned from GE API. Aborting upload.")
        return 0

    # Filter only finished matches (have a score set)
    finished_matches = [m for m in all_matches if m.get("score_home") is not None and m.get("match_started") is True]
    logger.info("Finished matches available: %d out of %d total", len(finished_matches), len(all_matches))

    total_uploaded = 0

    if is_first_run:
        logger.info("First run detected: Partitioning all historical matches up to %s by date.", execution_date_str)

        # Group finished matches by date, filtering up to D-1 (execution_date)
        grouped: dict[str, list[dict]] = {}
        for m in finished_matches:
            match_date = m.get("date", "")
            if match_date and match_date <= execution_date_str:
                grouped.setdefault(match_date, []).append(m)

        logger.info("Historical dates to upload: %d", len(grouped))

        for match_date in sorted(grouped.keys()):
            day_matches = grouped[match_date]
            object_name = f"{BASE_PREFIX}/{match_date}/matches.json"
            _upload_json(client, STAGING_BUCKET, object_name, day_matches)
            total_uploaded += len(day_matches)
            logger.info("Uploaded %d records to %s", len(day_matches), object_name)

    else:
        # Incremental run: filter for execution_date (D-1) only
        target_date = execution_date_str
        day_matches = [m for m in finished_matches if m.get("date") == target_date]
        logger.info("Incremental run: Filtered %d matches for date %s", len(day_matches), target_date)

        if day_matches:
            object_name = f"{BASE_PREFIX}/{target_date}/matches.json"
            _upload_json(client, STAGING_BUCKET, object_name, day_matches)
            total_uploaded = len(day_matches)
            logger.info("Uploaded %d records to %s", total_uploaded, object_name)
        else:
            logger.info("No matches found for date=%s (no games on this day)", target_date)

    return total_uploaded


def _upload_json(
    client: Minio,
    bucket: str,
    object_name: str,
    data: list[dict],
) -> None:
    """Upload JSON data to MinIO (idempotent overwrite)."""
    json_bytes = json.dumps(data, ensure_ascii=False, indent=2).encode("utf-8")
    file_obj = io.BytesIO(json_bytes)

    client.put_object(
        bucket_name=bucket,
        object_name=object_name,
        data=file_obj,
        length=len(json_bytes),
        content_type="application/json",
    )
