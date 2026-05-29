"""Unit tests for Brasileirão Staging layer — API fetch and MinIO upload."""

import json
from unittest.mock import MagicMock, patch

import pytest
import requests
import responses
from minio.error import S3Error

from src.staging.fetch_brasileirao import (
    GE_API_BASE,
    _check_is_first_run,
    _fetch_all_matches,
    _normalize_match,
    fetch_and_upload,
)


class TestNormalizeMatch:
    """Tests for the _normalize_match function."""

    def test_valid_match(self):
        """Should parse a valid GE JSON match perfectly."""
        mock_ge_json = {
            "data_realizacao": "2026-05-28T19:00",
            "hora_realizacao": "19:00",
            "equipes": {
                "mandante": {"nome_popular": "Vasco", "sigla": "VAS"},
                "visitante": {"nome_popular": "Palmeiras", "sigla": "PAL"}
            },
            "placar_oficial_mandante": 1,
            "placar_oficial_visitante": 0,
            "sede": {"nome_popular": "São Januário"},
            "jogo_ja_comecou": True,
            "id": 12345
        }
        
        result = _normalize_match(mock_ge_json, rodada=10)
        
        assert result is not None
        assert result["matchweek"] == 10
        assert result["home_team"] == "Vasco"
        assert result["home_team_code"] == "VAS"
        assert result["away_team"] == "Palmeiras"
        assert result["away_team_code"] == "PAL"
        assert result["score_home"] == 1
        assert result["score_away"] == 0
        assert result["date"] == "2026-05-28"
        assert result["kickoff_time"] == "19:00"
        assert result["stadium"] == "São Januário"
        assert result["ge_match_id"] == 12345
        assert result["match_started"] is True

    def test_missing_date(self):
        """Should return None if data_realizacao is missing."""
        mock_ge_json = {"equipes": {"mandante": {}, "visitante": {}}}
        result = _normalize_match(mock_ge_json, rodada=1)
        assert result is None

    def test_no_score_yet(self):
        """Should handle matches that haven't started (None scores)."""
        mock_ge_json = {
            "data_realizacao": "2026-06-01T16:00",
            "hora_realizacao": "16:00",
            "equipes": {
                "mandante": {"nome_popular": "Galo", "sigla": "CAM"},
                "visitante": {"nome_popular": "Bahia", "sigla": "BAH"}
            },
            "placar_oficial_mandante": None,
            "placar_oficial_visitante": None,
            "sede": {"nome_popular": "Arena MRV"},
            "jogo_ja_comecou": False,
            "id": 999
        }
        
        result = _normalize_match(mock_ge_json, rodada=12)
        assert result is not None
        assert result["score_home"] is None
        assert result["score_away"] is None


class TestFetchAllMatches:
    """Tests for the _fetch_all_matches function."""

    @responses.activate
    @patch("src.staging.fetch_brasileirao.TOTAL_ROUNDS", 2)
    def test_fetch_all_rounds_success(self):
        """Should iterate through rounds and accumulate matches."""
        # Round 1 data
        responses.add(
            responses.GET,
            GE_API_BASE.format(rodada=1),
            json=[{
                "data_realizacao": "2026-04-10T16:00",
                "equipes": {"mandante": {"nome_popular": "A"}, "visitante": {"nome_popular": "B"}}
            }],
            status=200
        )
        # Round 2 data
        responses.add(
            responses.GET,
            GE_API_BASE.format(rodada=2),
            json=[{
                "data_realizacao": "2026-04-17T16:00",
                "equipes": {"mandante": {"nome_popular": "C"}, "visitante": {"nome_popular": "D"}}
            }],
            status=200
        )

        matches = _fetch_all_matches()
        assert len(matches) == 2
        assert matches[0]["home_team"] == "A"
        assert matches[1]["home_team"] == "C"

    @responses.activate
    @patch("src.staging.fetch_brasileirao.TOTAL_ROUNDS", 1)
    def test_http_error_handling(self):
        """Should gracefully ignore a round if the HTTP request fails."""
        responses.add(
            responses.GET,
            GE_API_BASE.format(rodada=1),
            json={"error": "Not found"},
            status=404
        )
        
        matches = _fetch_all_matches()
        assert len(matches) == 0


class TestCheckIsFirstRun:
    """Tests for the _check_is_first_run function."""

    def test_empty_bucket(self):
        """Should return True when list_objects is empty."""
        client = MagicMock()
        client.list_objects.return_value = iter([])
        assert _check_is_first_run(client, "staging") is True

    def test_populated_bucket(self):
        """Should return False when list_objects yields items."""
        client = MagicMock()
        client.list_objects.return_value = iter([MagicMock()])
        assert _check_is_first_run(client, "staging") is False

    def test_s3_error(self):
        """Should return True if list_objects throws S3Error (e.g. bucket/prefix not found)."""
        client = MagicMock()
        client.list_objects.side_effect = S3Error("code", "msg", "res", "req", "host", "resp")
        assert _check_is_first_run(client, "staging") is True


class TestFetchAndUpload:
    """Tests for the main fetch_and_upload orchestration function."""

    @patch("src.staging.fetch_brasileirao.create_minio_client")
    @patch("src.staging.fetch_brasileirao.ensure_bucket_exists")
    @patch("src.staging.fetch_brasileirao._check_is_first_run")
    @patch("src.staging.fetch_brasileirao._fetch_all_matches")
    def test_first_run_uploads_all_past_dates(self, mock_fetch, mock_first_run, mock_ensure, mock_client):
        """First run should group by date and upload multiple files (only <= D-1)."""
        mock_first_run.return_value = True
        mock_minio = MagicMock()
        mock_client.return_value = mock_minio
        
        # Matches from 3 different dates, plus one future date (2026-05-30)
        mock_fetch.return_value = [
            {"date": "2026-05-20", "home_team": "A", "score_home": 1, "match_started": True},
            {"date": "2026-05-20", "home_team": "B", "score_home": 1, "match_started": True},
            {"date": "2026-05-21", "home_team": "C", "score_home": 1, "match_started": True},
            {"date": "2026-05-30", "home_team": "D", "score_home": 1, "match_started": True}, # Future
        ]

        total = fetch_and_upload("2026-05-29")
        
        assert total == 3  # Excludes the future match
        assert mock_minio.put_object.call_count == 2 # 1 for 05-20, 1 for 05-21

    @patch("src.staging.fetch_brasileirao.create_minio_client")
    @patch("src.staging.fetch_brasileirao.ensure_bucket_exists")
    @patch("src.staging.fetch_brasileirao._check_is_first_run")
    @patch("src.staging.fetch_brasileirao._fetch_all_matches")
    def test_incremental_run_uploads_only_execution_date(self, mock_fetch, mock_first_run, mock_ensure, mock_client):
        """Incremental run should strictly filter matches by execution_date."""
        mock_first_run.return_value = False
        mock_minio = MagicMock()
        mock_client.return_value = mock_minio
        
        # Matches from 3 different dates
        mock_fetch.return_value = [
            {"date": "2026-05-28", "home_team": "A", "score_home": 1, "match_started": True}, # Match D-1
            {"date": "2026-05-28", "home_team": "B", "score_home": 1, "match_started": True}, # Match D-1
            {"date": "2026-05-27", "home_team": "C", "score_home": 1, "match_started": True}, # Past match
            {"date": "2026-05-29", "home_team": "D", "score_home": 1, "match_started": True}, # Future match
        ]

        # Execution date is 2026-05-28
        total = fetch_and_upload("2026-05-28")
        
        assert total == 2
        assert mock_minio.put_object.call_count == 1
        
        # Verify it uploaded to the correct date prefix
        call_args = mock_minio.put_object.call_args.kwargs
        assert call_args["object_name"] == "brasileirao/2026-05-28/matches.json"
