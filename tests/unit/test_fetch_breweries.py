"""Unit tests for Bronze layer — API fetch and upload."""

import json
from unittest.mock import MagicMock, patch

import pytest
import responses

from src.bronze.fetch_breweries import _fetch_page, _upload_json, fetch_and_upload


class TestFetchPage:
    """Tests for the _fetch_page function."""

    @responses.activate
    def test_fetch_page_returns_data(self):
        """Should return a list of breweries for a valid page."""
        mock_data = [{"id": "1", "name": "Test Brewery"}]
        responses.add(
            responses.GET,
            "https://api.openbrewerydb.org/v1/breweries",
            json=mock_data,
            status=200,
        )

        result = _fetch_page(
            base_url="https://api.openbrewerydb.org/v1/breweries",
            page=1,
            per_page=50,
            timeout=10,
        )

        assert result == mock_data
        assert len(result) == 1

    @responses.activate
    def test_fetch_page_empty_response(self):
        """Should return empty list when API has no more data."""
        responses.add(
            responses.GET,
            "https://api.openbrewerydb.org/v1/breweries",
            json=[],
            status=200,
        )

        result = _fetch_page(
            base_url="https://api.openbrewerydb.org/v1/breweries",
            page=999,
            per_page=50,
            timeout=10,
        )

        assert result == []

    @responses.activate
    def test_fetch_page_invalid_response_raises_value_error(self):
        """Should raise ValueError if response is not a list."""
        responses.add(
            responses.GET,
            "https://api.openbrewerydb.org/v1/breweries",
            json={"error": "not found"},
            status=200,
        )

        with pytest.raises(ValueError, match="expected list"):
            _fetch_page(
                base_url="https://api.openbrewerydb.org/v1/breweries",
                page=1,
                per_page=50,
                timeout=10,
            )

    @responses.activate
    def test_fetch_page_http_error_raises(self):
        """Should raise HTTPError on non-2xx status."""
        responses.add(
            responses.GET,
            "https://api.openbrewerydb.org/v1/breweries",
            json={"error": "server error"},
            status=500,
        )

        with pytest.raises(Exception):
            _fetch_page(
                base_url="https://api.openbrewerydb.org/v1/breweries",
                page=1,
                per_page=50,
                timeout=10,
            )


class TestUploadJson:
    """Tests for the _upload_json function."""

    def test_upload_json_calls_put_object(self):
        """Should call put_object with correct parameters."""
        mock_client = MagicMock()
        data = [{"id": "1", "name": "Test"}]

        _upload_json(mock_client, "bronze", "test/file.json", data)

        mock_client.put_object.assert_called_once()
        call_args = mock_client.put_object.call_args
        assert call_args.kwargs["bucket_name"] == "bronze"
        assert call_args.kwargs["object_name"] == "test/file.json"
        assert call_args.kwargs["content_type"] == "application/json"


class TestFetchAndUpload:
    """Tests for the main fetch_and_upload function."""

    @responses.activate
    @patch("src.bronze.fetch_breweries.create_minio_client")
    @patch("src.bronze.fetch_breweries.ensure_bucket_exists")
    def test_fetch_and_upload_two_pages(self, mock_ensure, mock_client):
        """Should fetch all pages and return total record count."""
        page1 = [{"id": str(i), "name": f"Brewery {i}"} for i in range(50)]
        page2 = [{"id": str(i), "name": f"Brewery {i}"} for i in range(50, 60)]

        responses.add(responses.GET, "https://api.openbrewerydb.org/v1/breweries", json=page1, status=200)
        responses.add(responses.GET, "https://api.openbrewerydb.org/v1/breweries", json=page2, status=200)
        responses.add(responses.GET, "https://api.openbrewerydb.org/v1/breweries", json=[], status=200)

        mock_minio = MagicMock()
        mock_client.return_value = mock_minio

        total = fetch_and_upload("2026-04-29")

        assert total == 60
        assert mock_minio.put_object.call_count == 2

    @responses.activate
    @patch("src.bronze.fetch_breweries.create_minio_client")
    @patch("src.bronze.fetch_breweries.ensure_bucket_exists")
    def test_fetch_and_upload_empty_api(self, mock_ensure, mock_client):
        """Should return 0 when API returns empty on first page."""
        responses.add(responses.GET, "https://api.openbrewerydb.org/v1/breweries", json=[], status=200)

        mock_minio = MagicMock()
        mock_client.return_value = mock_minio

        total = fetch_and_upload("2026-04-29")

        assert total == 0
        mock_minio.put_object.assert_not_called()
