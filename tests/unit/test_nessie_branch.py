"""Unit tests for ``src.utils.nessie_branch``.

The real Nessie server is mocked via ``responses`` — we don't want unit
tests to need a live container, and the value here is asserting the
**call shape** (right URL, right body, right headers) rather than
network round-trips.
"""

from __future__ import annotations

from unittest.mock import patch

import pytest
import responses

from src.utils.nessie_branch import (
    NessieAPIError,
    branch_exists,
    build_branch_name,
    create_branch,
    drop_branch,
    merge_branch,
)


@pytest.fixture
def fake_settings():
    """Pin the Nessie URI so the URLs we assert match exactly."""
    with patch("src.utils.nessie_branch.get_settings") as g:
        g.return_value.nessie_uri = "http://nessie:19120/api/v2"
        yield g.return_value


# ---------------------------------------------------------------------------
# build_branch_name
# ---------------------------------------------------------------------------


class TestBuildBranchName:
    def test_normalises_date(self):
        name = build_branch_name(dag_id="bronze_silver", execution_date="2026-04-29")
        assert name == "etl_bronze_silver_2026_04_29"

    def test_passes_validation(self):
        # Validation re-runs inside the API calls, so any name we build
        # here must also pass `_validate_branch_name`.
        assert build_branch_name("xyz", "2099-01-02") == "etl_xyz_2099_01_02"


# ---------------------------------------------------------------------------
# Name validation
# ---------------------------------------------------------------------------


class TestNameValidation:
    @pytest.mark.parametrize("bad", ["foo/bar", "with space", "tab\there", "back\\slash"])
    def test_rejects_url_special_characters(self, bad, fake_settings):
        with pytest.raises(ValueError, match="Invalid Nessie branch name"):
            create_branch(bad)


# ---------------------------------------------------------------------------
# create_branch
# ---------------------------------------------------------------------------


class TestCreateBranch:
    @responses.activate
    def test_creates_when_missing(self, fake_settings):
        # branch_exists check returns 404 -> doesn't exist
        responses.add(
            responses.GET,
            "http://nessie:19120/api/v2/trees/etl_x_2026_04_29",
            json={"reference": {"type": "BRANCH", "hash": "abc"}},
            status=404,
        )
        # GET source to resolve hash
        responses.add(
            responses.GET,
            "http://nessie:19120/api/v2/trees/main",
            json={"reference": {"type": "BRANCH", "hash": "mainHASH"}},
            status=200,
        )
        # POST creates the branch
        responses.add(
            responses.POST,
            "http://nessie:19120/api/v2/trees",
            json={"reference": {"type": "BRANCH", "name": "etl_x_2026_04_29", "hash": "abc"}},
            status=200,
        )

        create_branch("etl_x_2026_04_29", source_ref="main")

        post_call = next(c for c in responses.calls if c.request.method == "POST")
        # Nessie v2: new branch name + type go in the query string.
        assert "name=etl_x_2026_04_29" in post_call.request.url
        assert "type=BRANCH" in post_call.request.url
        # Body carries the source Reference (its own type + hash).
        body = post_call.request.body.decode()
        assert "BRANCH" in body
        assert "mainHASH" in body
        assert "main" in body  # source ref name in the Reference body

    @responses.activate
    def test_is_noop_when_branch_exists(self, fake_settings, caplog):
        # branch_exists -> 200 + BRANCH
        responses.add(
            responses.GET,
            "http://nessie:19120/api/v2/trees/etl_existing",
            json={"reference": {"type": "BRANCH", "hash": "abc"}},
            status=200,
        )

        create_branch("etl_existing", source_ref="main")

        # No POST was issued.
        assert all(c.request.method != "POST" for c in responses.calls)

    @responses.activate
    def test_raises_on_5xx(self, fake_settings):
        # branch_exists 404 -> proceed
        responses.add(
            responses.GET,
            "http://nessie:19120/api/v2/trees/etl_x",
            json={},
            status=404,
        )
        # GET source hash 200
        responses.add(
            responses.GET,
            "http://nessie:19120/api/v2/trees/main",
            json={"reference": {"type": "BRANCH", "hash": "h"}},
            status=200,
        )
        # POST returns 500
        responses.add(
            responses.POST,
            "http://nessie:19120/api/v2/trees",
            json={"error": "boom"},
            status=500,
        )

        with pytest.raises(NessieAPIError, match="status=500"):
            create_branch("etl_x", source_ref="main")


# ---------------------------------------------------------------------------
# drop_branch
# ---------------------------------------------------------------------------


class TestDropBranch:
    @responses.activate
    def test_drops_when_exists(self, fake_settings):
        # branch_exists -> exists
        responses.add(
            responses.GET,
            "http://nessie:19120/api/v2/trees/etl_x",
            json={"reference": {"type": "BRANCH", "hash": "h1"}},
            status=200,
        )
        # DELETE — 200 with a JSON body instead of 204 No-Content; the
        # `responses` library can't replay an empty 204 cleanly so we mock
        # what Nessie does in practice (returns the deleted ref).
        responses.add(
            responses.DELETE,
            "http://nessie:19120/api/v2/trees/etl_x@h1",
            json={"deleted": True},
            status=200,
        )

        drop_branch("etl_x")

        delete_call = next(c for c in responses.calls if c.request.method == "DELETE")
        assert "etl_x@h1" in delete_call.request.url

    @responses.activate
    def test_noop_when_missing(self, fake_settings):
        responses.add(
            responses.GET,
            "http://nessie:19120/api/v2/trees/etl_missing",
            json={},
            status=404,
        )

        drop_branch("etl_missing")

        assert all(c.request.method != "DELETE" for c in responses.calls)


# ---------------------------------------------------------------------------
# merge_branch
# ---------------------------------------------------------------------------


class TestMergeBranch:
    @responses.activate
    def test_merges_into_main(self, fake_settings):
        # GET source hash
        responses.add(
            responses.GET,
            "http://nessie:19120/api/v2/trees/etl_x",
            json={"reference": {"type": "BRANCH", "hash": "src"}},
            status=200,
        )
        # GET target hash
        responses.add(
            responses.GET,
            "http://nessie:19120/api/v2/trees/main",
            json={"reference": {"type": "BRANCH", "hash": "tgt"}},
            status=200,
        )
        responses.add(
            responses.POST,
            "http://nessie:19120/api/v2/trees/main@tgt/merge",
            json={"merged": True},
            status=200,
        )

        merge_branch("etl_x", target="main")

        post = next(c for c in responses.calls if c.request.method == "POST")
        assert "main@tgt/merge" in post.request.url
        assert "fromRefName" in post.request.body.decode()
        assert "etl_x" in post.request.body.decode()
        assert "src" in post.request.body.decode()  # fromHash

    @responses.activate
    def test_raises_on_conflict(self, fake_settings):
        responses.add(
            responses.GET,
            "http://nessie:19120/api/v2/trees/etl_x",
            json={"reference": {"type": "BRANCH", "hash": "src"}},
            status=200,
        )
        responses.add(
            responses.GET,
            "http://nessie:19120/api/v2/trees/main",
            json={"reference": {"type": "BRANCH", "hash": "tgt"}},
            status=200,
        )
        responses.add(
            responses.POST,
            "http://nessie:19120/api/v2/trees/main@tgt/merge",
            json={"reason": "conflict"},
            status=409,
        )

        with pytest.raises(NessieAPIError, match="status=409"):
            merge_branch("etl_x", target="main")


# ---------------------------------------------------------------------------
# branch_exists
# ---------------------------------------------------------------------------


class TestBranchExists:
    @responses.activate
    def test_true_for_branch(self, fake_settings):
        responses.add(
            responses.GET,
            "http://nessie:19120/api/v2/trees/etl_x",
            json={"reference": {"type": "BRANCH", "hash": "h"}},
            status=200,
        )
        assert branch_exists("etl_x") is True

    @responses.activate
    def test_false_for_404(self, fake_settings):
        responses.add(
            responses.GET,
            "http://nessie:19120/api/v2/trees/etl_x",
            json={},
            status=404,
        )
        assert branch_exists("etl_x") is False

    @responses.activate
    def test_false_for_tag(self, fake_settings):
        """A TAG ref is not a BRANCH — function should distinguish."""
        responses.add(
            responses.GET,
            "http://nessie:19120/api/v2/trees/etl_x",
            json={"reference": {"type": "TAG", "hash": "h"}},
            status=200,
        )
        assert branch_exists("etl_x") is False
