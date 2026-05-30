"""Unit tests for ``src.silver.stadium_enrichment``.

The module is essentially two static dicts plus a tiny helper that
materializes them as Spark DataFrames. Tests cover the structural contract
(every UF is 2 letters, every key is non-empty) and the DataFrame factory
(right columns, right counts).
"""

import re

import pytest

from src.silver.stadium_enrichment import (
    _ACCENTS,
    _PLAIN,
    HOME_TEAM_TO_STATE,
    ORIGIN_HOME_TEAM,
    ORIGIN_STADIUM,
    ORIGIN_UNKNOWN,
    SENTINEL_STATE,
    STADIUM_TO_STATE,
    _strip_accents,
    build_lookup_frames,
)

# Two-letter UF (Unidade Federativa) — 26 estados + DF.
_VALID_UFS = {
    "AC",
    "AL",
    "AP",
    "AM",
    "BA",
    "CE",
    "DF",
    "ES",
    "GO",
    "MA",
    "MT",
    "MS",
    "MG",
    "PA",
    "PB",
    "PR",
    "PE",
    "PI",
    "RJ",
    "RN",
    "RS",
    "RO",
    "RR",
    "SC",
    "SP",
    "SE",
    "TO",
}


class TestSeedShape:
    """Both seed dicts must be non-empty and only map to valid UF codes."""

    @pytest.mark.parametrize("seed", [STADIUM_TO_STATE, HOME_TEAM_TO_STATE])
    def test_seed_is_non_empty(self, seed):
        assert len(seed) > 0, "seed dict is empty"

    @pytest.mark.parametrize("seed", [STADIUM_TO_STATE, HOME_TEAM_TO_STATE])
    def test_keys_are_non_empty_strings(self, seed):
        for k in seed:
            assert isinstance(k, str) and k.strip(), f"invalid key: {k!r}"

    @pytest.mark.parametrize("seed", [STADIUM_TO_STATE, HOME_TEAM_TO_STATE])
    def test_values_are_valid_ufs(self, seed):
        for k, v in seed.items():
            assert v in _VALID_UFS, f"key {k!r} maps to invalid UF {v!r}"

    @pytest.mark.parametrize("seed", [STADIUM_TO_STATE, HOME_TEAM_TO_STATE])
    def test_uf_format_is_two_uppercase_letters(self, seed):
        for k, v in seed.items():
            assert re.fullmatch(r"[A-Z]{2}", v), f"key {k!r} → {v!r} not a 2-letter uppercase UF"


class TestSerieACoverage:
    """Sanity check that the most-traveled keys are present.

    Failing here means the seed regressed (typo, accidental deletion). The
    specific keys are the ones with the highest match volume in Série A —
    if any of these is missing, the UNKNOWN bucket in production
    immediately spikes.
    """

    @pytest.mark.parametrize(
        "stadium,expected_uf",
        [
            ("Maracanã", "RJ"),
            ("Allianz Parque", "SP"),
            ("Neo Química Arena", "SP"),
            ("Mineirão", "MG"),
            ("Arena MRV", "MG"),
            ("Beira-Rio", "RS"),
            ("Arena do Grêmio", "RS"),
            ("Couto Pereira", "PR"),
            ("Arena Fonte Nova", "BA"),
            ("Castelão", "CE"),
            ("Arena Pantanal", "MT"),
            ("Mané Garrincha", "DF"),
        ],
    )
    def test_major_stadiums_mapped(self, stadium, expected_uf):
        assert STADIUM_TO_STATE.get(stadium) == expected_uf

    @pytest.mark.parametrize(
        "team,expected_uf",
        [
            ("Flamengo", "RJ"),
            ("Palmeiras", "SP"),
            ("São Paulo", "SP"),
            ("Corinthians", "SP"),
            ("Santos", "SP"),
            ("Atlético-MG", "MG"),
            ("Cruzeiro", "MG"),
            ("Internacional", "RS"),
            ("Grêmio", "RS"),
            ("Bahia", "BA"),
            ("Fortaleza", "CE"),
            ("Athletico-PR", "PR"),
        ],
    )
    def test_major_teams_mapped(self, team, expected_uf):
        assert HOME_TEAM_TO_STATE.get(team) == expected_uf


class TestConstants:
    """Origin / sentinel constants are stable strings — Gold filters on them."""

    def test_origin_constants_distinct(self):
        assert len({ORIGIN_STADIUM, ORIGIN_HOME_TEAM, ORIGIN_UNKNOWN}) == 3

    def test_sentinel_state_value(self):
        # Documenting the contract: 'UNKNOWN' (uppercase) is what downstream
        # filters / dashboards count when measuring enrichment coverage.
        assert SENTINEL_STATE == "UNKNOWN"


class TestBuildLookupFrames:
    """``build_lookup_frames`` must return two DataFrames with the right
    schema and row count, ready for ``F.broadcast()`` joins.
    """

    def test_returns_two_dataframes(self, spark):
        stadiums, teams = build_lookup_frames(spark)
        assert stadiums is not None
        assert teams is not None

    def test_stadiums_schema(self, spark):
        stadiums, _ = build_lookup_frames(spark)
        assert set(stadiums.columns) == {"_lookup_stadium", "_lookup_stadium_state"}

    def test_teams_schema(self, spark):
        _, teams = build_lookup_frames(spark)
        assert set(teams.columns) == {"_lookup_home_team", "_lookup_home_team_state"}

    def test_stadium_row_count_matches_dict(self, spark):
        stadiums, _ = build_lookup_frames(spark)
        assert stadiums.count() == len(STADIUM_TO_STATE)

    def test_team_row_count_matches_dict(self, spark):
        _, teams = build_lookup_frames(spark)
        assert teams.count() == len(HOME_TEAM_TO_STATE)

    def test_stadium_lookup_keys_are_accent_stripped(self, spark):
        """Lookup keys must match what the Silver transform produces
        (which runs ``F.translate`` to strip accents). Otherwise every
        join lands in UNKNOWN.
        """
        stadiums, _ = build_lookup_frames(spark)
        rows = {row["_lookup_stadium"]: row["_lookup_stadium_state"] for row in stadiums.collect()}
        # "Maracanã" in the dict → "Maracana" in the materialized frame.
        assert "Maracana" in rows
        assert rows["Maracana"] == "RJ"
        # ASCII-only keys round-trip unchanged.
        assert rows["Allianz Parque"] == "SP"
        # The accented form must NOT survive the materialization.
        assert "Maracanã" not in rows

    def test_team_lookup_keys_are_accent_stripped(self, spark):
        _, teams = build_lookup_frames(spark)
        rows = {row["_lookup_home_team"]: row["_lookup_home_team_state"] for row in teams.collect()}
        # ASCII-only key round-trips unchanged.
        assert rows["Flamengo"] == "RJ"
        # "Atlético-MG" in the dict → "Atletico-MG" in the materialized frame.
        assert "Atletico-MG" in rows
        assert rows["Atletico-MG"] == "MG"
        assert "Atlético-MG" not in rows


class TestStripAccents:
    """The helper that normalizes dict keys at materialization time."""

    @pytest.mark.parametrize(
        "raw,normalized",
        [
            ("Maracanã", "Maracana"),
            ("São Paulo", "Sao Paulo"),
            ("Atlético-MG", "Atletico-MG"),
            ("Grêmio", "Gremio"),
            ("Cuiabá", "Cuiaba"),
            ("Allianz Parque", "Allianz Parque"),  # already ASCII
            ("", ""),  # empty string is a no-op
        ],
    )
    def test_handles_known_cases(self, raw, normalized):
        assert _strip_accents(raw) == normalized

    def test_accent_map_matches_transform_module(self):
        """Smoke test: the constants here MUST stay in lockstep with
        ``transform_brasileirao._ACCENTS`` / ``_PLAIN``. Divergence would
        silently break the enrichment join (different normalized strings
        on each side of the equality predicate).
        """
        from src.silver import transform_brasileirao as tb

        assert _ACCENTS == tb._ACCENTS
        assert _PLAIN == tb._PLAIN
