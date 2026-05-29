"""Unit tests for ``src.utils.quality_runner``.

The runner reads YAML rules and executes each one against a DataFrame.
We avoid spinning up Spark by stubbing only the calls the runner reaches:
``df.count()``, ``df.filter(...).count()``, and a small chain on
``df.select(...).distinct().count()``.

A real Spark DataFrame test fits in ``test_data_quality.py`` if/when we
want a full integration; this file focuses on the dispatch logic, severity
handling, and YAML loading.
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from src.utils.quality_runner import (
    SEVERITY_WARN,
    QualityCheckError,
    run_quality_checks,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def checks_file(tmp_path: Path) -> Path:
    """Yield a temp directory + a helper to drop a YAML file into it."""
    return tmp_path


def _write_yaml(directory: Path, name: str, content: str) -> None:
    (directory / name).write_text(content, encoding="utf-8")


def _df_with_counts(
    total: int = 10,
    null_counts: dict[str, int] | None = None,
    distinct_counts: dict[str, int] | None = None,
) -> MagicMock:
    """Build a DataFrame mock that responds to the runner's call shape.

    Args:
        total: value returned by ``df.count()`` for table-level checks.
        null_counts: per-column overrides for ``df.filter(col.isNull()).count()``.
        distinct_counts: per-column overrides for ``df.filter(col.isNotNull()).select(col).distinct().count()``.
    """
    null_counts = null_counts or {}
    distinct_counts = distinct_counts or {}

    df = MagicMock(name="DataFrame")
    df.count.return_value = total

    # F.col(name).isNull() returns a Column-like that filter() uses as a
    # predicate; we don't model it — just stub the chained .count() result
    # based on which column was passed.
    null_filtered = MagicMock(name="null_filtered_df")
    not_null_filtered = MagicMock(name="not_null_filtered_df")

    def _filter_side_effect(_predicate):
        col_name = _last_column_name.value
        is_null_path = _last_is_null.value
        if is_null_path:
            null_filtered.count.return_value = null_counts.get(col_name, 0)
            return null_filtered
        not_null_filtered.count.return_value = total - null_counts.get(col_name, 0)
        distinct_chain = MagicMock(name="distinct_chain")
        distinct_chain.distinct.return_value.count.return_value = distinct_counts.get(
            col_name, total - null_counts.get(col_name, 0)
        )
        not_null_filtered.select.return_value = distinct_chain
        return not_null_filtered

    df.filter.side_effect = _filter_side_effect
    return df


# Shared mutable holders used so the side_effect closures can read what
# column / sense was last asked for. pytest does not provide a clean way to
# do this without globals or fixtures-of-fixtures; the indirection is local
# to this file.
class _Holder:
    def __init__(self, value=None):
        self.value = value


_last_column_name = _Holder()
_last_is_null = _Holder()


@pytest.fixture(autouse=True)
def _reset_holders():
    _last_column_name.value = None
    _last_is_null.value = False
    yield


# ---------------------------------------------------------------------------
# YAML loading
# ---------------------------------------------------------------------------


class TestYamlLoading:
    def test_missing_file_raises(self, checks_file: Path):
        df = _df_with_counts()
        with pytest.raises(FileNotFoundError, match="not found"):
            run_quality_checks(df, "does_not_exist.yml", checks_dir=checks_file)

    def test_invalid_yaml_structure_raises(self, checks_file: Path):
        _write_yaml(checks_file, "bad.yml", "this_is_not_checks: [a, b]")
        df = _df_with_counts()
        with pytest.raises(ValueError, match="expected a top-level 'checks' list"):
            run_quality_checks(df, "bad.yml", checks_dir=checks_file)

    def test_checks_must_be_a_list(self, checks_file: Path):
        _write_yaml(checks_file, "bad.yml", "checks: not_a_list")
        df = _df_with_counts()
        with pytest.raises(ValueError, match="must be a list"):
            run_quality_checks(df, "bad.yml", checks_dir=checks_file)


# ---------------------------------------------------------------------------
# Row count rule
# ---------------------------------------------------------------------------


class TestRowCountRule:
    def test_passes_when_above_min(self, checks_file: Path):
        _write_yaml(
            checks_file,
            "r.yml",
            "dataset: t\nchecks:\n  - name: must_have_rows\n    type: row_count\n    min: 1\n",
        )
        df = _df_with_counts(total=5)

        results = run_quality_checks(df, "r.yml", checks_dir=checks_file)

        assert len(results) == 1
        assert results[0].passed
        assert results[0].actual == 5

    def test_fails_when_below_min(self, checks_file: Path):
        _write_yaml(
            checks_file,
            "r.yml",
            "dataset: t\nchecks:\n  - name: must_have_rows\n    type: row_count\n    min: 10\n",
        )
        df = _df_with_counts(total=5)

        with pytest.raises(QualityCheckError, match="must_have_rows"):
            run_quality_checks(df, "r.yml", checks_dir=checks_file)


# ---------------------------------------------------------------------------
# Missing-count rule
# ---------------------------------------------------------------------------


class TestMissingCountRule:
    @patch("src.utils.quality_runner.F")
    def test_passes_when_zero_nulls(self, mock_f, checks_file: Path):
        _write_yaml(
            checks_file,
            "r.yml",
            "dataset: t\nchecks:\n  - name: id_no_nulls\n    type: missing_count\n    column: id\n    max: 0\n",
        )

        # Wire the chained isNull predicate so the side_effect sees what
        # column was asked for.
        def _col(name):
            _last_column_name.value = name
            col = MagicMock()
            col.isNull.side_effect = lambda: (_setattr_is_null(True),)[0]
            col.isNotNull.side_effect = lambda: (_setattr_is_null(False),)[0]
            return col

        mock_f.col.side_effect = _col

        df = _df_with_counts(total=5, null_counts={"id": 0})
        results = run_quality_checks(df, "r.yml", checks_dir=checks_file)

        assert results[0].passed
        assert results[0].actual == 0

    @patch("src.utils.quality_runner.F")
    def test_fails_when_nulls_present(self, mock_f, checks_file: Path):
        _write_yaml(
            checks_file,
            "r.yml",
            "dataset: t\nchecks:\n  - name: id_no_nulls\n    type: missing_count\n    column: id\n    max: 0\n",
        )

        def _col(name):
            _last_column_name.value = name
            col = MagicMock()
            col.isNull.side_effect = lambda: (_setattr_is_null(True),)[0]
            col.isNotNull.side_effect = lambda: (_setattr_is_null(False),)[0]
            return col

        mock_f.col.side_effect = _col

        df = _df_with_counts(total=5, null_counts={"id": 2})
        with pytest.raises(QualityCheckError, match="id_no_nulls"):
            run_quality_checks(df, "r.yml", checks_dir=checks_file)


# ---------------------------------------------------------------------------
# Missing-percent rule
# ---------------------------------------------------------------------------


class TestMissingPercentRule:
    @patch("src.utils.quality_runner.F")
    def test_warn_severity_does_not_raise(self, mock_f, checks_file: Path):
        """A `warn` violation must NOT raise, even when the rule failed."""
        _write_yaml(
            checks_file,
            "r.yml",
            (
                "dataset: t\n"
                "checks:\n"
                "  - name: name_rarely_missing\n"
                "    type: missing_percent\n"
                "    column: name\n"
                "    max_percent: 1.0\n"
                "    severity: warn\n"
            ),
        )

        def _col(name):
            _last_column_name.value = name
            col = MagicMock()
            col.isNull.side_effect = lambda: (_setattr_is_null(True),)[0]
            return col

        mock_f.col.side_effect = _col

        # 10% missing on a 1% threshold — would FAIL if severity=fail.
        df = _df_with_counts(total=100, null_counts={"name": 10})
        results = run_quality_checks(df, "r.yml", checks_dir=checks_file)

        assert not results[0].passed
        assert results[0].severity == SEVERITY_WARN
        # No raise — warn does not abort.

    def test_empty_df_passes(self, checks_file: Path):
        _write_yaml(
            checks_file,
            "r.yml",
            (
                "dataset: t\n"
                "checks:\n"
                "  - name: any_missing\n"
                "    type: missing_percent\n"
                "    column: name\n"
                "    max_percent: 0\n"
            ),
        )
        df = _df_with_counts(total=0)

        results = run_quality_checks(df, "r.yml", checks_dir=checks_file)

        assert results[0].passed  # trivially passes on empty
        assert results[0].actual == 0.0


# ---------------------------------------------------------------------------
# Unknown rule type + missing required field
# ---------------------------------------------------------------------------


class TestRuleDispatch:
    def test_unknown_rule_type_is_treated_as_fail(self, checks_file: Path):
        _write_yaml(
            checks_file,
            "r.yml",
            "dataset: t\nchecks:\n  - name: bogus\n    type: not_a_real_rule\n",
        )
        df = _df_with_counts()

        # Default severity = fail; an unknown rule type means we couldn't
        # validate, so the runner returns a failed CheckResult AND raises.
        with pytest.raises(QualityCheckError):
            run_quality_checks(df, "r.yml", checks_dir=checks_file)

    def test_missing_column_field_raises_value_error(self, checks_file: Path):
        _write_yaml(
            checks_file,
            "r.yml",
            "dataset: t\nchecks:\n  - name: bad\n    type: missing_count\n    max: 0\n",
        )
        df = _df_with_counts()
        with pytest.raises(ValueError, match="missing required field 'column'"):
            run_quality_checks(df, "r.yml", checks_dir=checks_file)


# ---------------------------------------------------------------------------
# Bundled rule file shape (sanity / freeze)
# ---------------------------------------------------------------------------


class TestBundledChecksFile:
    """Guarantees on the YAML we ship under quality/checks/."""

    def test_bronze_breweries_yaml_is_loadable_and_has_required_keys(self):
        import yaml

        bundled = Path(__file__).resolve().parents[2] / "quality" / "checks" / "bronze_breweries.yml"
        assert bundled.exists(), "shipped bronze rules file should exist"

        data = yaml.safe_load(bundled.read_text(encoding="utf-8"))
        assert data["dataset"] == "bronze.breweries"
        assert isinstance(data["checks"], list)
        assert len(data["checks"]) >= 3

        # Stable names — downstream alerting filters on these.
        names = {r["name"] for r in data["checks"]}
        assert {"bronze_must_not_be_empty", "bronze_id_no_nulls", "bronze_id_unique_per_partition"} <= names


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _setattr_is_null(flag: bool):
    _last_is_null.value = flag
    return MagicMock()
