"""Static validation tests for Airflow DAG files.

These tests parse the DAG files as Python AST without importing Airflow,
so they run in any environment that has Python — no Airflow install needed.
They catch the most common DAG bugs: syntax errors, missing @dag decorators,
missing asset declarations, and missing pipeline instantiation.

For full DagBag-based validation (which requires Airflow installed),
see tests/integration/ when that suite is added.
"""

import ast
from pathlib import Path

import pytest

DAGS_DIR = Path(__file__).resolve().parents[2] / "dags"

EXPECTED_DAGS = {
    "staging_ingestion.py": {
        "dag_id": "staging_ingestion",
        "pipeline_func": "staging_ingestion_pipeline",
        "asset_uri": "s3://staging/breweries",
    },
    "staging_brasileirao_ingestion.py": {
        "dag_id": "staging_brasileirao_ingestion",
        "pipeline_func": "staging_brasileirao_pipeline",
        "asset_uri": "s3://staging/brasileirao",
    },
    "bronze_silver_processing.py": {
        "dag_id": "bronze_silver_processing",
        "pipeline_func": "bronze_silver_pipeline",
        "asset_uri": "iceberg://nessie/silver/breweries",
    },
    "gold_dbt_processing.py": {
        "dag_id": "gold_dbt_processing",
        "pipeline_func": "gold_dbt_pipeline",
        "asset_uri": "iceberg://nessie/gold/breweries",
    },
    "iceberg_maintenance.py": {
        "dag_id": "iceberg_maintenance",
        "pipeline_func": "iceberg_maintenance_pipeline",
        # Non-asset DAG: maintenance runs on a @weekly cron, does not consume
        # or emit data assets. asset_uri is intentionally None so the parametrized
        # asset test below skips it.
        "asset_uri": None,
    },
}


def _parse(filename: str) -> ast.Module:
    """Parse a DAG file and return its AST module."""
    path = DAGS_DIR / filename
    return ast.parse(path.read_text(encoding="utf-8"))


def _collect_decorator_names(tree: ast.Module) -> list[str]:
    """Return the names of all decorators applied to top-level functions."""
    names = []
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef):
            for dec in node.decorator_list:
                if isinstance(dec, ast.Call) and isinstance(dec.func, ast.Name):
                    names.append(dec.func.id)
                elif isinstance(dec, ast.Name):
                    names.append(dec.id)
    return names


def _collect_string_constants(tree: ast.Module) -> list[str]:
    """Return every string literal in the module."""
    return [n.value for n in ast.walk(tree) if isinstance(n, ast.Constant) and isinstance(n.value, str)]


def _collect_top_level_calls(tree: ast.Module) -> list[str]:
    """Return names of functions called at module top level (not inside other defs)."""
    calls = []
    for node in tree.body:
        if isinstance(node, ast.Expr) and isinstance(node.value, ast.Call):
            call = node.value
            if isinstance(call.func, ast.Name):
                calls.append(call.func.id)
    return calls


class TestDagFilesExist:
    """All expected DAG files must be present on disk."""

    @pytest.mark.parametrize("filename", list(EXPECTED_DAGS.keys()))
    def test_dag_file_exists(self, filename):
        assert (DAGS_DIR / filename).is_file(), f"missing DAG file: {filename}"


class TestDagSyntax:
    """All DAG files must be valid Python."""

    @pytest.mark.parametrize("filename", list(EXPECTED_DAGS.keys()))
    def test_dag_parses_as_python(self, filename):
        _parse(filename)


class TestDagStructure:
    """Each DAG file must declare its @dag decorator, asset, and instantiate the pipeline."""

    @pytest.mark.parametrize(
        "filename,spec",
        [(f, s) for f, s in EXPECTED_DAGS.items()],
    )
    def test_has_dag_decorator(self, filename, spec):
        tree = _parse(filename)
        decorators = _collect_decorator_names(tree)
        assert "dag" in decorators, f"{filename} is missing the @dag decorator"

    @pytest.mark.parametrize(
        "filename,spec",
        [(f, s) for f, s in EXPECTED_DAGS.items()],
    )
    def test_asset_uri_declared(self, filename, spec):
        if spec["asset_uri"] is None:
            pytest.skip(f"{filename} is a non-asset DAG (cron-scheduled)")
        tree = _parse(filename)
        strings = _collect_string_constants(tree)
        assert spec["asset_uri"] in strings, f"{filename} does not reference its expected asset URI {spec['asset_uri']}"

    @pytest.mark.parametrize(
        "filename,spec",
        [(f, s) for f, s in EXPECTED_DAGS.items()],
    )
    def test_dag_id_declared(self, filename, spec):
        tree = _parse(filename)
        strings = _collect_string_constants(tree)
        assert spec["dag_id"] in strings, f"{filename} does not declare dag_id={spec['dag_id']}"

    @pytest.mark.parametrize(
        "filename,spec",
        [(f, s) for f, s in EXPECTED_DAGS.items()],
    )
    def test_pipeline_is_instantiated(self, filename, spec):
        """The DAG function must be called at module top-level to register the DAG."""
        tree = _parse(filename)
        top_calls = _collect_top_level_calls(tree)
        assert spec["pipeline_func"] in top_calls, (
            f"{filename} defines {spec['pipeline_func']} but never calls it at module level "
            "— the DAG will not be registered with Airflow"
        )


class TestFailureCallbacks:
    """Every DAG should wire up an on_failure_callback for observability."""

    @pytest.mark.parametrize("filename", list(EXPECTED_DAGS.keys()))
    def test_wires_failure_callback(self, filename):
        """Each DAG must bind ``on_failure_callback`` — either as a local function
        or as an assignment (e.g. ``on_failure_callback = build_failure_callback(...)``).
        """
        tree = _parse(filename)
        func_names = {n.name for n in ast.walk(tree) if isinstance(n, ast.FunctionDef)}
        assigned_names: set[str] = set()
        for node in ast.walk(tree):
            if isinstance(node, ast.Assign):
                for target in node.targets:
                    if isinstance(target, ast.Name):
                        assigned_names.add(target.id)
        assert "on_failure_callback" in (func_names | assigned_names), f"{filename} does not bind on_failure_callback"


class TestMaintenancePoolIsolation:
    """iceberg_maintenance must run in its own pool so it cannot starve the daily ETL."""

    def test_pool_name_constant_declared(self):
        """The DAG must declare a top-level ``MAINTENANCE_POOL`` string constant.

        Frozen as a contract: the docker-compose scheduler bootstraps a pool
        with the same name, so renaming the Python constant without updating
        the compose command would silently leave the task in waiting state.
        """
        tree = _parse("iceberg_maintenance.py")
        strings = _collect_string_constants(tree)
        assert "maintenance_pool" in strings, (
            "iceberg_maintenance.py must reference the literal 'maintenance_pool' — "
            "the docker-compose scheduler creates a pool with this exact name."
        )

    def test_spark_submit_uses_pool_kwarg(self):
        """The SparkSubmitOperator instantiation must pass pool=MAINTENANCE_POOL.

        Without it, the operator falls back to default_pool and would compete
        directly with bronze_silver_processing on the single 2g Spark worker
        whenever the @weekly schedule lands on the same day as the daily ETL.
        """
        tree = _parse("iceberg_maintenance.py")
        for node in ast.walk(tree):
            if isinstance(node, ast.Call) and isinstance(node.func, ast.Name) and node.func.id == "SparkSubmitOperator":
                kwargs = {kw.arg: kw.value for kw in node.keywords}
                assert "pool" in kwargs, "SparkSubmitOperator in iceberg_maintenance must set pool=..."
                pool_arg = kwargs["pool"]
                # Accept either a literal "maintenance_pool" or the constant Name.
                if isinstance(pool_arg, ast.Constant):
                    assert pool_arg.value == "maintenance_pool"
                elif isinstance(pool_arg, ast.Name):
                    assert pool_arg.id == "MAINTENANCE_POOL"
                else:
                    pytest.fail(f"unexpected AST node for pool kwarg: {ast.dump(pool_arg)}")
                return
        pytest.fail("iceberg_maintenance.py does not instantiate SparkSubmitOperator")
