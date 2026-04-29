"""Reusable data quality check functions for PySpark DataFrames.

Provides composable quality checks that can be used across Bronze, Silver,
and Gold layer transformations. Each function logs results and optionally
raises exceptions for critical failures.
"""

import logging

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

logger = logging.getLogger(__name__)


def check_null_counts(
    df: DataFrame,
    columns: list[str],
    fail_on_nulls: bool = False,
) -> dict[str, int]:
    """Check null value counts in specified columns.

    Args:
        df: Input DataFrame to check.
        columns: List of column names to inspect for nulls.
        fail_on_nulls: If True, raises ValueError when nulls are found.

    Returns:
        Dictionary mapping column names to their null counts.

    Raises:
        ValueError: If fail_on_nulls is True and any column has null values.
    """
    results: dict[str, int] = {}

    for col_name in columns:
        if col_name not in df.columns:
            logger.warning("Column '%s' not found in DataFrame, skipping null check", col_name)
            continue

        null_count = df.filter(F.col(col_name).isNull()).count()
        results[col_name] = null_count

        if null_count > 0:
            msg = f"{null_count} null values found in column '{col_name}'"
            if fail_on_nulls:
                logger.error(msg)
                raise ValueError(msg)
            logger.warning(msg)

    return results


def check_schema(
    df: DataFrame,
    expected_columns: set[str],
) -> set[str]:
    """Validate that a DataFrame contains all expected columns.

    Args:
        df: Input DataFrame to validate.
        expected_columns: Set of column names that must be present.

    Returns:
        Set of missing column names (empty if all present).

    Raises:
        ValueError: If any expected columns are missing.
    """
    actual_columns = set(df.columns)
    missing = expected_columns - actual_columns

    if missing:
        msg = f"Missing columns in DataFrame: {missing}"
        logger.error(msg)
        raise ValueError(msg)

    logger.info("Schema validation passed: all %d expected columns present", len(expected_columns))
    return missing


def check_row_count(
    df: DataFrame,
    min_rows: int = 1,
) -> int:
    """Validate that a DataFrame has a minimum number of rows.

    Args:
        df: Input DataFrame to check.
        min_rows: Minimum number of rows expected.

    Returns:
        Actual row count.

    Raises:
        ValueError: If the DataFrame has fewer rows than min_rows.
    """
    count = df.count()

    if count < min_rows:
        msg = f"DataFrame has {count} rows, expected at least {min_rows}"
        logger.error(msg)
        raise ValueError(msg)

    logger.info("Row count check passed: %d rows (minimum: %d)", count, min_rows)
    return count


def log_quality_summary(
    df: DataFrame,
    layer: str,
    critical_columns: list[str] | None = None,
) -> dict[str, int | dict[str, int]]:
    """Log a comprehensive data quality summary for a DataFrame.

    Args:
        df: Input DataFrame to summarize.
        layer: Layer name for logging context (e.g., 'bronze', 'silver', 'gold').
        critical_columns: Columns to check for nulls (optional).

    Returns:
        Dictionary with quality metrics: row_count, column_count, null_counts.
    """
    row_count = df.count()
    col_count = len(df.columns)

    summary: dict[str, int | dict[str, int]] = {
        "row_count": row_count,
        "column_count": col_count,
    }

    logger.info(
        "[%s] Quality Summary — rows: %d, columns: %d",
        layer.upper(),
        row_count,
        col_count,
    )

    if critical_columns:
        null_counts = check_null_counts(df, critical_columns)
        summary["null_counts"] = null_counts

    return summary
