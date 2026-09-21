"""Null rate parity check.

Compares null counts and null percentages for configured columns. Supports
per-column tolerances so critical identifiers can be strict while descriptive
fields can allow small drift.
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from validation.core.models import CheckResult
from validation.core.result import fail_result, pass_result


def check_null_rate(
    left_df: DataFrame,
    right_df: DataFrame,
    *,
    columns: list[str],
    severity: str = "NON_BLOCKING",
    tolerances_pct: dict[str, float] | float = 0,
) -> CheckResult:
    # Validate configured columns before building Spark aggregate expressions.
    missing_columns = _missing_columns(left_df, right_df, columns)
    if any(missing_columns.values()):
        return fail_result(
            "null_rate",
            severity,
            details={"missing_columns": missing_columns, "columns": columns},
        )

    # Calculate all null metrics in one aggregation per side.
    left_metrics = _calculate_null_metrics(left_df, columns)
    right_metrics = _calculate_null_metrics(right_df, columns)

    column_results = {}
    failed_columns = []
    for column in columns:
        # Compare null percentages column-by-column using configured tolerances.
        tolerance = _get_tolerance(tolerances_pct, column)
        difference_pct = abs(left_metrics[column]["null_pct"] - right_metrics[column]["null_pct"])
        passed = difference_pct <= tolerance
        if not passed:
            failed_columns.append(column)

        column_results[column] = {
            "left_null_count": left_metrics[column]["null_count"],
            "right_null_count": right_metrics[column]["null_count"],
            "left_null_pct": left_metrics[column]["null_pct"],
            "right_null_pct": right_metrics[column]["null_pct"],
            "difference_pct": difference_pct,
            "tolerance_pct": tolerance,
            "status": "PASS" if passed else "FAIL",
        }

    details = {
        "columns": columns,
        "failed_columns": failed_columns,
        "per_column": column_results,
    }

    result_factory = pass_result if not failed_columns else fail_result
    return result_factory(
        "null_rate",
        severity,
        difference={"failed_columns": len(failed_columns)},
        tolerance=tolerances_pct,
        details=details,
    )


def _missing_columns(left_df: DataFrame, right_df: DataFrame, columns: list[str]) -> dict[str, list[str]]:
    left_columns = set(left_df.columns)
    right_columns = set(right_df.columns)
    return {
        "left": sorted(column for column in columns if column not in left_columns),
        "right": sorted(column for column in columns if column not in right_columns),
    }


def _calculate_null_metrics(df: DataFrame, columns: list[str]) -> dict[str, dict[str, float]]:
    total_alias = "__total_rows"

    # Build one aggregate query with total rows and all requested null counts.
    expressions = [F.count(F.lit(1)).alias(total_alias)]
    expressions.extend(
        F.sum(F.when(F.col(column).isNull(), F.lit(1)).otherwise(F.lit(0))).alias(f"{column}__null_count")
        for column in columns
    )

    row = df.agg(*expressions).first().asDict()
    total = row[total_alias]
    return {
        column: {
            "null_count": row[f"{column}__null_count"],
            "null_pct": (row[f"{column}__null_count"] / total * 100) if total else 0.0,
        }
        for column in columns
    }


def _get_tolerance(tolerances_pct: dict[str, float] | float, column: str) -> float:
    if isinstance(tolerances_pct, dict):
        return float(tolerances_pct.get(column, 0))

    return float(tolerances_pct)
