"""Row-level parity check.

Joins left and right DataFrames by business keys and detects missing rows,
extra rows, and changed business values. Supports excluded metadata columns,
numeric tolerances, composite keys, and null-safe comparisons.
"""

from __future__ import annotations

from functools import reduce

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from validation.core.models import CheckResult
from validation.core.result import fail_result, pass_result


def check_row_parity(
    left_df: DataFrame,
    right_df: DataFrame,
    *,
    key_columns: list[str],
    compare_columns: list[str] | None = None,
    exclude_columns: list[str] | None = None,
    numeric_tolerances: dict[str, float] | None = None,
    severity: str = "BLOCKING",
    sample_limit: int = 20,
) -> CheckResult:
    exclude_columns = exclude_columns or []
    numeric_tolerances = numeric_tolerances or {}
    if not key_columns:
        # Row parity needs business keys to avoid accidental cartesian comparison.
        return fail_result(
            "row_parity",
            severity,
            details={"reason": "RowParityCheck requires at least one key column"},
        )

    # Resolve compared business columns after removing keys and excluded metadata.
    resolved_compare_columns = _resolve_compare_columns(left_df, right_df, key_columns, compare_columns, exclude_columns)
    required_columns = set(key_columns) | set(resolved_compare_columns)

    # Validate all key and compared columns before building joins.
    missing_columns = _missing_columns(left_df, right_df, required_columns)
    if any(missing_columns.values()):
        return fail_result(
            "row_parity",
            severity,
            details={
                "key_columns": key_columns,
                "compare_columns": resolved_compare_columns,
                "missing_columns": missing_columns,
            },
        )

    # Distinct key anti-joins identify missing and extra rows.
    left_keys = left_df.select(*key_columns).dropDuplicates()
    right_keys = right_df.select(*key_columns).dropDuplicates()
    missing_on_right = left_keys.join(right_keys, on=key_columns, how="left_anti")
    extra_on_right = right_keys.join(left_keys, on=key_columns, how="left_anti")

    left = left_df.alias("left")
    right = right_df.alias("right")

    # Inner join aligns rows that exist on both sides for value comparison.
    joined = left.join(right, on=_join_condition(key_columns), how="inner")

    # Build one mismatch indicator column per compared business column.
    comparison_columns = [
        _mismatch_indicator(column, numeric_tolerances.get(column)).alias(f"__mismatch_{column}")
        for column in resolved_compare_columns
    ]

    missing_count = missing_on_right.count()
    extra_count = extra_on_right.count()
    compared_row_count = joined.count()

    if comparison_columns:
        # Keep only keys and mismatch indicators to reduce downstream shuffle width.
        compared = joined.select(
            *[_qualified_column("left", column).alias(column) for column in key_columns],
            *comparison_columns,
        )

        # Sum indicator columns to find rows with at least one changed value.
        mismatch_sum = reduce(
            lambda left_col, right_col: left_col + right_col,
            [F.col(f"__mismatch_{column}") for column in resolved_compare_columns],
        )
        mismatched_rows = compared.withColumn("__row_mismatch_count", mismatch_sum).filter(
            F.col("__row_mismatch_count") > 0
        )
        mismatched_row_count = mismatched_rows.count()
        mismatch_counts_by_column = _mismatch_counts_by_column(compared, resolved_compare_columns)
        mismatch_sample = [
            row.asDict()
            for row in _mismatch_sample(mismatched_rows, key_columns, resolved_compare_columns, sample_limit).collect()
        ]
    else:
        mismatched_row_count = 0
        mismatch_counts_by_column = {}
        mismatch_sample = []

    details = {
        "key_columns": key_columns,
        "compare_columns": resolved_compare_columns,
        "exclude_columns": exclude_columns,
        "numeric_tolerances": numeric_tolerances,
        "compared_row_count": compared_row_count,
        "missing_on_right_count": missing_count,
        "extra_on_right_count": extra_count,
        "mismatched_row_count": mismatched_row_count,
        "mismatch_counts_by_column": mismatch_counts_by_column,
        "missing_on_right_sample": [row.asDict() for row in missing_on_right.limit(sample_limit).collect()],
        "extra_on_right_sample": [row.asDict() for row in extra_on_right.limit(sample_limit).collect()],
        "mismatch_sample": mismatch_sample,
    }

    has_failures = missing_count > 0 or extra_count > 0 or mismatched_row_count > 0
    result_factory = fail_result if has_failures else pass_result
    return result_factory(
        "row_parity",
        severity,
        difference={
            "missing_on_right": missing_count,
            "extra_on_right": extra_count,
            "mismatched_rows": mismatched_row_count,
        },
        tolerance=numeric_tolerances,
        details=details,
    )


def _resolve_compare_columns(
    left_df: DataFrame,
    right_df: DataFrame,
    key_columns: list[str],
    compare_columns: list[str] | None,
    exclude_columns: list[str],
) -> list[str]:
    if compare_columns is not None:
        return compare_columns

    excluded = set(key_columns) | set(exclude_columns)
    return sorted((set(left_df.columns) & set(right_df.columns)) - excluded)


def _join_condition(key_columns: list[str]) -> Column:
    return reduce(
        lambda left_condition, right_condition: left_condition & right_condition,
        [
            _qualified_column("left", column).eqNullSafe(_qualified_column("right", column))
            for column in key_columns
        ],
    )


def _mismatch_indicator(column: str, tolerance: float | None) -> Column:
    left_col = _qualified_column("left", column)
    right_col = _qualified_column("right", column)

    if tolerance is not None:
        both_null = left_col.isNull() & right_col.isNull()
        both_present_and_within_tolerance = (
            left_col.isNotNull()
            & right_col.isNotNull()
            & (F.abs(left_col.cast("double") - right_col.cast("double")) <= F.lit(float(tolerance)))
        )
        matches = both_null | both_present_and_within_tolerance
    else:
        matches = left_col.eqNullSafe(right_col)

    return F.when(matches, F.lit(0)).otherwise(F.lit(1))


def _mismatch_counts_by_column(df: DataFrame, compare_columns: list[str]) -> dict[str, int]:
    row = df.agg(
        *[F.sum(F.col(f"__mismatch_{column}")).alias(column) for column in compare_columns]
    ).first()
    return {column: row[column] for column in compare_columns}


def _mismatch_sample(
    mismatched_rows: DataFrame,
    key_columns: list[str],
    compare_columns: list[str],
    sample_limit: int,
) -> DataFrame:
    mismatch_names = F.array_remove(
        F.array(
            *[
                F.when(F.col(f"__mismatch_{column}") == 1, F.lit(column)).otherwise(F.lit("__match__"))
                for column in compare_columns
            ]
        ),
        "__match__",
    )
    return mismatched_rows.withColumn("mismatched_columns", mismatch_names).select(
        *key_columns,
        "mismatched_columns",
    ).limit(sample_limit)


def _qualified_column(alias: str, column: str) -> Column:
    return F.col(f"{alias}.{column}")


def _missing_columns(left_df: DataFrame, right_df: DataFrame, required_columns: set[str]) -> dict[str, list[str]]:
    left_columns = set(left_df.columns)
    right_columns = set(right_df.columns)
    return {
        "left": sorted(required_columns - left_columns),
        "right": sorted(required_columns - right_columns),
    }
