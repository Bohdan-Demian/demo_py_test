"""Business aggregate parity check.

Compares grouped metrics between left and right DataFrames. Supported metrics
include sum, count, count_distinct, min, max, and avg.
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import NumericType

from validation.core.models import CheckResult
from validation.core.result import fail_result, pass_result

SUPPORTED_AGGREGATIONS = {"sum", "count", "count_distinct", "min", "max", "avg"}


def check_aggregates(
    left_df: DataFrame,
    right_df: DataFrame,
    *,
    group_by: list[str],
    metrics: dict[str, list[str]],
    severity: str = "BLOCKING",
    tolerance: float = 0,
    sample_limit: int = 20,
) -> CheckResult:
    # Resolve metric aliases up front so left/right aggregate columns can be compared.
    aliases = _metric_aliases(metrics)

    # Fail early on unsupported metrics or missing required columns.
    unsupported = _unsupported_metrics(metrics)
    missing_columns = _missing_columns(left_df, right_df, group_by, metrics)
    if unsupported or any(missing_columns.values()):
        return fail_result(
            "aggregates",
            severity,
            details={
                "unsupported_metrics": unsupported,
                "missing_columns": missing_columns,
                "group_by": group_by,
                "metrics": metrics,
            },
        )

    # Build grouped aggregate DataFrames for both sides using the same config.
    left_agg = _aggregate(left_df, group_by, metrics, "left")
    right_agg = _aggregate(right_df, group_by, metrics, "right")

    # Join aggregate rows by group keys; use a synthetic key for global aggregates.
    join_columns = group_by if group_by else ["__validation_group"]
    joined = left_agg.join(right_agg, on=join_columns, how="full_outer")

    # Mark each metric whose numeric difference is above tolerance.
    comparisons = []
    for alias in aliases:
        mismatch_col = _metric_mismatch_expression(joined, alias, tolerance)
        comparisons.append(F.when(mismatch_col, F.lit(alias)).otherwise(F.lit("__match__")))

    # Keep only groups where at least one aggregate metric differs.
    mismatch_array = F.array_remove(F.array(*comparisons), "__match__")
    compared = joined.withColumn("mismatched_metrics", mismatch_array).withColumn(
        "mismatch_count", F.size("mismatched_metrics")
    )
    mismatches = compared.filter(F.col("mismatch_count") > 0)
    mismatch_count = mismatches.count()

    details = {
        "group_by": group_by,
        "metrics": metrics,
        "tolerance": tolerance,
        "mismatched_group_count": mismatch_count,
        "mismatch_sample": [row.asDict() for row in mismatches.limit(sample_limit).collect()],
    }

    result_factory = pass_result if mismatch_count == 0 else fail_result
    return result_factory(
        "aggregates",
        severity,
        difference=mismatch_count,
        tolerance=tolerance,
        details=details,
    )


def _aggregate(df: DataFrame, group_by: list[str], metrics: dict[str, list[str]], prefix: str) -> DataFrame:
    working_df = df
    grouping_columns = group_by
    if not group_by:
        # Add a synthetic grouping key so global metrics use the same code path.
        working_df = df.withColumn("__validation_group", F.lit("__all__"))
        grouping_columns = ["__validation_group"]

    expressions = []
    for column, aggregations in metrics.items():
        for aggregation in aggregations:
            expressions.append(_aggregation_expression(column, aggregation).alias(f"{prefix}__{aggregation}_{column}"))

    return working_df.groupBy(*grouping_columns).agg(*expressions)


def _aggregation_expression(column: str, aggregation: str):
    if aggregation == "sum":
        return F.sum(column)
    if aggregation == "count":
        return F.count(column)
    if aggregation == "count_distinct":
        return F.countDistinct(column)
    if aggregation == "min":
        return F.min(column)
    if aggregation == "max":
        return F.max(column)
    if aggregation == "avg":
        return F.avg(column)

    raise ValueError(f"Unsupported aggregation: {aggregation}")


def _metric_mismatch_expression(df: DataFrame, alias: str, tolerance: float):
    left_name = f"left__{alias}"
    right_name = f"right__{alias}"
    left_col = F.col(left_name)
    right_col = F.col(right_name)

    if _is_numeric_column(df, left_name) and _is_numeric_column(df, right_name):
        # Numeric aggregate metrics can use tolerance-based diff.
        numeric_difference = F.abs(left_col.cast("double") - right_col.cast("double"))
        return (
            # Treat null/null as match, but null/value as mismatch before numeric comparison.
            F.when(left_col.isNull() & right_col.isNull(), F.lit(False))
            .when(left_col.isNull() | right_col.isNull(), F.lit(True))
            .otherwise(numeric_difference > F.lit(tolerance))
        )

    # Date/string min/max metrics are compared directly to avoid invalid numeric coalesce/casts.
    return ~left_col.eqNullSafe(right_col)


def _is_numeric_column(df: DataFrame, column: str) -> bool:
    return isinstance(df.schema[column].dataType, NumericType)


def _metric_aliases(metrics: dict[str, list[str]]) -> list[str]:
    return [f"{aggregation}_{column}" for column, aggregations in metrics.items() for aggregation in aggregations]


def _unsupported_metrics(metrics: dict[str, list[str]]) -> dict[str, list[str]]:
    return {
        column: [aggregation for aggregation in aggregations if aggregation not in SUPPORTED_AGGREGATIONS]
        for column, aggregations in metrics.items()
        if any(aggregation not in SUPPORTED_AGGREGATIONS for aggregation in aggregations)
    }


def _missing_columns(
    left_df: DataFrame,
    right_df: DataFrame,
    group_by: list[str],
    metrics: dict[str, list[str]],
) -> dict[str, list[str]]:
    required_columns = set(group_by) | set(metrics)
    left_columns = set(left_df.columns)
    right_columns = set(right_df.columns)
    return {
        "left": sorted(required_columns - left_columns),
        "right": sorted(required_columns - right_columns),
    }
