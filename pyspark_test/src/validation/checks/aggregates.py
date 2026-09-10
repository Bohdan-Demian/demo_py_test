from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

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
    aliases = _metric_aliases(metrics)
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

    left_agg = _aggregate(left_df, group_by, metrics, "left")
    right_agg = _aggregate(right_df, group_by, metrics, "right")

    join_columns = group_by if group_by else ["__validation_group"]
    joined = left_agg.join(right_agg, on=join_columns, how="full_outer")

    comparisons = []
    for alias in aliases:
        left_col = F.col(f"left__{alias}")
        right_col = F.col(f"right__{alias}")
        difference_col = F.abs(F.coalesce(left_col, F.lit(0.0)) - F.coalesce(right_col, F.lit(0.0)))
        comparisons.append(F.when(difference_col > F.lit(tolerance), F.lit(alias)).otherwise(F.lit("__match__")))

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
