"""Partition coverage check.

Compares which logical partitions exist on both sides and whether each
partition has the same row count. Use this with business windows such as
order_date, event_date, ingestion_date, region, or tenant.
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from validation.core.models import CheckResult
from validation.core.result import fail_result, pass_result


def check_partition(
    left_df: DataFrame,
    right_df: DataFrame,
    *,
    partition_column: str | None,
    severity: str = "BLOCKING",
    sample_limit: int = 20,
) -> CheckResult:
    if not partition_column:
        # No partition configured means this optional check is intentionally skipped.
        return pass_result(
            "partition",
            severity,
            details={"enabled": False, "reason": "No partition_column configured"},
        )

    # Validate that both sides contain the configured partition column.
    missing_columns = _missing_columns(left_df, right_df, partition_column)
    if any(missing_columns.values()):
        return fail_result(
            "partition",
            severity,
            details={"partition_column": partition_column, "missing_columns": missing_columns},
        )

    # Aggregate row counts by partition on both sides.
    left_counts = _partition_counts(left_df, partition_column, "left_count")
    right_counts = _partition_counts(right_df, partition_column, "right_count")

    # Full outer join exposes missing, extra, and count-mismatched partitions.
    joined = left_counts.join(right_counts, on=partition_column, how="full_outer").fillna(
        {"left_count": 0, "right_count": 0}
    )
    mismatches = joined.filter(F.col("left_count") != F.col("right_count"))
    missing_on_right = joined.filter(F.col("left_count") > 0).filter(F.col("right_count") == 0)
    extra_on_right = joined.filter(F.col("left_count") == 0).filter(F.col("right_count") > 0)

    mismatch_count = mismatches.count()
    missing_count = missing_on_right.count()
    extra_count = extra_on_right.count()

    details = {
        "partition_column": partition_column,
        "mismatched_partition_count": mismatch_count,
        "missing_on_right_count": missing_count,
        "extra_on_right_count": extra_count,
        "mismatch_sample": [row.asDict() for row in mismatches.limit(sample_limit).collect()],
    }

    result_factory = pass_result if mismatch_count == 0 else fail_result
    return result_factory(
        "partition",
        severity,
        difference=mismatch_count,
        details=details,
    )


def _partition_counts(df: DataFrame, partition_column: str, count_alias: str) -> DataFrame:
    return df.groupBy(partition_column).agg(F.count(F.lit(1)).alias(count_alias))


def _missing_columns(left_df: DataFrame, right_df: DataFrame, partition_column: str) -> dict[str, list[str]]:
    return {
        "left": [] if partition_column in left_df.columns else [partition_column],
        "right": [] if partition_column in right_df.columns else [partition_column],
    }
