"""Duplicate business key check.

Detects duplicate key groups independently on the left and right sides. This is
important before row parity because duplicate keys can make joins ambiguous.
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from validation.core.models import CheckResult
from validation.core.result import fail_result, pass_result


def _duplicate_keys(df: DataFrame, key_columns: list[str]) -> DataFrame:
    # Group by business keys and keep only key groups with more than one row.
    return (
        df.groupBy(*key_columns)
        .agg(F.count(F.lit(1)).alias("row_count"))
        .filter(F.col("row_count") > 1)
    )


def check_duplicates(
    left_df: DataFrame,
    right_df: DataFrame,
    *,
    key_columns: list[str],
    severity: str = "BLOCKING",
    sample_limit: int = 20,
) -> CheckResult:
    # Detect duplicate key groups separately because each side can fail independently.
    left_duplicates = _duplicate_keys(left_df, key_columns)
    right_duplicates = _duplicate_keys(right_df, key_columns)

    # Count duplicate key groups; collect only limited samples for debugging.
    left_duplicate_key_count = left_duplicates.count()
    right_duplicate_key_count = right_duplicates.count()

    details = {
        "key_columns": key_columns,
        "left_duplicate_key_count": left_duplicate_key_count,
        "right_duplicate_key_count": right_duplicate_key_count,
        "left_duplicate_sample": [row.asDict() for row in left_duplicates.limit(sample_limit).collect()],
        "right_duplicate_sample": [row.asDict() for row in right_duplicates.limit(sample_limit).collect()],
    }

    result_factory = pass_result if left_duplicate_key_count == 0 and right_duplicate_key_count == 0 else fail_result
    return result_factory(
        "duplicates",
        severity,
        left_value=left_duplicate_key_count,
        right_value=right_duplicate_key_count,
        difference=left_duplicate_key_count + right_duplicate_key_count,
        details=details,
    )
