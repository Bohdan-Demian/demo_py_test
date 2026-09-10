from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from validation.core.models import CheckResult
from validation.core.result import fail_result, pass_result


GLOBAL_PARTITION_COLUMN = "__validation_partition"


def check_partition_hash(
    left_df: DataFrame,
    right_df: DataFrame,
    *,
    partition_column: str | None,
    business_columns: list[str] | None = None,
    severity: str = "BLOCKING",
    sample_limit: int = 20,
) -> CheckResult:
    columns_to_hash = business_columns or sorted(set(left_df.columns) & set(right_df.columns))
    missing_columns = _missing_columns(left_df, right_df, columns_to_hash, partition_column)
    if any(missing_columns.values()):
        return fail_result(
            "partition_hash",
            severity,
            details={
                "partition_column": partition_column,
                "business_columns": columns_to_hash,
                "missing_columns": missing_columns,
            },
        )

    left_hashes = _partition_hashes(left_df, partition_column, columns_to_hash, "left")
    right_hashes = _partition_hashes(right_df, partition_column, columns_to_hash, "right")
    join_column = partition_column or GLOBAL_PARTITION_COLUMN

    joined = left_hashes.join(right_hashes, on=join_column, how="full_outer").fillna(
        {"left_row_count": 0, "right_row_count": 0}
    )
    mismatches = joined.filter(
        (F.col("left_row_count") != F.col("right_row_count"))
        | (~F.col("left_partition_hash").eqNullSafe(F.col("right_partition_hash")))
    )
    mismatch_count = mismatches.count()

    details = {
        "partition_column": partition_column,
        "business_columns": columns_to_hash,
        "mismatched_partition_count": mismatch_count,
        "mismatch_sample": [row.asDict() for row in mismatches.limit(sample_limit).collect()],
        "limitations": [
            "Hash comparison is a locating check, not a formal proof of row equality.",
            "Timestamp string formatting depends on the active Spark session timezone.",
            "Numeric formatting follows Spark cast semantics for the input data types.",
        ],
    }

    result_factory = pass_result if mismatch_count == 0 else fail_result
    return result_factory(
        "partition_hash",
        severity,
        difference=mismatch_count,
        details=details,
    )


def _partition_hashes(
    df: DataFrame,
    partition_column: str | None,
    business_columns: list[str],
    prefix: str,
) -> DataFrame:
    partition_key = partition_column or GLOBAL_PARTITION_COLUMN
    working_df = df
    if partition_column is None:
        working_df = df.withColumn(partition_key, F.lit("__all__"))

    canonical_values = [_canonical_column(column) for column in business_columns]
    row_hash = F.xxhash64(*canonical_values)

    hashed = working_df.withColumn("__validation_row_hash", row_hash)
    aggregated = hashed.groupBy(partition_key).agg(
        F.count(F.lit(1)).alias(f"{prefix}_row_count"),
        F.sum(F.col("__validation_row_hash").cast("decimal(38,0)")).alias(f"{prefix}_hash_sum"),
        F.sum(F.abs(F.col("__validation_row_hash")).cast("decimal(38,0)")).alias(f"{prefix}_hash_abs_sum"),
    )

    return aggregated.withColumn(
        f"{prefix}_partition_hash",
        F.sha2(
            F.concat_ws(
                "||",
                F.col(f"{prefix}_row_count").cast("string"),
                F.coalesce(F.col(f"{prefix}_hash_sum").cast("string"), F.lit("__NULL__")),
                F.coalesce(F.col(f"{prefix}_hash_abs_sum").cast("string"), F.lit("__NULL__")),
            ),
            256,
        ),
    )


def _canonical_column(column: str):
    return F.concat(
        F.lit(f"{column}="),
        F.coalesce(F.col(column).cast("string"), F.lit("__NULL__")),
    )


def _missing_columns(
    left_df: DataFrame,
    right_df: DataFrame,
    business_columns: list[str],
    partition_column: str | None,
) -> dict[str, list[str]]:
    required_columns = set(business_columns)
    if partition_column:
        required_columns.add(partition_column)

    left_columns = set(left_df.columns)
    right_columns = set(right_df.columns)
    return {
        "left": sorted(required_columns - left_columns),
        "right": sorted(required_columns - right_columns),
    }

