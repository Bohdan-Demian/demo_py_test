from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from validation.core.models import CheckResult
from validation.core.result import fail_result, pass_result


def check_key_parity(
    left_df: DataFrame,
    right_df: DataFrame,
    *,
    key_columns: list[str],
    severity: str = "BLOCKING",
    sample_limit: int = 20,
) -> CheckResult:
    left_keys = left_df.select(*key_columns).dropDuplicates()
    right_keys = right_df.select(*key_columns).dropDuplicates()

    missing_on_right = left_keys.join(right_keys, on=key_columns, how="left_anti")
    extra_on_right = right_keys.join(left_keys, on=key_columns, how="left_anti")

    left_key_count = left_keys.count()
    right_key_count = right_keys.count()
    missing_count = missing_on_right.count()
    extra_count = extra_on_right.count()

    details = {
        "key_columns": key_columns,
        "left_distinct_key_count": left_key_count,
        "right_distinct_key_count": right_key_count,
        "missing_on_right_count": missing_count,
        "extra_on_right_count": extra_count,
        "missing_on_right_sample": [row.asDict() for row in missing_on_right.limit(sample_limit).collect()],
        "extra_on_right_sample": [row.asDict() for row in extra_on_right.limit(sample_limit).collect()],
    }

    result_factory = pass_result if missing_count == 0 and extra_count == 0 else fail_result
    return result_factory(
        "key_parity",
        severity,
        left_value=left_key_count,
        right_value=right_key_count,
        difference={"missing_on_right": missing_count, "extra_on_right": extra_count},
        details=details,
    )

