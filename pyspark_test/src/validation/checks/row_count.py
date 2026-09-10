from __future__ import annotations

from pyspark.sql import DataFrame

from validation.core.models import CheckResult
from validation.core.result import fail_result, pass_result


def check_row_count(
    left_df: DataFrame,
    right_df: DataFrame,
    *,
    severity: str = "BLOCKING",
    tolerance_pct: float = 0,
) -> CheckResult:
    left_count = left_df.count()
    right_count = right_df.count()
    absolute_difference = abs(left_count - right_count)
    baseline = max(left_count, 1)
    percentage_difference = (absolute_difference / baseline) * 100

    details = {
        "left_count": left_count,
        "right_count": right_count,
        "absolute_difference": absolute_difference,
        "percentage_difference": percentage_difference,
    }

    result_factory = pass_result if percentage_difference <= tolerance_pct else fail_result
    return result_factory(
        "row_count",
        severity,
        left_value=left_count,
        right_value=right_count,
        difference=absolute_difference,
        tolerance=tolerance_pct,
        details=details,
    )

