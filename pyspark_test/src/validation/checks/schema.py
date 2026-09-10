from __future__ import annotations

from pyspark.sql import DataFrame

from validation.core.models import CheckResult
from validation.core.result import fail_result, pass_result


def check_schema(
    left_df: DataFrame,
    right_df: DataFrame,
    *,
    severity: str = "BLOCKING",
    enforce_order: bool = False,
) -> CheckResult:
    left_fields = {field.name: field.dataType.simpleString() for field in left_df.schema.fields}
    right_fields = {field.name: field.dataType.simpleString() for field in right_df.schema.fields}

    left_columns = list(left_fields)
    right_columns = list(right_fields)
    missing_columns = sorted(set(left_fields) - set(right_fields))
    extra_columns = sorted(set(right_fields) - set(left_fields))
    common_columns = sorted(set(left_fields) & set(right_fields))
    type_mismatches = {
        column: {"left": left_fields[column], "right": right_fields[column]}
        for column in common_columns
        if left_fields[column] != right_fields[column]
    }
    order_mismatch = enforce_order and left_columns != right_columns

    details = {
        "missing_columns": missing_columns,
        "extra_columns": extra_columns,
        "type_mismatches": type_mismatches,
        "order_mismatch": order_mismatch,
    }

    if missing_columns or extra_columns or type_mismatches or order_mismatch:
        return fail_result("schema", severity, details=details)

    return pass_result("schema", severity, details=details)

