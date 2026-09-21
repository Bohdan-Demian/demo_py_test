"""Schema parity check.

Compares left and right DataFrame schemas by column name and Spark data type.
Column order is ignored by default because many migration pipelines reorder
columns without changing business meaning.
"""

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
    # Normalize schema fields to comparable name -> Spark type maps.
    left_fields = {field.name: field.dataType.simpleString() for field in left_df.schema.fields}
    right_fields = {field.name: field.dataType.simpleString() for field in right_df.schema.fields}

    left_columns = list(left_fields)
    right_columns = list(right_fields)

    # Compare column coverage from the left/source perspective.
    missing_columns = sorted(set(left_fields) - set(right_fields))
    extra_columns = sorted(set(right_fields) - set(left_fields))

    # Compare types only for columns that exist on both sides.
    common_columns = sorted(set(left_fields) & set(right_fields))
    type_mismatches = {
        column: {"left": left_fields[column], "right": right_fields[column]}
        for column in common_columns
        if left_fields[column] != right_fields[column]
    }

    # Column order is optional because many pipelines reorder fields safely.
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
