from __future__ import annotations

from datetime import datetime, timezone
from uuid import uuid4

from pyspark.sql import DataFrame, SparkSession

from validation.checks import (
    check_aggregates,
    check_duplicates,
    check_key_parity,
    check_null_rate,
    check_partition,
    check_partition_hash,
    check_row_parity,
    check_row_count,
    check_schema,
)
from validation.core.models import CheckResult, ValidationRunResult
from validation.reporting.delta_reporter import DeltaReporter
from validation.sources.databricks import load_table


def run_validation(
    *,
    spark: SparkSession,
    config: dict,
    left_df: DataFrame | None = None,
    right_df: DataFrame | None = None,
    run_id: str | None = None,
    window_start: str | None = None,
    window_end: str | None = None,
    reporter: DeltaReporter | None = None,
) -> ValidationRunResult:
    started_at = datetime.now(timezone.utc)
    entity = config["entity"]
    environment = config.get("environment", "demo")
    checks_config = config.get("checks", {})
    window_config = config.get("window") or {}
    window_column = window_config.get("column")

    if left_df is None:
        left_df = load_table(
            spark,
            config["left"]["table"],
            window_column=window_column,
            window_start=window_start,
            window_end=window_end,
        )

    if right_df is None:
        right_df = load_table(
            spark,
            config["right"]["table"],
            window_column=window_column,
            window_start=window_start,
            window_end=window_end,
        )

    results = _run_checks(
        left_df,
        right_df,
        config.get("keys", []),
        checks_config,
        window_column,
    )
    overall_status = "FAIL" if any(result.is_blocking_failure for result in results) else "PASS"
    finished_at = datetime.now(timezone.utc)

    result = ValidationRunResult(
        run_id=run_id or str(uuid4()),
        entity=entity,
        environment=environment,
        window_start=window_start,
        window_end=window_end,
        started_at=started_at,
        finished_at=finished_at,
        overall_status=overall_status,
        checks=results,
    )

    if reporter is not None:
        reporter.write(result)
    elif config.get("reporting", {}).get("enabled", False):
        reporting_config = config["reporting"]
        DeltaReporter(
            spark,
            runs_table=reporting_config.get("runs_table", "workspace.default.validation_runs"),
            checks_table=reporting_config.get("checks_table", "workspace.default.validation_check_results"),
        ).write(result)

    return result


def _run_checks(
    left_df: DataFrame,
    right_df: DataFrame,
    key_columns: list[str],
    checks_config: dict,
    window_column: str | None,
) -> list[CheckResult]:
    results: list[CheckResult] = []

    schema_config = checks_config.get("schema", {})
    if schema_config.get("enabled", False):
        results.append(
            check_schema(
                left_df,
                right_df,
                severity=schema_config.get("severity", "BLOCKING"),
                enforce_order=schema_config.get("enforce_order", False),
            )
        )

    partition_config = checks_config.get("partition", {})
    if partition_config.get("enabled", False):
        results.append(
            check_partition(
                left_df,
                right_df,
                partition_column=partition_config.get("column", window_column),
                severity=partition_config.get("severity", "BLOCKING"),
                sample_limit=partition_config.get("sample_limit", 20),
            )
        )

    row_count_config = checks_config.get("row_count", {})
    if row_count_config.get("enabled", False):
        results.append(
            check_row_count(
                left_df,
                right_df,
                severity=row_count_config.get("severity", "BLOCKING"),
                tolerance_pct=row_count_config.get("tolerance_pct", 0),
            )
        )

    key_parity_config = checks_config.get("key_parity", {})
    if key_parity_config.get("enabled", False):
        results.append(
            check_key_parity(
                left_df,
                right_df,
                key_columns=key_columns,
                severity=key_parity_config.get("severity", "BLOCKING"),
                sample_limit=key_parity_config.get("sample_limit", 20),
            )
        )

    duplicates_config = checks_config.get("duplicates", {})
    if duplicates_config.get("enabled", False):
        results.append(
            check_duplicates(
                left_df,
                right_df,
                key_columns=key_columns,
                severity=duplicates_config.get("severity", "BLOCKING"),
                sample_limit=duplicates_config.get("sample_limit", 20),
            )
        )

    null_rate_config = checks_config.get("null_rate", {})
    if null_rate_config.get("enabled", False):
        results.append(
            check_null_rate(
                left_df,
                right_df,
                columns=null_rate_config.get("columns", []),
                severity=null_rate_config.get("severity", "NON_BLOCKING"),
                tolerances_pct=null_rate_config.get("tolerances_pct", null_rate_config.get("tolerance_pct", 0)),
            )
        )

    aggregates_config = checks_config.get("aggregates", {})
    if aggregates_config.get("enabled", False):
        results.append(
            check_aggregates(
                left_df,
                right_df,
                group_by=aggregates_config.get("group_by", []),
                metrics=aggregates_config.get("metrics", {}),
                severity=aggregates_config.get("severity", "BLOCKING"),
                tolerance=aggregates_config.get("tolerance", 0),
                sample_limit=aggregates_config.get("sample_limit", 20),
            )
        )

    partition_hash_config = checks_config.get("partition_hash", {})
    if partition_hash_config.get("enabled", False):
        results.append(
            check_partition_hash(
                left_df,
                right_df,
                partition_column=partition_hash_config.get("partition_column", window_column),
                business_columns=partition_hash_config.get("business_columns"),
                severity=partition_hash_config.get("severity", "BLOCKING"),
                sample_limit=partition_hash_config.get("sample_limit", 20),
            )
        )

    row_parity_config = checks_config.get("row_parity", {})
    if row_parity_config.get("enabled", False):
        results.append(
            check_row_parity(
                left_df,
                right_df,
                key_columns=key_columns,
                compare_columns=row_parity_config.get("compare_columns"),
                exclude_columns=row_parity_config.get("exclude_columns", []),
                numeric_tolerances=row_parity_config.get("numeric_tolerances", {}),
                severity=row_parity_config.get("severity", "BLOCKING"),
                sample_limit=row_parity_config.get("sample_limit", 20),
            )
        )

    return results
