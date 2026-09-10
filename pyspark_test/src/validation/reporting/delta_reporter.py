from __future__ import annotations

import json

from pyspark.sql import DataFrame, SparkSession

from validation.core.models import ValidationRunResult


class DeltaReporter:
    def __init__(
        self,
        spark: SparkSession,
        *,
        runs_table: str = "workspace.default.validation_runs",
        checks_table: str = "workspace.default.validation_check_results",
    ) -> None:
        self.spark = spark
        self.runs_table = runs_table
        self.checks_table = checks_table

    def write(self, result: ValidationRunResult) -> None:
        self.build_runs_dataframe(result).write.mode("append").format("delta").saveAsTable(self.runs_table)
        self.build_checks_dataframe(result).write.mode("append").format("delta").saveAsTable(self.checks_table)

    def build_runs_dataframe(self, result: ValidationRunResult) -> DataFrame:
        rows = [
            (
                result.run_id,
                result.entity,
                result.environment,
                result.window_start,
                result.window_end,
                result.started_at,
                result.finished_at,
                float((result.finished_at - result.started_at).total_seconds()),
                result.overall_status,
                len(result.checks),
                sum(1 for check in result.checks if check.status == "PASS"),
                sum(1 for check in result.checks if check.status == "FAIL"),
                sum(1 for check in result.checks if check.status == "WARN"),
                result.has_blocking_failures,
            )
        ]
        schema = (
            "run_id string, entity string, environment string, window_start string, window_end string, "
            "started_at timestamp, finished_at timestamp, duration_seconds double, overall_status string, "
            "total_checks int, passed_checks int, failed_checks int, warned_checks int, has_blocking_failures boolean"
        )
        return self.spark.createDataFrame(rows, schema)

    def build_checks_dataframe(self, result: ValidationRunResult) -> DataFrame:
        rows = [
            (
                result.run_id,
                result.entity,
                result.environment,
                result.window_start,
                result.window_end,
                check.check_name,
                check.status,
                check.severity,
                _to_json(check.left_value),
                _to_json(check.right_value),
                _to_json(check.difference),
                _to_json(check.tolerance),
                _to_json(check.details),
            )
            for check in result.checks
        ]
        schema = (
            "run_id string, entity string, environment string, window_start string, window_end string, "
            "check_name string, status string, severity string, left_value string, right_value string, "
            "difference string, tolerance string, details string"
        )
        return self.spark.createDataFrame(rows, schema)


def _to_json(value) -> str:
    return json.dumps(value, default=str, sort_keys=True)

