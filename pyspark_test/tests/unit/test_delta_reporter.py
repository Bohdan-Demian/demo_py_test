from datetime import datetime, timezone

from validation.core.models import CheckResult, ValidationRunResult
from validation.reporting.delta_reporter import DeltaReporter


def test_delta_reporter_builds_run_and_check_dataframes(spark):
    started_at = datetime(2026, 9, 9, 10, 0, 0, tzinfo=timezone.utc)
    finished_at = datetime(2026, 9, 9, 10, 0, 5, tzinfo=timezone.utc)
    result = ValidationRunResult(
        run_id="run-1",
        entity="orders",
        environment="demo",
        window_start="2026-09-01",
        window_end="2026-09-02",
        started_at=started_at,
        finished_at=finished_at,
        overall_status="FAIL",
        checks=[
            CheckResult(
                check_name="row_count",
                status="FAIL",
                severity="BLOCKING",
                left_value=10,
                right_value=9,
                difference=1,
                tolerance=0,
                details={"absolute_difference": 1},
            ),
            CheckResult(check_name="schema", status="PASS", severity="BLOCKING"),
        ],
    )

    reporter = DeltaReporter(spark)

    runs = reporter.build_runs_dataframe(result).collect()
    checks = reporter.build_checks_dataframe(result).orderBy("check_name").collect()

    assert len(runs) == 1
    assert runs[0]["run_id"] == "run-1"
    assert runs[0]["overall_status"] == "FAIL"
    assert runs[0]["total_checks"] == 2
    assert runs[0]["failed_checks"] == 1
    assert runs[0]["has_blocking_failures"] is True

    assert len(checks) == 2
    checks_by_name = {row["check_name"]: row for row in checks}
    assert checks_by_name["row_count"]["details"] == '{"absolute_difference": 1}'
