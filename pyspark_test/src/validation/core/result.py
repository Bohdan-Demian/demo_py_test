from __future__ import annotations

from validation.core.models import CheckResult


def pass_result(
    check_name: str,
    severity: str,
    *,
    left_value=None,
    right_value=None,
    difference=None,
    tolerance=None,
    details: dict | None = None,
) -> CheckResult:
    return CheckResult(
        check_name=check_name,
        status="PASS",
        severity=severity,
        left_value=left_value,
        right_value=right_value,
        difference=difference,
        tolerance=tolerance,
        details=details or {},
    )


def fail_result(
    check_name: str,
    severity: str,
    *,
    left_value=None,
    right_value=None,
    difference=None,
    tolerance=None,
    details: dict | None = None,
) -> CheckResult:
    return CheckResult(
        check_name=check_name,
        status="FAIL",
        severity=severity,
        left_value=left_value,
        right_value=right_value,
        difference=difference,
        tolerance=tolerance,
        details=details or {},
    )

