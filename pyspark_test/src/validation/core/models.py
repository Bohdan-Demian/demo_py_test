from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any


@dataclass(frozen=True)
class CheckResult:
    check_name: str
    status: str
    severity: str
    left_value: Any = None
    right_value: Any = None
    difference: Any = None
    tolerance: Any = None
    details: dict[str, Any] = field(default_factory=dict)

    @property
    def is_blocking_failure(self) -> bool:
        return self.status == "FAIL" and self.severity == "BLOCKING"


@dataclass(frozen=True)
class ValidationRunResult:
    run_id: str
    entity: str
    environment: str
    window_start: str | None
    window_end: str | None
    started_at: datetime
    finished_at: datetime
    overall_status: str
    checks: list[CheckResult]

    @property
    def has_blocking_failures(self) -> bool:
        return any(check.is_blocking_failure for check in self.checks)

