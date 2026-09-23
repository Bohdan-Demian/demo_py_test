# Databricks notebook source
dbutils.widgets.text("environment", "demo")
dbutils.widgets.text("runs_json_path", "configs/runs_databricks_example.json")
dbutils.widgets.text("runs_json", "")
dbutils.widgets.dropdown("write_results", "false", ["false", "true"])
dbutils.widgets.dropdown("fail_on_blocking", "true", ["true", "false"])

# COMMAND ----------

import json
import os
import sys
from functools import reduce
from pathlib import Path

notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
project_root = Path("/Workspace") / notebook_path.lstrip("/")
project_root = project_root.parent.parent

sys.dont_write_bytecode = True
os.chdir(project_root)
sys.path.insert(0, str(project_root / "src"))

from validation.config import load_validation_config
from validation.reporting.delta_reporter import DeltaReporter
from validation.runner import run_validation


def project_path(raw_path: str) -> str:
    path = Path(raw_path)
    if path.is_absolute():
        return str(path)
    return str(project_root / path)


def widget_bool(name: str) -> bool:
    return dbutils.widgets.get(name).strip().lower() == "true"


def read_runs_json() -> str:
    raw_json = dbutils.widgets.get("runs_json").strip()
    if raw_json:
        return raw_json

    runs_json_path = dbutils.widgets.get("runs_json_path").strip()
    if not runs_json_path:
        raise ValueError("Set runs_json or runs_json_path for batch validation")

    return Path(project_path(runs_json_path)).read_text(encoding="utf-8")


def load_run_specs(raw_json: str) -> list[dict]:
    if not raw_json.strip():
        raise ValueError("runs_json is required for batch validation")

    payload = json.loads(raw_json)
    if isinstance(payload, list):
        return payload
    if isinstance(payload, dict):
        return payload.get("runs", [payload])

    raise ValueError("runs_json must be a JSON object, a JSON object with a 'runs' list, or a JSON list")


def get_run_config_path(run_spec: dict) -> str:
    config_path = run_spec.get("config_path")
    if not config_path:
        raise ValueError("Each runs_json item must include config_path")
    return project_path(config_path)


def union_dataframes(dataframes):
    return reduce(lambda left, right: left.unionByName(right), dataframes)


environment = dbutils.widgets.get("environment")
run_specs = load_run_specs(read_runs_json())
write_results = widget_bool("write_results")
fail_on_blocking = widget_bool("fail_on_blocking")

if not run_specs:
    raise ValueError("runs_json produced no validation runs")

results = []

for run_spec in run_specs:
    config_path = get_run_config_path(run_spec)
    entity = run_spec.get("entity")
    run_environment = run_spec.get("environment") or environment

    config = load_validation_config(config_path, entity=entity, environment=run_environment)
    reporting_config = config.get("reporting", {})
    reporter = DeltaReporter(
        spark,
        runs_table=reporting_config.get("runs_table", "workspace.default.validation_runs"),
        checks_table=reporting_config.get("checks_table", "workspace.default.validation_check_results"),
    )

    result = run_validation(
        spark=spark,
        config=config,
        run_id=run_spec.get("run_id"),
        entity=entity,
        environment=run_environment,
        suite=run_spec.get("suite"),
        left_type=run_spec.get("left_type"),
        right_type=run_spec.get("right_type"),
        left_table=run_spec.get("left_table"),
        right_table=run_spec.get("right_table"),
        window_start=run_spec.get("window_start"),
        window_end=run_spec.get("window_end"),
        write_results=False,
    )

    if write_results:
        reporter.write(result)

    results.append((result, reporter))

runs_df = union_dataframes([reporter.build_runs_dataframe(result) for result, reporter in results])
checks_df = union_dataframes([reporter.build_checks_dataframe(result) for result, reporter in results])

display(runs_df.orderBy("started_at"))
display(checks_df.orderBy("run_id", "check_name"))

failed_runs = [result for result, _ in results if result.has_blocking_failures]
if fail_on_blocking and failed_runs:
    failed_run_ids = ", ".join(result.run_id for result in failed_runs)
    raise RuntimeError(f"Blocking validation checks failed for run_id(s): {failed_run_ids}")
