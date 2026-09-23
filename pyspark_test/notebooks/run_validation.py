# Databricks notebook source
dbutils.widgets.text("entity", "orders")
dbutils.widgets.text("environment", "demo")
dbutils.widgets.text("suite", "")
dbutils.widgets.text("left_type", "")
dbutils.widgets.text("right_type", "")
dbutils.widgets.text("left_table", "")
dbutils.widgets.text("right_table", "")
dbutils.widgets.text("window_start", "")
dbutils.widgets.text("window_end", "")
dbutils.widgets.text("config_path", "")
dbutils.widgets.dropdown("write_results", "false", ["false", "true"])

# COMMAND ----------

import os
import sys
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


entity = dbutils.widgets.get("entity")
environment = dbutils.widgets.get("environment")
suite = dbutils.widgets.get("suite") or None
left_type = dbutils.widgets.get("left_type") or None
right_type = dbutils.widgets.get("right_type") or None
left_table = dbutils.widgets.get("left_table") or None
right_table = dbutils.widgets.get("right_table") or None
window_start = dbutils.widgets.get("window_start") or None
window_end = dbutils.widgets.get("window_end") or None
config_path = project_path(dbutils.widgets.get("config_path") or "configs/demo.yaml")
write_results = dbutils.widgets.get("write_results").lower() == "true"

config = load_validation_config(config_path, entity=entity, environment=environment)
reporting_config = config.get("reporting", {})
reporter = DeltaReporter(
    spark,
    runs_table=reporting_config.get("runs_table", "workspace.default.validation_runs"),
    checks_table=reporting_config.get("checks_table", "workspace.default.validation_check_results"),
)

result = run_validation(
    spark=spark,
    config=config,
    suite=suite,
    left_type=left_type,
    right_type=right_type,
    left_table=left_table,
    right_table=right_table,
    window_start=window_start,
    window_end=window_end,
    reporter=reporter,
    write_results=write_results,
)

display(reporter.build_runs_dataframe(result))
display(reporter.build_checks_dataframe(result).orderBy("check_name"))

if result.has_blocking_failures:
    raise RuntimeError(f"Blocking validation checks failed for run_id={result.run_id}")
