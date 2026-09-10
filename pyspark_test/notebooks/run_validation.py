# Databricks notebook source
dbutils.widgets.text("entity", "orders")
dbutils.widgets.text("environment", "demo")
dbutils.widgets.text("window_start", "")
dbutils.widgets.text("window_end", "")
dbutils.widgets.text("config_path", "")

# COMMAND ----------

import os
import sys
from pathlib import Path

from pyspark.sql import functions as F

notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
project_root = Path("/Workspace") / notebook_path.lstrip("/")
project_root = project_root.parent.parent

sys.dont_write_bytecode = True
os.chdir(project_root)
sys.path.insert(0, str(project_root / "src"))

from validation.config import load_validation_config
from validation.runner import run_validation

entity = dbutils.widgets.get("entity")
environment = dbutils.widgets.get("environment")
window_start = dbutils.widgets.get("window_start") or None
window_end = dbutils.widgets.get("window_end") or None
config_path = dbutils.widgets.get("config_path") or str(project_root / "configs" / "demo.yaml")

config = load_validation_config(config_path, entity=entity, environment=environment)

result = run_validation(
    spark=spark,
    config=config,
    window_start=window_start,
    window_end=window_end,
)

runs_table = config.get("reporting", {}).get("runs_table", "workspace.default.validation_runs")
checks_table = config.get("reporting", {}).get("checks_table", "workspace.default.validation_check_results")

display(spark.table(runs_table).where(F.col("run_id") == result.run_id))
display(spark.table(checks_table).where(F.col("run_id") == result.run_id).orderBy("check_name"))

if result.has_blocking_failures:
    raise RuntimeError(f"Blocking validation checks failed for run_id={result.run_id}")
