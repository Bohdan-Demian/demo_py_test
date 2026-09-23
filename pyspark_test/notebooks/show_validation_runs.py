# Databricks notebook source
dbutils.widgets.text("runs_table", "workspace.default.validation_runs")
dbutils.widgets.text("checks_table", "workspace.default.validation_check_results")
dbutils.widgets.text("environment", "")
dbutils.widgets.text("suite", "")
dbutils.widgets.text("limit", "25")

# COMMAND ----------

from pyspark.sql import functions as F

runs_table = dbutils.widgets.get("runs_table")
checks_table = dbutils.widgets.get("checks_table")
environment = dbutils.widgets.get("environment") or None
suite = dbutils.widgets.get("suite") or None
limit = int(dbutils.widgets.get("limit") or "25")

runs_df = spark.table(runs_table)
checks_df = spark.table(checks_table)

if environment:
    runs_df = runs_df.where(F.col("environment") == environment)
    checks_df = checks_df.where(F.col("environment") == environment)

if suite:
    runs_df = runs_df.where(F.col("suite") == suite)
    checks_df = checks_df.where(F.col("suite") == suite)

latest_runs_df = runs_df.orderBy(F.col("started_at").desc()).limit(limit)
latest_run_ids = [row["run_id"] for row in latest_runs_df.select("run_id").collect()]

display(latest_runs_df)

if latest_run_ids:
    display(
        checks_df.where(F.col("run_id").isin(latest_run_ids))
        .where(F.col("status") != "PASS")
        .orderBy(F.col("run_id"), F.col("check_name"))
    )
