# PySpark Migration Validation Framework

Small PySpark/Databricks demo project for AWS -> GCP migration parity testing.

The main validation flow is production-like:

```text
Databricks Job
  -> thin Databricks notebook
  -> Python validation runner
  -> load left/right DataFrames
  -> run reusable checks
  -> write Delta result tables
  -> fail the job when blocking checks fail
```

The validation code does not depend on `dbutils` or notebook state. The notebook only reads widgets and calls the Python runner.

## Project Layout

```text
configs/
  demo.yaml

notebooks/
  run_validation.py

src/
  validation/
    runner.py
    config.py
    core/
      models.py
      result.py
    sources/
      databricks.py
    checks/
      schema.py
      partition.py
      row_count.py
      key_parity.py
      duplicates.py
      null_rate.py
      aggregates.py
      partition_hash.py
      row_parity.py
    reporting/
      delta_reporter.py

tests/
  unit/
  integration/
```

Legacy e-commerce demo code still exists in `src/ecommerce_quality/` with the original tests in `tests/test_*.py`.

## Local Development

PySpark needs Java. On macOS with Homebrew:

```bash
brew install openjdk@17
```

Create a virtual environment and install the project:

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip setuptools wheel
python -m pip install -e ".[dev]"
```

Run all tests:

```bash
make test
```

Run the Databricks-applicable pytest subset:

```bash
make databricks-test
```

## Validation Config

The demo config lives in:

```text
configs/demo.yaml
```

For the first smoke test both sides point to the same Databricks table:

```yaml
left:
  table: workspace.default.orderscsv

right:
  table: workspace.default.orderscsv
```

Later, replace them with separate AWS and GCP output tables:

```yaml
left:
  table: workspace.default.aws_orders

right:
  table: workspace.default.gcp_orders
```

Do not invent key/window columns. Inspect the Databricks table schemas first, then fill:

```yaml
keys:
  - order_id

window:
  column: order_date
```

The runtime window uses the same semantics on both sides:

```text
window_column >= window_start
AND window_column < window_end
```

## Checks

Implemented checks:

- `schema` - column names and Spark data types
- `partition` - partition coverage and row counts per partition
- `row_count` - total row count with percentage tolerance
- `key_parity` - missing/extra business keys
- `duplicates` - duplicate business keys on both sides
- `null_rate` - null count/rate per configured column
- `aggregates` - grouped business metrics
- `partition_hash` - scalable partition-level content fingerprint
- `row_parity` - key-based row comparison with excluded columns and numeric tolerance

Each check returns a structured `CheckResult`, not a plain boolean.

## Databricks Execution

Use this notebook as the Databricks Job task:

```text
notebooks/run_validation.py
```

Notebook widgets:

```text
entity
environment
window_start
window_end
config_path
```

Example windowed run:

```text
entity=orders
environment=demo
window_start=2026-09-01
window_end=2026-09-02
config_path=/Workspace/Repos/<user>/<repo>/pyspark_test/configs/demo.yaml
```

The notebook:

- loads YAML config
- calls `validation.runner.run_validation(...)`
- displays run/check result rows
- raises `RuntimeError` if any `BLOCKING` check fails

It does not run pytest.

## Result Tables

The Delta reporter appends to:

```text
workspace.default.validation_runs
workspace.default.validation_check_results
```

These tables support:

- latest validation run
- overall status
- failed checks
- validation window
- check values/differences
- run history

The old pytest reporting tables are left untouched:

```text
workspace.default.pyspark_demo_test_run_results
workspace.default.pyspark_demo_test_case_results
```

## Pytest Role

Pytest tests the framework itself with small synthetic Spark DataFrames.

Unit tests do not require production Databricks tables. They verify matching data, mismatches, edge cases, tolerance logic, samples, and runner behavior.

## Legacy Demo

The original e-commerce ETL demo can still be run locally:

```bash
make pipeline
```

It reads `data/raw/*.csv` and writes sample Parquet outputs to `data/processed/`.
