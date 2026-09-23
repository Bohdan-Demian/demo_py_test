# PySpark Migration Validation Framework

Small PySpark/Databricks demo project for AWS -> GCP migration parity testing.

The main validation flow is production-like:

```text
Databricks Job
  -> thin Databricks notebook
  -> Python validation runner
  -> load left/right DataFrames
  -> run reusable checks
  -> display validation results
  -> optionally write official runs to Delta result tables
  -> fail the job when blocking checks fail
```

The validation code does not depend on `dbutils` or notebook state. The notebook only reads widgets and calls the Python runner.

## Project Layout

```text
configs/
  demo.yaml

notebooks/
  run_validation.py
  run_validation_batch.py
  show_validation_runs.py

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

Legacy e-commerce source code is no longer part of the active validation test path.

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

Ready Databricks self-compare configs aligned with the current `workspace.default` demo tables:

```text
configs/customers.yaml  -> workspace.default.customerscsv
configs/orders.yaml     -> workspace.default.orderscsv
configs/products.yaml   -> workspace.default.productscsv
```

For the first smoke test both sides point to the same Databricks table:

```yaml
left:
  type: databricks
  table: workspace.default.orderscsv

right:
  type: databricks
  table: workspace.default.orderscsv
```

Later, replace them with separate AWS and GCP output tables. If AWS data is exposed to the GCP validation workspace through Delta Sharing, use the shared table name on the left side and the native GCP output table name on the right side:

```yaml
left:
  type: databricks
  table: aws_share.sales.orders

right:
  type: databricks
  table: gcp_curated.sales.orders
```

The Databricks runner loads both sides with `spark.table(...)`, so shared AWS tables and local GCP tables are handled the same way once they are visible in the validation workspace.

Supported source pairs:

- `databricks_to_databricks` - reads both sides with `spark.table(...)`
- `snowflake_to_snowflake` - reads both sides with Spark Snowflake connector using `spark.read.format("snowflake")`

The runner intentionally rejects mixed pairs such as `snowflake` vs `databricks`. Keep Databricks parity and Snowflake parity as separate validation suites.

Snowflake-to-Snowflake source example:

```yaml
suite: orders_snowflake_parity

left:
  type: snowflake
  table: ORDERS
  options:
    sfURL: aws_account.snowflakecomputing.com
    sfUser: AWS_VALIDATION_USER
    sfWarehouse: AWS_VALIDATION_WH
    sfDatabase: AWS_SALES_DB
    sfSchema: PUBLIC
  env_options:
    sfPassword: AWS_SNOWFLAKE_PASSWORD

right:
  type: snowflake
  table: ORDERS
  options:
    sfURL: gcp_account.snowflakecomputing.com
    sfUser: GCP_VALIDATION_USER
    sfWarehouse: GCP_VALIDATION_WH
    sfDatabase: GCP_SALES_DB
    sfSchema: PUBLIC
  env_options:
    sfPassword: GCP_SNOWFLAKE_PASSWORD
```

For Snowflake, install/configure the Spark Snowflake connector on the Databricks cluster or job. Keep credentials in Databricks secrets, environment variables, or secret-backed cluster config rather than committing them to YAML.

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
- `partition_hash` - scalable checksum-style partition-level content fingerprint
- `row_parity` - key-based row comparison with excluded columns and numeric tolerance

Each check returns a structured `CheckResult`, not a plain boolean.

For migration work, `partition_hash` is often the most practical first deep check. Think of it as a checksum-style validation per partition:

```yaml
partition_hash:
  enabled: true
  severity: BLOCKING
  partition_column: order_date
  business_columns:
    - order_id
    - customer_id
    - amount
    - status
```

It creates deterministic row fingerprints and aggregates them per partition. If a partition hash differs, use `row_parity` to identify the concrete missing/extra/changed rows.

Detailed check documentation lives in [docs/checks.md](docs/checks.md).

## Databricks Execution

Use this notebook for a single validation run:

```text
notebooks/run_validation.py
```

Notebook widgets:

```text
entity
environment
suite
left_type
right_type
left_table
right_table
window_start
window_end
config_path
write_results
```

Example windowed run:

```text
entity=orders
environment=dev
suite=orders_databricks_parity
left_type=databricks
right_type=databricks
left_table=aws_share.sales.orders
right_table=gcp_curated.sales.orders
window_start=2026-09-01
window_end=2026-09-02
config_path=/Workspace/Repos/<user>/<repo>/pyspark_test/configs/demo.yaml
write_results=false
```

`left_type`, `right_type`, `left_table`, and `right_table` are runtime overrides. Keep YAML as the stable default config, then let Airflow/Databricks Workflows pass the exact output sources produced by the AWS and GCP DAG runs.

`write_results=false` is the default. The notebook displays run/check DataFrames from memory and does not append to Delta result tables unless `write_results=true`. Use `write_results=true` only for official validation runs that should appear in dashboards/history.

For Snowflake parity use a separate job/config with:

```text
suite=orders_snowflake_parity
left_type=snowflake
right_type=snowflake
left_table=ORDERS
right_table=ORDERS
```

Connection options still come from YAML or secret-backed environment variables.

The single-run notebook:

- loads YAML config
- calls `validation.runner.run_validation(...)`
- displays run/check result rows from the current result
- writes result rows only when `write_results=true`
- raises `RuntimeError` if any `BLOCKING` check fails

It does not run pytest.

For repeated DAG-style validation, use:

```text
notebooks/run_validation_batch.py
```

Batch widgets:

```text
environment
runs_json
write_results
fail_on_blocking
```

`runs_json` can be a JSON object with a `runs` list or a plain JSON list. Each item must include its own `config_path` and is passed as runtime overrides to the same validation runner.

A ready-to-copy example lives in:

```text
configs/runs_databricks_example.json
```

Shape:

```json
{
  "runs": [
    {
      "config_path": "/Workspace/Repos/<user>/<repo>/pyspark_test/configs/orders.yaml",
      "entity": "orders",
      "suite": "orders_databricks_self_compare",
      "left_table": "workspace.default.orderscsv",
      "right_table": "workspace.default.orderscsv"
    }
  ]
}
```

The batch notebook runs all validations, collects result DataFrames, displays the combined run/check output at the end, and writes to Delta result tables only when `write_results=true`.

To inspect official result history, use:

```text
notebooks/show_validation_runs.py
```

## Result Tables

Official runs append to:

```text
workspace.default.validation_runs
workspace.default.validation_check_results
```

These tables support:

- latest validation run
- validation suite and source pair
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
