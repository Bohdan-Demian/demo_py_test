# Validation Checks

This project validates parity between a left dataset and a right dataset, for example AWS output vs GCP output.

Checks are intentionally reusable Python functions. They accept Spark DataFrames and configuration values, then return a structured `CheckResult`. They do not load tables by themselves and they do not depend on notebooks or `dbutils`.

## Recommended Runtime Order

Run cheap, broad checks before expensive row-level checks:

```text
schema
partition
row_count
key_parity
duplicates
null_rate
aggregates
partition_hash
row_parity
```

For migration validation, always compare the same logical data window on both sides:

```text
window_column >= window_start
AND window_column < window_end
```

## Schema Check

File: `src/validation/checks/schema.py`

Validates:

- missing columns on the right side
- extra columns on the right side
- Spark data type mismatches
- optional column order mismatch

Use it as a blocking check. If schema parity fails, most downstream checks become harder to trust.

Good for AWS -> GCP migration:

- catching renamed fields
- catching decimal/string/date type drift
- catching accidental metadata columns

## Partition Check

File: `src/validation/checks/partition.py`

Validates partition coverage and partition row counts.

It detects:

- partitions present only on the left side
- partitions present only on the right side
- partitions present on both sides but with different row counts

Use it when the entity has a natural partition/window column, such as:

- `order_date`
- `event_date`
- `ingestion_date`
- `business_date`
- `region`

## Row Count Check

File: `src/validation/checks/row_count.py`

Validates total row counts.

It returns:

- left count
- right count
- absolute difference
- percentage difference

Use exact equality for deterministic migration outputs. Use tolerance only when the source system itself is expected to be slightly non-deterministic.

## Key Parity Check

File: `src/validation/checks/key_parity.py`

Validates business key coverage.

It detects:

- distinct key count on both sides
- keys missing on the right side
- keys extra on the right side
- limited samples of missing/extra keys

Supports composite keys.

Use it before row-level value comparison. If keys do not match, row parity will be noisy.

## Duplicate Check

File: `src/validation/checks/duplicates.py`

Validates key uniqueness separately on both sides.

It detects duplicate business keys on:

- left side
- right side

Use it as a blocking check when business keys are expected to be unique. Duplicate keys can invalidate row parity joins.

## Null Rate Check

File: `src/validation/checks/null_rate.py`

Validates null counts and null percentages for configured columns.

It supports per-column tolerances:

```yaml
null_rate:
  enabled: true
  severity: NON_BLOCKING
  columns:
    - customer_id
    - email
  tolerances_pct:
    customer_id: 0
    email: 0.5
```

Useful AWS -> GCP checks:

- unexpected null inflation after migration
- empty strings becoming nulls
- nullable fields becoming populated differently

## Aggregate Check

File: `src/validation/checks/aggregates.py`

Validates grouped business metrics.

Supported metrics:

- `sum`
- `count`
- `count_distinct`
- `min`
- `max`
- `avg`

Example:

```yaml
aggregates:
  enabled: true
  severity: BLOCKING
  group_by:
    - order_date
  metrics:
    revenue:
      - sum
      - min
      - max
      - avg
    order_id:
      - count
      - count_distinct
  tolerance: 0
```

Good migration aggregate patterns:

- `sum(amount)` by business date
- `count(order_id)` by business date
- `count_distinct(customer_id)` by business date or region
- `min(created_at)` and `max(created_at)` to catch truncated windows
- `min(amount)` and `max(amount)` to catch scaling/sign issues
- `avg(amount)` as a secondary signal, not as a replacement for sum/count

For numeric metrics, use a small tolerance only when source systems can differ because of floating point precision or rounding.

## Partition Hash Check

File: `src/validation/checks/partition_hash.py`

This is the checksum-style check.

It:

1. Builds deterministic row fingerprints from configured business columns.
2. Aggregates row hash metrics per partition.
3. Compares partition-level fingerprints between left and right.

Example:

```yaml
partition_hash:
  enabled: true
  severity: BLOCKING
  partition_column: order_date
  business_columns:
    - order_id
    - customer_id
    - product_id
    - amount
    - status
```

Use it to cheaply locate partitions that differ before running deeper row parity.

Limitations:

- It is a locating check, not a formal proof of equality.
- Hash collisions are unlikely but theoretically possible.
- Timestamp string formatting depends on Spark session timezone.
- Numeric formatting follows Spark cast semantics.

## Row Parity Check

File: `src/validation/checks/row_parity.py`

This is the deepest check.

It joins left and right using configured business keys and detects:

- missing rows
- extra rows
- changed business values
- mismatch counts per compared column
- limited mismatch samples

It supports:

- composite keys
- excluded columns
- numeric tolerances
- null-safe comparisons

Example:

```yaml
row_parity:
  enabled: true
  severity: BLOCKING
  compare_columns: null
  exclude_columns:
    - ingestion_timestamp
    - processing_timestamp
    - cloud_run_id
  numeric_tolerances:
    amount: 0.01
```

Use row parity on a controlled validation window or on partitions that failed `partition_hash`.

## Extra AWS -> GCP Parity Ideas

Useful checks to configure through the existing check types:

- Schema: make decimal precision/scale explicit.
- Partition: verify exact business-date coverage.
- Row count: exact count per full validation window.
- Key parity: use real business keys, not surrogate runtime IDs.
- Duplicates: verify uniqueness before row parity.
- Null rate: monitor critical nullable columns.
- Aggregates: check `sum`, `count`, `count_distinct`, `min`, `max`, `avg`.
- Partition hash: use stable business columns only.
- Row parity: exclude ingestion metadata and run-specific columns.

Columns often excluded from row parity:

- ingestion timestamp
- processing timestamp
- cloud-specific metadata
- batch ID
- run ID
- file path
- load timestamp

Columns that deserve aggregate checks:

- money amounts
- quantities
- discounts
- taxes
- created/update timestamps via min/max
- row identifiers via count/count_distinct

Good first production smoke test:

```text
schema
row_count
key_parity
duplicates
aggregates
partition_hash
```

Then add `row_parity` for a small window or for failed partitions.
