from validation.runner import run_validation


def test_runner_returns_pass_when_enabled_checks_pass(spark):
    config = {
        "entity": "orders",
        "environment": "unit",
        "keys": ["order_id"],
        "checks": {
            "schema": {"enabled": True, "severity": "BLOCKING"},
            "partition": {"enabled": True, "severity": "BLOCKING", "column": "order_date"},
            "row_count": {"enabled": True, "severity": "BLOCKING", "tolerance_pct": 0},
            "key_parity": {"enabled": True, "severity": "BLOCKING"},
            "duplicates": {"enabled": True, "severity": "BLOCKING"},
            "null_rate": {"enabled": True, "severity": "NON_BLOCKING", "columns": ["amount"]},
            "aggregates": {
                "enabled": True,
                "severity": "BLOCKING",
                "group_by": ["order_date"],
                "metrics": {"amount": ["sum"], "order_id": ["count"]},
            },
            "partition_hash": {
                "enabled": True,
                "severity": "BLOCKING",
                "partition_column": "order_date",
                "business_columns": ["order_id", "amount"],
            },
            "row_parity": {
                "enabled": True,
                "severity": "BLOCKING",
                "exclude_columns": [],
                "numeric_tolerances": {"amount": 0},
            },
        },
    }
    left_df = spark.createDataFrame(
        [(1, "2026-09-01", 10.0), (2, "2026-09-01", 20.0)],
        "order_id int, order_date string, amount double",
    )
    right_df = spark.createDataFrame(
        [(2, "2026-09-01", 20.0), (1, "2026-09-01", 10.0)],
        "order_id int, order_date string, amount double",
    )

    result = run_validation(spark=spark, config=config, left_df=left_df, right_df=right_df, run_id="run-1")

    assert result.run_id == "run-1"
    assert result.entity == "orders"
    assert result.environment == "unit"
    assert result.overall_status == "PASS"
    assert [check.check_name for check in result.checks] == [
        "schema",
        "partition",
        "row_count",
        "key_parity",
        "duplicates",
        "null_rate",
        "aggregates",
        "partition_hash",
        "row_parity",
    ]


def test_runner_returns_fail_when_blocking_check_fails(spark):
    config = {
        "entity": "orders",
        "keys": ["order_id"],
        "checks": {
            "row_count": {"enabled": True, "severity": "BLOCKING", "tolerance_pct": 0},
        },
    }
    left_df = spark.createDataFrame([(1,), (2,)], "order_id int")
    right_df = spark.createDataFrame([(1,)], "order_id int")

    result = run_validation(spark=spark, config=config, left_df=left_df, right_df=right_df)

    assert result.overall_status == "FAIL"
    assert result.has_blocking_failures is True
