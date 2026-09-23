import pytest

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
    assert result.suite == "orders_databricks_to_databricks"
    assert result.source_pair == "databricks_to_databricks"
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


def test_runner_uses_runtime_table_overrides_without_mutating_config(monkeypatch, spark):
    loaded_sources = []

    def fake_load_source(spark, source_config, **kwargs):
        loaded_sources.append((source_config.get("type", "databricks"), source_config["table"]))
        return spark.createDataFrame([(1,)], "order_id int")

    monkeypatch.setattr("validation.runner.load_source", fake_load_source)

    config = {
        "entity": "orders",
        "environment": "demo",
        "suite": "orders_databricks_parity",
        "left": {"table": "workspace.default.yaml_aws_orders"},
        "right": {"table": "workspace.default.yaml_gcp_orders"},
        "checks": {
            "row_count": {"enabled": True, "severity": "BLOCKING", "tolerance_pct": 0},
        },
    }

    result = run_validation(
        spark=spark,
        config=config,
        entity="orders",
        environment="dev",
        suite="orders_dev_databricks_parity",
        left_type="databricks",
        right_type="databricks",
        left_table="aws_share.sales.orders",
        right_table="gcp_curated.sales.orders",
    )

    assert loaded_sources == [
        ("databricks", "aws_share.sales.orders"),
        ("databricks", "gcp_curated.sales.orders"),
    ]
    assert result.environment == "dev"
    assert result.suite == "orders_dev_databricks_parity"
    assert result.source_pair == "databricks_to_databricks"
    assert result.overall_status == "PASS"
    assert config["left"]["table"] == "workspace.default.yaml_aws_orders"
    assert config["right"]["table"] == "workspace.default.yaml_gcp_orders"


def test_runner_rejects_mixed_source_types(spark):
    config = {
        "entity": "orders",
        "left": {"type": "snowflake", "table": "ORDERS"},
        "right": {"type": "databricks", "table": "gcp_curated.sales.orders"},
        "checks": {
            "row_count": {"enabled": True, "severity": "BLOCKING", "tolerance_pct": 0},
        },
    }

    with pytest.raises(ValueError, match="matching source types"):
        run_validation(spark=spark, config=config)


def test_runner_tracks_snowflake_to_snowflake_source_pair(spark):
    config = {
        "entity": "orders",
        "left": {"type": "snowflake", "table": "AWS_ORDERS"},
        "right": {"type": "snowflake", "table": "GCP_ORDERS"},
        "checks": {
            "row_count": {"enabled": True, "severity": "BLOCKING", "tolerance_pct": 0},
        },
    }
    left_df = spark.createDataFrame([(1,)], "order_id int")
    right_df = spark.createDataFrame([(1,)], "order_id int")

    result = run_validation(spark=spark, config=config, left_df=left_df, right_df=right_df)

    assert result.suite == "orders_snowflake_to_snowflake"
    assert result.source_pair == "snowflake_to_snowflake"
    assert result.overall_status == "PASS"


def test_runner_write_results_false_skips_reporter_even_when_config_enables_reporting(spark):
    class FakeReporter:
        def __init__(self):
            self.write_calls = 0

        def write(self, result):
            self.write_calls += 1

    reporter = FakeReporter()
    config = {
        "entity": "orders",
        "checks": {
            "row_count": {"enabled": True, "severity": "BLOCKING", "tolerance_pct": 0},
        },
        "reporting": {"enabled": True},
    }
    left_df = spark.createDataFrame([(1,)], "order_id int")
    right_df = spark.createDataFrame([(1,)], "order_id int")

    run_validation(
        spark=spark,
        config=config,
        left_df=left_df,
        right_df=right_df,
        reporter=reporter,
        write_results=False,
    )

    assert reporter.write_calls == 0


def test_runner_write_results_true_uses_reporter(spark):
    class FakeReporter:
        def __init__(self):
            self.write_calls = 0

        def write(self, result):
            self.write_calls += 1

    reporter = FakeReporter()
    config = {
        "entity": "orders",
        "checks": {
            "row_count": {"enabled": True, "severity": "BLOCKING", "tolerance_pct": 0},
        },
    }
    left_df = spark.createDataFrame([(1,)], "order_id int")
    right_df = spark.createDataFrame([(1,)], "order_id int")

    run_validation(
        spark=spark,
        config=config,
        left_df=left_df,
        right_df=right_df,
        reporter=reporter,
        write_results=True,
    )

    assert reporter.write_calls == 1
