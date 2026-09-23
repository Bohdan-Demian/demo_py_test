from datetime import date

from validation.checks.aggregates import check_aggregates


def test_aggregates_pass_when_grouped_metrics_match(spark):
    left_df = spark.createDataFrame(
        [
            (1, date(2026, 9, 1), 10.0),
            (2, date(2026, 9, 1), 15.0),
            (3, date(2026, 9, 2), 20.0),
        ],
        "order_id int, order_date date, revenue double",
    )
    right_df = spark.createDataFrame(
        [
            (10, date(2026, 9, 1), 10.0),
            (20, date(2026, 9, 1), 15.0),
            (30, date(2026, 9, 2), 20.0),
        ],
        "order_id int, order_date date, revenue double",
    )

    result = check_aggregates(
        left_df,
        right_df,
        group_by=["order_date"],
        metrics={"revenue": ["sum"], "order_id": ["count"]},
    )

    assert result.status == "PASS"
    assert result.details["mismatched_group_count"] == 0


def test_aggregates_fail_when_metric_values_differ(spark):
    left_df = spark.createDataFrame([(1, "PL", 10.0), (2, "PL", 15.0)], "order_id int, country string, revenue double")
    right_df = spark.createDataFrame([(1, "PL", 10.0), (2, "PL", 16.0)], "order_id int, country string, revenue double")

    result = check_aggregates(
        left_df,
        right_df,
        group_by=["country"],
        metrics={"revenue": ["sum", "avg"], "order_id": ["count_distinct"]},
        tolerance=0,
    )

    assert result.status == "FAIL"
    assert result.details["mismatched_group_count"] == 1
    assert "sum_revenue" in result.details["mismatch_sample"][0]["mismatched_metrics"]
    assert "avg_revenue" in result.details["mismatch_sample"][0]["mismatched_metrics"]


def test_aggregates_support_global_metrics_without_group_by(spark):
    left_df = spark.createDataFrame([(1, 10.0), (2, 20.0)], "order_id int, revenue double")
    right_df = spark.createDataFrame([(10, 10.0), (20, 20.0)], "order_id int, revenue double")

    result = check_aggregates(
        left_df,
        right_df,
        group_by=[],
        metrics={"revenue": ["min", "max", "avg"], "order_id": ["count"]},
    )

    assert result.status == "PASS"


def test_aggregates_support_date_min_max_metrics(spark):
    left_df = spark.createDataFrame(
        [
            (1, "PL", date(2026, 9, 1)),
            (2, "PL", date(2026, 9, 2)),
        ],
        "customer_id int, country string, signup_date date",
    )
    right_df = spark.createDataFrame(
        [
            (10, "PL", date(2026, 9, 1)),
            (20, "PL", date(2026, 9, 2)),
        ],
        "customer_id int, country string, signup_date date",
    )

    result = check_aggregates(
        left_df,
        right_df,
        group_by=["country"],
        metrics={"signup_date": ["min", "max"]},
    )

    assert result.status == "PASS"


def test_aggregates_fail_when_date_min_max_metrics_differ(spark):
    left_df = spark.createDataFrame(
        [
            (1, "PL", date(2026, 9, 1)),
            (2, "PL", date(2026, 9, 2)),
        ],
        "customer_id int, country string, signup_date date",
    )
    right_df = spark.createDataFrame(
        [
            (10, "PL", date(2026, 9, 1)),
            (20, "PL", date(2026, 9, 3)),
        ],
        "customer_id int, country string, signup_date date",
    )

    result = check_aggregates(
        left_df,
        right_df,
        group_by=["country"],
        metrics={"signup_date": ["min", "max"]},
    )

    assert result.status == "FAIL"
    assert result.details["mismatched_group_count"] == 1
    assert "max_signup_date" in result.details["mismatch_sample"][0]["mismatched_metrics"]
