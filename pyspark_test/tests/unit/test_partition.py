from datetime import date

from validation.checks.partition import check_partition


def test_partition_passes_when_partition_counts_match(spark):
    left_df = spark.createDataFrame(
        [(1, date(2026, 9, 1)), (2, date(2026, 9, 1)), (3, date(2026, 9, 2))],
        "id int, order_date date",
    )
    right_df = spark.createDataFrame(
        [(10, date(2026, 9, 1)), (20, date(2026, 9, 1)), (30, date(2026, 9, 2))],
        "id int, order_date date",
    )

    result = check_partition(left_df, right_df, partition_column="order_date")

    assert result.status == "PASS"
    assert result.details["mismatched_partition_count"] == 0


def test_partition_fails_when_partition_coverage_or_counts_differ(spark):
    left_df = spark.createDataFrame(
        [(1, date(2026, 9, 1)), (2, date(2026, 9, 1)), (3, date(2026, 9, 2))],
        "id int, order_date date",
    )
    right_df = spark.createDataFrame(
        [(10, date(2026, 9, 1)), (20, date(2026, 9, 3))],
        "id int, order_date date",
    )

    result = check_partition(left_df, right_df, partition_column="order_date")

    assert result.status == "FAIL"
    assert result.details["mismatched_partition_count"] == 3
    assert result.details["missing_on_right_count"] == 1
    assert result.details["extra_on_right_count"] == 1


def test_partition_is_noop_when_partition_column_is_not_configured(spark):
    left_df = spark.createDataFrame([(1,)], "id int")
    right_df = spark.createDataFrame([(1,)], "id int")

    result = check_partition(left_df, right_df, partition_column=None)

    assert result.status == "PASS"
    assert result.details["enabled"] is False
