from datetime import date

from validation.checks.partition_hash import check_partition_hash


def test_partition_hash_passes_for_same_content_in_different_order(spark):
    left_df = spark.createDataFrame(
        [
            (1, date(2026, 9, 1), "DONE", 10.0),
            (2, date(2026, 9, 1), "DONE", 20.0),
            (3, date(2026, 9, 2), None, 30.0),
        ],
        "order_id int, order_date date, status string, amount double",
    )
    right_df = spark.createDataFrame(
        [
            (3, date(2026, 9, 2), None, 30.0),
            (2, date(2026, 9, 1), "DONE", 20.0),
            (1, date(2026, 9, 1), "DONE", 10.0),
        ],
        "order_id int, order_date date, status string, amount double",
    )

    result = check_partition_hash(
        left_df,
        right_df,
        partition_column="order_date",
        business_columns=["order_id", "status", "amount"],
    )

    assert result.status == "PASS"
    assert result.details["mismatched_partition_count"] == 0


def test_partition_hash_fails_when_partition_content_differs(spark):
    left_df = spark.createDataFrame(
        [(1, date(2026, 9, 1), 10.0), (2, date(2026, 9, 1), 20.0)],
        "order_id int, order_date date, amount double",
    )
    right_df = spark.createDataFrame(
        [(1, date(2026, 9, 1), 10.0), (2, date(2026, 9, 1), 21.0)],
        "order_id int, order_date date, amount double",
    )

    result = check_partition_hash(
        left_df,
        right_df,
        partition_column="order_date",
        business_columns=["order_id", "amount"],
    )

    assert result.status == "FAIL"
    assert result.difference == 1
    assert result.details["mismatched_partition_count"] == 1


def test_partition_hash_supports_global_hash_without_partition_column(spark):
    left_df = spark.createDataFrame([(1, "a"), (2, "b")], "id int, value string")
    right_df = spark.createDataFrame([(2, "b"), (1, "a")], "id int, value string")

    result = check_partition_hash(left_df, right_df, partition_column=None)

    assert result.status == "PASS"
