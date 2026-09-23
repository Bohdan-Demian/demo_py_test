from validation.checks.row_parity import check_row_parity


def test_row_parity_passes_for_matching_rows_in_different_order(spark):
    left_df = spark.createDataFrame([(1, "DONE", 10.0), (2, None, 20.0)], "id int, status string, amount double")
    right_df = spark.createDataFrame([(2, None, 20.0), (1, "DONE", 10.0)], "id int, status string, amount double")

    result = check_row_parity(left_df, right_df, key_columns=["id"])

    assert result.status == "PASS"
    assert result.details["compared_row_count"] == 2
    assert result.details["mismatched_row_count"] == 0


def test_row_parity_fails_for_missing_extra_and_changed_rows(spark):
    left_df = spark.createDataFrame(
        [(1, "DONE", 10.0), (2, "DONE", 20.0), (3, "DONE", 30.0)],
        "id int, status string, amount double",
    )
    right_df = spark.createDataFrame(
        [(1, "DONE", 10.0), (2, "FAILED", 20.0), (4, "DONE", 40.0)],
        "id int, status string, amount double",
    )

    result = check_row_parity(left_df, right_df, key_columns=["id"])

    assert result.status == "FAIL"
    assert result.difference == {"missing_on_right": 1, "extra_on_right": 1, "mismatched_rows": 1}
    assert result.details["mismatch_counts_by_column"]["status"] == 1
    assert result.details["missing_on_right_sample"] == [{"id": 3}]
    assert result.details["extra_on_right_sample"] == [{"id": 4}]


def test_row_parity_respects_numeric_tolerance(spark):
    left_df = spark.createDataFrame([(1, 10.0), (2, 20.0)], "id int, amount double")
    right_df = spark.createDataFrame([(1, 10.01), (2, 20.5)], "id int, amount double")

    result = check_row_parity(
        left_df,
        right_df,
        key_columns=["id"],
        numeric_tolerances={"amount": 0.05},
    )

    assert result.status == "FAIL"
    assert result.details["mismatched_row_count"] == 1
    assert result.details["mismatch_counts_by_column"]["amount"] == 1


def test_row_parity_ignores_excluded_columns(spark):
    left_df = spark.createDataFrame([(1, "DONE", "run-a")], "id int, status string, run_id string")
    right_df = spark.createDataFrame([(1, "DONE", "run-b")], "id int, status string, run_id string")

    result = check_row_parity(left_df, right_df, key_columns=["id"], exclude_columns=["run_id"])

    assert result.status == "PASS"
    assert result.details["compare_columns"] == ["status"]


def test_row_parity_supports_composite_keys(spark):
    left_df = spark.createDataFrame([(1, "PL", 10.0), (1, "UA", 20.0)], "id int, country string, amount double")
    right_df = spark.createDataFrame([(1, "PL", 10.0), (1, "UA", 21.0)], "id int, country string, amount double")

    result = check_row_parity(left_df, right_df, key_columns=["id", "country"])

    assert result.status == "FAIL"
    assert result.details["mismatched_row_count"] == 1
