from validation.checks.null_rate import check_null_rate


def test_null_rate_passes_when_differences_are_within_tolerance(spark):
    left_df = spark.createDataFrame([(1, "a"), (2, None), (3, None), (4, "d")], "id int, email string")
    right_df = spark.createDataFrame([(1, "a"), (2, None), (3, "c"), (4, "d")], "id int, email string")

    result = check_null_rate(left_df, right_df, columns=["email"], tolerances_pct={"email": 25})

    assert result.status == "PASS"
    assert result.details["per_column"]["email"]["left_null_pct"] == 50
    assert result.details["per_column"]["email"]["right_null_pct"] == 25


def test_null_rate_fails_when_difference_exceeds_column_tolerance(spark):
    left_df = spark.createDataFrame([(1, None), (2, None)], "id int, email string")
    right_df = spark.createDataFrame([(1, "a"), (2, "b")], "id int, email string")

    result = check_null_rate(left_df, right_df, columns=["email"], tolerances_pct={"email": 10})

    assert result.status == "FAIL"
    assert result.details["failed_columns"] == ["email"]
    assert result.details["per_column"]["email"]["difference_pct"] == 100


def test_null_rate_fails_when_configured_column_is_missing(spark):
    left_df = spark.createDataFrame([(1, "a")], "id int, email string")
    right_df = spark.createDataFrame([(1,)], "id int")

    result = check_null_rate(left_df, right_df, columns=["email"])

    assert result.status == "FAIL"
    assert result.details["missing_columns"] == {"left": [], "right": ["email"]}

