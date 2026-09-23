from validation.checks.duplicates import check_duplicates


def test_duplicates_passes_when_keys_are_unique_on_both_sides(spark):
    left_df = spark.createDataFrame([(1,), (2,)], "id int")
    right_df = spark.createDataFrame([(1,), (2,)], "id int")

    result = check_duplicates(left_df, right_df, key_columns=["id"])

    assert result.status == "PASS"
    assert result.details["left_duplicate_key_count"] == 0
    assert result.details["right_duplicate_key_count"] == 0


def test_duplicates_fails_when_duplicate_keys_exist(spark):
    left_df = spark.createDataFrame([(1,), (1,), (2,)], "id int")
    right_df = spark.createDataFrame([(1,), (2,), (2,)], "id int")

    result = check_duplicates(left_df, right_df, key_columns=["id"])

    assert result.status == "FAIL"
    assert result.left_value == 1
    assert result.right_value == 1
    assert result.difference == 2
