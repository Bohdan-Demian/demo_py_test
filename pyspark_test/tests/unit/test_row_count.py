from validation.checks.row_count import check_row_count


def test_row_count_passes_when_counts_match(spark):
    left_df = spark.createDataFrame([(1,), (2,)], "id int")
    right_df = spark.createDataFrame([(10,), (20,)], "id int")

    result = check_row_count(left_df, right_df)

    assert result.status == "PASS"
    assert result.left_value == 2
    assert result.right_value == 2
    assert result.difference == 0


def test_row_count_fails_when_difference_exceeds_tolerance(spark):
    left_df = spark.createDataFrame([(1,), (2,), (3,), (4,)], "id int")
    right_df = spark.createDataFrame([(1,), (2,)], "id int")

    result = check_row_count(left_df, right_df, tolerance_pct=10)

    assert result.status == "FAIL"
    assert result.details["absolute_difference"] == 2
    assert result.details["percentage_difference"] == 50


def test_row_count_passes_when_difference_is_inside_tolerance(spark):
    left_df = spark.createDataFrame([(1,), (2,), (3,), (4,)], "id int")
    right_df = spark.createDataFrame([(1,), (2,), (3,)], "id int")

    result = check_row_count(left_df, right_df, tolerance_pct=25)

    assert result.status == "PASS"
