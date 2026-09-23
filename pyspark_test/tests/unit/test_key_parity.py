from validation.checks.key_parity import check_key_parity


def test_key_parity_passes_when_distinct_keys_match(spark):
    left_df = spark.createDataFrame([(1, "a"), (2, "b")], "id int, value string")
    right_df = spark.createDataFrame([(2, "x"), (1, "y")], "id int, value string")

    result = check_key_parity(left_df, right_df, key_columns=["id"])

    assert result.status == "PASS"
    assert result.details["left_distinct_key_count"] == 2
    assert result.details["right_distinct_key_count"] == 2


def test_key_parity_fails_and_returns_limited_samples(spark):
    left_df = spark.createDataFrame([(1, "a"), (2, "b"), (3, "c")], "id int, value string")
    right_df = spark.createDataFrame([(1, "a"), (2, "b"), (4, "d")], "id int, value string")

    result = check_key_parity(left_df, right_df, key_columns=["id"], sample_limit=1)

    assert result.status == "FAIL"
    assert result.difference == {"missing_on_right": 1, "extra_on_right": 1}
    assert len(result.details["missing_on_right_sample"]) == 1
    assert len(result.details["extra_on_right_sample"]) == 1


def test_key_parity_supports_composite_keys(spark):
    left_df = spark.createDataFrame([(1, "PL"), (1, "UA")], "id int, country string")
    right_df = spark.createDataFrame([(1, "PL"), (1, "DE")], "id int, country string")

    result = check_key_parity(left_df, right_df, key_columns=["id", "country"])

    assert result.status == "FAIL"
    assert result.details["missing_on_right_count"] == 1
    assert result.details["extra_on_right_count"] == 1
