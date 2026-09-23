from validation.checks.schema import check_schema


def test_schema_passes_when_columns_and_types_match_regardless_order(spark):
    left_df = spark.createDataFrame([(1, "a")], "id int, name string")
    right_df = spark.createDataFrame([("a", 1)], "name string, id int")

    result = check_schema(left_df, right_df)

    assert result.status == "PASS"
    assert result.details["missing_columns"] == []
    assert result.details["extra_columns"] == []
    assert result.details["type_mismatches"] == {}


def test_schema_fails_when_columns_or_types_do_not_match(spark):
    left_df = spark.createDataFrame([(1, "a")], "id int, name string")
    right_df = spark.createDataFrame([(1.0, "x")], "id double, status string")

    result = check_schema(left_df, right_df)

    assert result.status == "FAIL"
    assert result.details["missing_columns"] == ["name"]
    assert result.details["extra_columns"] == ["status"]
    assert result.details["type_mismatches"] == {"id": {"left": "int", "right": "double"}}


def test_schema_can_enforce_column_order(spark):
    left_df = spark.createDataFrame([(1, "a")], "id int, name string")
    right_df = spark.createDataFrame([("a", 1)], "name string, id int")

    result = check_schema(left_df, right_df, enforce_order=True)

    assert result.status == "FAIL"
    assert result.details["order_mismatch"] is True
