from ecommerce_quality.pipeline import run_pipeline


def test_run_pipeline_writes_expected_outputs(spark, tmp_path):
    output_path = tmp_path / "processed"

    run_pipeline(
        spark=spark,
        events_path="data/raw/events.csv",
        users_path="data/raw/users.csv",
        products_path="data/raw/products.csv",
        output_path=str(output_path),
    )

    fact_orders = spark.read.parquet(str(output_path / "fact_orders"))
    daily_sales = spark.read.parquet(str(output_path / "daily_sales"))
    bad_records = spark.read.parquet(str(output_path / "bad_records"))

    assert fact_orders.count() == 4
    assert daily_sales.count() == 2
    assert bad_records.count() == 4
