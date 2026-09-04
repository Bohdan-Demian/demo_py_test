import argparse

from pyspark.sql import SparkSession

from ecommerce_quality.io import read_events, read_products, read_users, write_parquet
from ecommerce_quality.quality import split_valid_and_bad_events
from ecommerce_quality.transformations import (
    build_customer_summary,
    build_daily_sales,
    build_fact_orders,
    clean_events,
)


def create_spark(app_name: str = "ecommerce-quality-pipeline") -> SparkSession:
    return (
        SparkSession.builder.appName(app_name)
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )


def run_pipeline(spark: SparkSession, events_path: str, users_path: str, products_path: str, output_path: str) -> None:
    events = clean_events(read_events(spark, events_path))
    users = read_users(spark, users_path)
    products = read_products(spark, products_path)

    valid_events, bad_records = split_valid_and_bad_events(events, users, products)
    fact_orders = build_fact_orders(valid_events, users, products)
    daily_sales = build_daily_sales(fact_orders)
    customer_summary = build_customer_summary(fact_orders)

    write_parquet(fact_orders, f"{output_path}/fact_orders")
    write_parquet(daily_sales, f"{output_path}/daily_sales")
    write_parquet(customer_summary, f"{output_path}/customer_summary")
    write_parquet(bad_records, f"{output_path}/bad_records")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--events", required=True)
    parser.add_argument("--users", required=True)
    parser.add_argument("--products", required=True)
    parser.add_argument("--output", required=True)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    spark = create_spark()
    try:
        run_pipeline(spark, args.events, args.users, args.products, args.output)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

