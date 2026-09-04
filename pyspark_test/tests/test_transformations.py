from datetime import date

from chispa.dataframe_comparer import assert_df_equality

from ecommerce_quality.transformations import build_customer_summary, build_daily_sales, build_fact_orders, clean_events


def test_clean_events_removes_duplicates_and_adds_dates(spark):
    events = spark.createDataFrame(
        [
            ("evt-1", "u-1", "p-1", "purchase", "2026-05-18T10:00:00", 1, 10.0, "ord-1"),
            ("evt-1", "u-1", "p-1", "purchase", "2026-05-18T10:00:00", 1, 10.0, "ord-1"),
        ],
        "event_id string, user_id string, product_id string, event_type string, event_ts string, quantity int, price double, order_id string",
    )

    result = clean_events(events)

    assert result.count() == 1
    assert "event_timestamp" in result.columns
    assert "event_date" in result.columns


def test_build_fact_orders_enriches_purchases_and_calculates_revenue(spark):
    events = spark.createDataFrame(
        [("evt-1", "u-1", "p-1", "purchase", None, 2, 10.0, "ord-1", date(2026, 5, 18))],
        "event_id string, user_id string, product_id string, event_type string, event_timestamp timestamp, quantity int, price double, order_id string, event_date date",
    )
    users = spark.createDataFrame([("u-1", "alice@example.com", "PL", "2026-01-01")], "user_id string, email string, country string, created_at string")
    products = spark.createDataFrame([("p-1", "Laptop", "Electronics")], "product_id string, product_name string, category string")

    result = build_fact_orders(events, users, products)
    expected = spark.createDataFrame(
        [("ord-1", "evt-1", date(2026, 5, 18), "u-1", "PL", "p-1", "Electronics", 2, 10.0, 20.0)],
        "order_id string, event_id string, event_date date, user_id string, country string, product_id string, category string, quantity int, price double, revenue double",
    )

    assert_df_equality(result, expected, ignore_nullable=True, ignore_row_order=True)


def test_build_daily_sales_aggregates_orders(spark):
    fact_orders = spark.createDataFrame(
        [
            ("ord-1", date(2026, 5, 18), "u-1", 2, 10.0, 20.0),
            ("ord-2", date(2026, 5, 18), "u-2", 1, 15.5, 15.5),
        ],
        "order_id string, event_date date, user_id string, quantity int, price double, revenue double",
    )
    expected = spark.createDataFrame(
        [(date(2026, 5, 18), 2, 3, 35.5)],
        "event_date date, orders_count long, items_sold long, total_revenue double",
    )

    assert_df_equality(build_daily_sales(fact_orders), expected, ignore_nullable=True, ignore_row_order=True)


def test_build_customer_summary_aggregates_by_customer(spark):
    fact_orders = spark.createDataFrame(
        [
            ("ord-1", "u-1", "PL", 20.0),
            ("ord-2", "u-1", "PL", 15.5),
            ("ord-3", "u-2", "UA", 7.0),
        ],
        "order_id string, user_id string, country string, revenue double",
    )
    expected = spark.createDataFrame(
        [
            ("u-1", "PL", 2, 35.5),
            ("u-2", "UA", 1, 7.0),
        ],
        "user_id string, country string, orders_count long, total_revenue double",
    )

    assert_df_equality(build_customer_summary(fact_orders), expected, ignore_nullable=True, ignore_row_order=True)
