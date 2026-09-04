from ecommerce_quality.quality import split_valid_and_bad_events
from ecommerce_quality.transformations import clean_events


def test_split_valid_and_bad_events_flags_invalid_records(spark):
    events = spark.createDataFrame(
        [
            ("evt-1", "u-1", "p-1", "purchase", "2026-05-18T10:00:00", 1, 10.0, "ord-1"),
            ("evt-2", "u-404", "p-1", "purchase", "2026-05-18T10:00:00", 1, 10.0, "ord-2"),
            ("evt-3", "u-1", "p-1", "purchase", "2026-05-18T10:00:00", -1, 10.0, "ord-3"),
            ("evt-4", "u-1", "p-1", "purchase", "2026-05-18T10:00:00", 1, -10.0, "ord-4"),
        ],
        "event_id string, user_id string, product_id string, event_type string, event_ts string, quantity int, price double, order_id string",
    )
    users = spark.createDataFrame([("u-1", "alice@example.com", "PL", "2026-01-01")], "user_id string, email string, country string, created_at string")
    products = spark.createDataFrame([("p-1", "Laptop", "Electronics")], "product_id string, product_name string, category string")

    valid, bad = split_valid_and_bad_events(clean_events(events), users, products)

    assert valid.count() == 1
    assert bad.count() == 3
    assert {row.bad_reason for row in bad.select("bad_reason").collect()} == {
        "unknown_user",
        "invalid_quantity",
        "invalid_price",
    }

