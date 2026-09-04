from pyspark.sql import DataFrame
from pyspark.sql import functions as F


VALID_EVENT_TYPES = ["view", "add_to_cart", "purchase", "refund"]


def split_valid_and_bad_events(events: DataFrame, users: DataFrame, products: DataFrame) -> tuple[DataFrame, DataFrame]:
    known_users = users.select("user_id").withColumn("known_user", F.lit(True))
    known_products = products.select("product_id").withColumn("known_product", F.lit(True))

    checked = (
        events.join(known_users, on="user_id", how="left")
        .join(known_products, on="product_id", how="left")
        .withColumn(
            "bad_reason",
            F.when(F.col("event_id").isNull(), F.lit("missing_event_id"))
            .when(~F.col("event_type").isin(VALID_EVENT_TYPES), F.lit("invalid_event_type"))
            .when(F.col("event_timestamp").isNull(), F.lit("missing_event_timestamp"))
            .when(F.col("event_type").isin("purchase", "refund") & F.col("order_id").isNull(), F.lit("missing_order_id"))
            .when(F.col("event_type").isin("purchase", "refund") & (F.col("quantity") <= 0), F.lit("invalid_quantity"))
            .when(F.col("event_type").isin("purchase", "refund") & (F.col("price") <= 0), F.lit("invalid_price"))
            .when(F.col("known_user").isNull(), F.lit("unknown_user"))
            .when(F.col("known_product").isNull(), F.lit("unknown_product")),
        )
        .drop("known_user", "known_product")
    )

    valid = checked.filter(F.col("bad_reason").isNull()).drop("bad_reason")
    bad = checked.filter(F.col("bad_reason").isNotNull())
    return valid, bad
