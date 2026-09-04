from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def clean_events(events: DataFrame) -> DataFrame:
    return (
        events.dropDuplicates(["event_id"])
        .withColumn("event_timestamp", F.to_timestamp("event_ts"))
        .withColumn("event_date", F.to_date("event_timestamp"))
        .drop("event_ts")
    )


def build_fact_orders(events: DataFrame, users: DataFrame, products: DataFrame) -> DataFrame:
    purchases = events.filter(F.col("event_type") == "purchase")

    return (
        purchases.join(users, on="user_id", how="inner")
        .join(products, on="product_id", how="inner")
        .withColumn("revenue", F.round(F.col("quantity") * F.col("price"), 2))
        .select(
            "order_id",
            "event_id",
            "event_date",
            "user_id",
            "country",
            "product_id",
            "category",
            "quantity",
            "price",
            "revenue",
        )
    )


def build_daily_sales(fact_orders: DataFrame) -> DataFrame:
    return (
        fact_orders.groupBy("event_date")
        .agg(
            F.countDistinct("order_id").alias("orders_count"),
            F.sum("quantity").alias("items_sold"),
            F.round(F.sum("revenue"), 2).alias("total_revenue"),
        )
        .orderBy("event_date")
    )


def build_customer_summary(fact_orders: DataFrame) -> DataFrame:
    return (
        fact_orders.groupBy("user_id", "country")
        .agg(
            F.countDistinct("order_id").alias("orders_count"),
            F.round(F.sum("revenue"), 2).alias("total_revenue"),
        )
        .orderBy("user_id")
    )

