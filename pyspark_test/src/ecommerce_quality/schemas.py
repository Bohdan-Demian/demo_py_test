from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

EVENTS_SCHEMA = StructType(
    [
        StructField("event_id", StringType(), False),
        StructField("user_id", StringType(), True),
        StructField("product_id", StringType(), True),
        StructField("event_type", StringType(), True),
        StructField("event_ts", StringType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("price", DoubleType(), True),
        StructField("order_id", StringType(), True),
    ]
)

USERS_SCHEMA = StructType(
    [
        StructField("user_id", StringType(), False),
        StructField("email", StringType(), True),
        StructField("country", StringType(), True),
        StructField("created_at", StringType(), True),
    ]
)

PRODUCTS_SCHEMA = StructType(
    [
        StructField("product_id", StringType(), False),
        StructField("product_name", StringType(), True),
        StructField("category", StringType(), True),
    ]
)

