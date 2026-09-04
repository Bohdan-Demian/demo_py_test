from pyspark.sql import DataFrame, SparkSession

from ecommerce_quality.schemas import EVENTS_SCHEMA, PRODUCTS_SCHEMA, USERS_SCHEMA


def read_events(spark: SparkSession, path: str) -> DataFrame:
    return spark.read.option("header", True).schema(EVENTS_SCHEMA).csv(path)


def read_users(spark: SparkSession, path: str) -> DataFrame:
    return spark.read.option("header", True).schema(USERS_SCHEMA).csv(path)


def read_products(spark: SparkSession, path: str) -> DataFrame:
    return spark.read.option("header", True).schema(PRODUCTS_SCHEMA).csv(path)


def write_parquet(df: DataFrame, path: str) -> None:
    df.write.mode("overwrite").parquet(path)


