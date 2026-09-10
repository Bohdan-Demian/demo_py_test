from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


def load_table(
    spark: SparkSession,
    table_name: str,
    *,
    window_column: str | None = None,
    window_start: str | None = None,
    window_end: str | None = None,
) -> DataFrame:
    df = spark.table(table_name)

    if window_column and window_start:
        df = df.filter(F.col(window_column) >= F.lit(window_start))

    if window_column and window_end:
        df = df.filter(F.col(window_column) < F.lit(window_end))

    return df

