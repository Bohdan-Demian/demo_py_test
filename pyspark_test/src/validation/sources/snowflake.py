from __future__ import annotations

from collections.abc import Mapping
from os import getenv
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


def load_snowflake_table(
    spark: SparkSession,
    table_name: str,
    *,
    options: Mapping[str, Any] | None = None,
    env_options: Mapping[str, str] | None = None,
    window_column: str | None = None,
    window_start: str | None = None,
    window_end: str | None = None,
) -> DataFrame:
    reader_options = _build_options(options or {}, env_options or {})
    reader_options["dbtable"] = table_name

    df = spark.read.format("snowflake").options(**reader_options).load()

    if window_column and window_start:
        df = df.filter(F.col(window_column) >= F.lit(window_start))

    if window_column and window_end:
        df = df.filter(F.col(window_column) < F.lit(window_end))

    return df


def _build_options(options: Mapping[str, Any], env_options: Mapping[str, str]) -> dict[str, str]:
    resolved_options = {key: str(value) for key, value in options.items()}

    for option_name, env_var in env_options.items():
        env_value = getenv(env_var)
        if env_value is None:
            raise ValueError(f"Missing environment variable for Snowflake option '{option_name}': {env_var}")
        resolved_options[option_name] = env_value

    return resolved_options
