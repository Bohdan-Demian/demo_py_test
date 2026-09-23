from __future__ import annotations

from typing import Any

from pyspark.sql import DataFrame, SparkSession

from validation.sources.databricks import load_table as load_databricks_table
from validation.sources.snowflake import load_snowflake_table


def load_source(
    spark: SparkSession,
    source_config: dict[str, Any],
    *,
    window_column: str | None = None,
    window_start: str | None = None,
    window_end: str | None = None,
) -> DataFrame:
    source_type = source_config.get("type", "databricks").lower()
    table_name = source_config["table"]

    if source_type == "databricks":
        return load_databricks_table(
            spark,
            table_name,
            window_column=window_column,
            window_start=window_start,
            window_end=window_end,
        )

    if source_type == "snowflake":
        return load_snowflake_table(
            spark,
            table_name,
            options=source_config.get("options", {}),
            env_options=source_config.get("env_options", {}),
            window_column=window_column,
            window_start=window_start,
            window_end=window_end,
        )

    raise ValueError(f"Unsupported source type: {source_type}")
