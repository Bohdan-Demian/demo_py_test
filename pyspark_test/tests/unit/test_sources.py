import pytest

from validation.sources.loader import load_source
from validation.sources.snowflake import _build_options


def test_snowflake_options_can_be_resolved_from_environment(monkeypatch):
    monkeypatch.setenv("SNOWFLAKE_PASSWORD", "secret-value")

    options = _build_options(
        {"sfURL": "account.snowflakecomputing.com", "sfUser": "validation_user"},
        {"sfPassword": "SNOWFLAKE_PASSWORD"},
    )

    assert options == {
        "sfURL": "account.snowflakecomputing.com",
        "sfUser": "validation_user",
        "sfPassword": "secret-value",
    }


def test_snowflake_env_option_requires_existing_environment_variable():
    with pytest.raises(ValueError, match="Missing environment variable"):
        _build_options({}, {"sfPassword": "MISSING_SNOWFLAKE_PASSWORD"})


def test_load_source_rejects_unknown_source_type(spark):
    with pytest.raises(ValueError, match="Unsupported source type"):
        load_source(spark, {"type": "unknown", "table": "demo.table"})
