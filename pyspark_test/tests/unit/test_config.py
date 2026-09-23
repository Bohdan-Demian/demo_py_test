from pathlib import Path

import pytest

from validation.config import load_validation_config

PROJECT_ROOT = Path(__file__).resolve().parents[2]


def test_load_validation_config_reads_yaml_and_applies_runtime_overrides(tmp_path):
    config_path = tmp_path / "demo.yaml"
    config_path.write_text(
        """
entity: orders
environment: demo
left:
  table: workspace.default.aws_orders
right:
  table: workspace.default.gcp_orders
checks:
  row_count:
    enabled: true
""",
        encoding="utf-8",
    )

    config = load_validation_config(config_path, entity="customers", environment="prod")

    assert config["entity"] == "customers"
    assert config["environment"] == "prod"
    assert config["left"]["table"] == "workspace.default.aws_orders"
    assert config["checks"]["row_count"]["enabled"] is True


@pytest.mark.parametrize(
    ("config_name", "entity", "table"),
    [
        ("customers.yaml", "customers", "workspace.default.customerscsv"),
        ("orders.yaml", "orders", "workspace.default.orderscsv"),
        ("products.yaml", "products", "workspace.default.productscsv"),
    ],
)
def test_databricks_self_compare_configs_are_loadable(config_name, entity, table):
    config = load_validation_config(PROJECT_ROOT / "configs" / config_name)

    assert config["entity"] == entity
    assert config["left"] == {"type": "databricks", "table": table}
    assert config["right"] == {"type": "databricks", "table": table}
    assert config["keys"]
    assert config["checks"]["schema"]["enabled"] is True
    assert config["checks"]["row_count"]["enabled"] is True
    assert config["reporting"]["enabled"] is False
