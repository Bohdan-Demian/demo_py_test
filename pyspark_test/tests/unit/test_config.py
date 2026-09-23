from validation.config import load_validation_config


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
