from pathlib import Path

import pytest

from lakehouse_platform.core.acon import Acon, AconError
from lakehouse_platform.engine import resolve_values

ROOT = Path(__file__).resolve().parents[2]


def test_messy_product_acon_is_valid():
    acon = Acon.from_yaml(
        ROOT / "products" / "messy_records" / "pipelines" / "bronze_to_silver.yaml"
    )
    assert acon.pipeline.id == "messy_records_bronze_to_silver"
    assert acon.transformations[0].input_id == "bronze_records"
    assert acon.outputs[0].input_id == "validated_records"


def test_example_works_acon_is_valid():
    acon = Acon.from_yaml(
        ROOT / "products" / "example_works" / "pipelines" / "bronze_to_silver.yaml"
    )
    assert acon.pipeline.id == "example_works_bronze_to_silver"
    assert acon.transformations[0].callable.endswith(":transform")


def test_kimball_gold_acon_has_fact_and_dimensions():
    acon = Acon.from_yaml(
        ROOT / "products" / "example_works" / "pipelines" / "silver_to_gold.yaml"
    )
    outputs = {spec.options["table"] for spec in acon.outputs}
    assert outputs == {
        "${catalog}.gold.dim_work",
        "${catalog}.gold.dim_author",
        "${catalog}.gold.dim_category",
        "${catalog}.gold.dim_date",
        "${catalog}.gold.fact_work",
    }


def test_runtime_variables_resolve_nested_values():
    assert resolve_values(
        {"table": "${catalog}.gold.fact_work"},
        {"catalog": "test_lakehouse"},
    ) == {"table": "test_lakehouse.gold.fact_work"}


def test_acon_rejects_unknown_input_reference():
    with pytest.raises(AconError, match="unknown input_id"):
        Acon.from_dict(
            {
                "pipeline": {"id": "broken"},
                "inputs": [{"id": "source", "reader": "json", "options": {"path": "x"}}],
                "transformations": [
                    {
                        "id": "result",
                        "input_id": "missing",
                        "callable": "some.module:transform",
                    }
                ],
                "outputs": [
                    {
                        "id": "target",
                        "input_id": "result",
                        "writer": "delta_table",
                        "options": {"table": "x"},
                    }
                ],
            }
        )
