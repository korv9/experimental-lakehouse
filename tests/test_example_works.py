"""Focused product checks; Lakehouse Engine owns framework-level testing."""

import ast
import json
import os
import runpy
import shutil
from copy import deepcopy
from pathlib import Path
from unittest.mock import patch

import pytest
import yaml

from products.example_works.bronze_example_works.notebook import (
    BRONZE_TABLE,
)
from products.example_works.bronze_example_works.notebook import (
    READ_ACON as BRONZE_READ_ACON,
)
from products.example_works.gold_example_works.notebook import (
    GOLD_TABLE,
)
from products.example_works.gold_example_works.notebook import (
    READ_ACON as GOLD_READ_ACON,
)
from products.example_works.silver_example_works.notebook import (
    READ_ACON as SILVER_READ_ACON,
)
from products.example_works.silver_example_works.notebook import REJECTED_TABLE, SILVER_TABLE

ROOT = Path(__file__).resolve().parents[1]
PRODUCT = ROOT / "products/example_works"
DATASET = ROOT / "datasets/example_works/works.json"
NOTEBOOKS = (
    PRODUCT / "bronze_example_works/notebook.py",
    PRODUCT / "silver_example_works/notebook.py",
    PRODUCT / "gold_example_works/notebook.py",
)
READ_ACONS = (BRONZE_READ_ACON, SILVER_READ_ACON, GOLD_READ_ACON)


def test_repository_structure_and_pipeline_contract() -> None:
    expected_files = {
        "README.md",
        "__init__.py",
        "bronze_example_works/__init__.py",
        "bronze_example_works/notebook.py",
        "silver_example_works/__init__.py",
        "silver_example_works/notebook.py",
        "gold_example_works/__init__.py",
        "gold_example_works/notebook.py",
    }
    actual_files = {
        path.relative_to(PRODUCT).as_posix()
        for path in PRODUCT.rglob("*")
        if path.is_file() and "__pycache__" not in path.parts
    }
    assert actual_files == expected_files

    for notebook, read_acon in zip(NOTEBOOKS, READ_ACONS, strict=True):
        source = notebook.read_text(encoding="utf-8")
        tree = ast.parse(source)

        assert not any(isinstance(node, ast.FunctionDef) for node in tree.body)
        assert "from pyspark.sql" in source
        assert '"data_format": "dataframe"' in source
        assert set(read_acon) == {"input_specs", "output_specs"}

    assert BRONZE_TABLE == SILVER_READ_ACON["input_specs"][0]["db_table"]
    assert SILVER_TABLE == GOLD_READ_ACON["input_specs"][0]["db_table"]

    raw = json.loads(DATASET.read_text(encoding="utf-8"))
    records = raw["records"]
    normalized_ids = [record["id"].strip().upper() for record in records]
    assert len(records) == 16
    assert len(normalized_ids) > len(set(normalized_ids))
    assert any(not record["id"].strip() for record in records)
    assert {"draft", "archived"} <= {record["status"].lower() for record in records}
    assert any(record["year"] == "N/A" for record in records)

    bundle = yaml.safe_load((ROOT / "databricks.yml").read_text(encoding="utf-8"))
    tasks = {
        task["task_key"]: task
        for task in bundle["resources"]["jobs"]["example_works"]["tasks"]
    }
    assert set(tasks) == {"bronze", "silver", "gold"}
    assert tasks["silver"]["depends_on"] == [{"task_key": "bronze"}]
    assert tasks["gold"]["depends_on"] == [{"task_key": "silver"}]
    assert all(
        task["libraries"][0]["pypi"]["package"]
        == "lakehouse-engine[dq,sharepoint]==2.1.1"
        for task in tasks.values()
    )


def test_example_works_medallion_pipeline(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    if not os.getenv("JAVA_HOME") and not shutil.which("java"):
        pytest.skip("JDK 17 is required for the local Spark test")

    pytest.importorskip("pyspark")
    engine = pytest.importorskip("lakehouse_engine.engine")
    real_load_data = engine.load_data
    tables = {}

    def execute(acon: dict):
        source = acon["input_specs"][0]
        outputs = acon["output_specs"]

        if source["data_format"] == "delta":
            return {outputs[0]["spec_id"]: tables[source["db_table"]]}

        if all(output["data_format"] == "dataframe" for output in outputs):
            return real_load_data(acon=acon)

        local_acon = {
            "input_specs": [dict(spec) for spec in acon["input_specs"]],
            "dq_specs": deepcopy(acon.get("dq_specs", [])),
            "output_specs": [
                {
                    "spec_id": output["spec_id"],
                    "input_id": output["input_id"],
                    "data_format": "dataframe",
                }
                for output in outputs
            ],
        }
        for dq_spec in local_acon["dq_specs"]:
            dq_spec["local_fs_root_dir"] = str(tmp_path / dq_spec["spec_id"])

        result = real_load_data(acon=local_acon)
        for output in outputs:
            tables[output["db_table"]] = result[output["spec_id"]]
        return result

    monkeypatch.setenv("EXAMPLE_WORKS_PREVIEW", "false")
    with patch.object(engine, "load_data", new=execute):
        for notebook in NOTEBOOKS:
            runpy.run_path(str(notebook), run_name="__main__")

    assert tables[BRONZE_TABLE].count() == 16

    silver = {row.work_id: row.asDict() for row in tables[SILVER_TABLE].collect()}
    assert len(silver) == 6
    assert silver["WK-001"]["title"] == "The Northern Light - revised"
    assert silver["WK-001"]["tags"] == ["novel", "nordic", "award winner"]
    assert silver["WK-002"]["category"] == "nonfiction"
    assert silver["WK-003"]["language"] == "en"
    assert silver["WK-010"]["price"] == 0

    rejected = tables[REJECTED_TABLE].collect()
    reasons = {reason for row in rejected for reason in row.rejection_reasons}
    assert len(rejected) == 10
    assert {
        "invalid_price",
        "invalid_rating",
        "invalid_updated_at",
        "missing_work_id",
        "not_published",
        "superseded_duplicate",
    } <= reasons

    gold = {
        (row.category, row.publication_decade): row.asDict()
        for row in tables[GOLD_TABLE].collect()
    }
    assert len(gold) == 5
    assert gold[("nonfiction", 2010)]["work_count"] == 2
    assert gold[("fiction", 1990)]["average_rating"] == 4.8
    assert gold[("science_fiction", 1980)]["priced_work_count"] == 0
