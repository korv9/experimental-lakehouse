"""Product-level checks; Lakehouse Engine owns framework-level testing."""

import ast
import os
import shutil
from copy import deepcopy
from pathlib import Path

import pytest
import yaml

from products.example_works.bronze_example_works.notebook import ACON as BRONZE_ACON
from products.example_works.gold_example_works.notebook import ACON as GOLD_ACON
from products.example_works.silver_example_works.notebook import ACON as SILVER_ACON

ROOT = Path(__file__).resolve().parents[1]
PRODUCT = ROOT / "products/example_works"
NOTEBOOKS = (
    PRODUCT / "bronze_example_works/notebook.py",
    PRODUCT / "silver_example_works/notebook.py",
    PRODUCT / "gold_example_works/notebook.py",
)


def test_repository_structure_and_acons() -> None:
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

    for notebook, acon in zip(NOTEBOOKS, (BRONZE_ACON, SILVER_ACON, GOLD_ACON), strict=True):
        tree = ast.parse(notebook.read_text(encoding="utf-8"))
        assert not any(isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) for node in tree.body)
        assert set(acon) == {"input_specs", "transform_specs", "dq_specs", "output_specs"}
        assert acon["output_specs"][0]["input_id"] == acon["dq_specs"][0]["spec_id"]

    assert BRONZE_ACON["output_specs"][0]["db_table"] == SILVER_ACON["input_specs"][0]["db_table"]
    assert SILVER_ACON["output_specs"][0]["db_table"] == GOLD_ACON["input_specs"][0]["db_table"]

    bundle = yaml.safe_load((ROOT / "databricks.yml").read_text(encoding="utf-8"))
    job = bundle["resources"]["jobs"]["example_works"]
    tasks = {task["task_key"]: task for task in job["tasks"]}
    assert set(tasks) == {"bronze", "silver", "gold"}
    assert tasks["silver"]["depends_on"] == [{"task_key": "bronze"}]
    assert tasks["gold"]["depends_on"] == [{"task_key": "silver"}]
    assert all(
        task["libraries"][0]["pypi"]["package"]
        == "lakehouse-engine[dq,sharepoint]==2.1.1"
        for task in tasks.values()
    )


def test_example_works_medallion_pipeline(tmp_path: Path) -> None:
    if not os.getenv("JAVA_HOME") and not shutil.which("java"):
        pytest.skip("JDK 17 is required for the local Spark test")

    pytest.importorskip("pyspark")
    engine = pytest.importorskip("lakehouse_engine.engine")

    def run_to_dataframe(acon: dict, input_df=None):
        local_acon = deepcopy(acon)
        if input_df is not None:
            source = local_acon["input_specs"][0]
            source.pop("db_table", None)
            source.update({"data_format": "dataframe", "df_name": input_df})

        dq_spec = local_acon["dq_specs"][0]
        dq_spec["local_fs_root_dir"] = str(tmp_path / dq_spec["spec_id"])

        output = local_acon["output_specs"][0]
        output.pop("db_table", None)
        output.pop("merge_opts", None)
        output.pop("options", None)
        output.pop("write_type", None)
        output["data_format"] = "dataframe"
        return engine.load_data(acon=local_acon)[output["spec_id"]]

    df_bronze = run_to_dataframe(BRONZE_ACON)
    assert df_bronze.count() == 2

    df_silver = run_to_dataframe(SILVER_ACON, df_bronze)
    silver = {row.work_id: row.asDict() for row in df_silver.collect()}
    assert silver["rec-001"]["title"] == "Example Work One"
    assert silver["rec-001"]["category"] == "fiction"
    assert silver["rec-002"]["publication_year"] == 2010

    df_gold = run_to_dataframe(GOLD_ACON, df_silver)
    gold = {row.category: row.asDict() for row in df_gold.collect()}
    assert gold["fiction"]["work_count"] == 1
    assert gold["fiction"]["tag_count"] == 2
    assert gold["nonfiction"]["author_count"] == 1
