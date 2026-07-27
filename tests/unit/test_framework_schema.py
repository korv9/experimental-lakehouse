"""Tests for the messy_records contracts (no Spark required).

These contracts stay importable without a Spark session — the StructType lives
in spark_schema.py — so the checks below can assert that each contract really
describes the table its pipeline builds. That drift is what the ACON
``contract:`` gate now catches at runtime.
"""
import dataclasses
import re
import types
from pathlib import Path

import yaml

from products.messy_records.tables.bronze.records_raw.contract import (
    TableDefinition as Bronze,
)
from products.messy_records.tables.silver.records.contract import (
    TableDefinition as Silver,
)

PRODUCT = Path(__file__).resolve().parents[2] / "products" / "messy_records"
ACON = PRODUCT / "pipelines" / "bronze_to_silver.yaml"
SPARK_SCHEMA = PRODUCT / "tables" / "silver" / "records" / "spark_schema.py"


def _acon():
    return yaml.safe_load(ACON.read_text(encoding="utf-8"))


def _clean_record_fields():
    """Field names of CLEAN_RECORD, read as text so no Spark import is needed."""
    return re.findall(r'StructField\("(\w+)"', SPARK_SCHEMA.read_text(encoding="utf-8"))


def test_contract_locations_match_the_acon_tables():
    acon = _acon()
    assert Bronze.object_location() == "bronze.messy_demo_records"
    assert Silver.object_location() == "silver.records"
    # the ACON reads/writes exactly the tables the contracts describe
    assert acon["inputs"][0]["options"]["table"] == f"${{catalog}}.{Bronze.object_location()}"
    assert acon["outputs"][0]["options"]["table"] == f"${{catalog}}.{Silver.object_location()}"


def test_silver_contract_matches_the_transformation_output():
    # transform does select("c.*", "ingested_at"), so CLEAN_RECORD + ingested_at
    assert Silver.column_names() == [*_clean_record_fields(), "ingested_at"]


def test_silver_primary_key_is_the_merge_key():
    acon = _acon()
    assert Silver.primary_keys() == ["record_id"]
    assert acon["outputs"][0]["options"]["keys"] == Silver.primary_keys()


def test_silver_output_is_contract_validated_by_the_acon():
    contract = _acon()["outputs"][0]["contract"]
    assert contract == "products.messy_records.tables.silver.records.contract:TableDefinition"


def test_bronze_is_append_only_raw_with_no_primary_key():
    columns = Bronze.column_names()
    assert "raw_payload" in columns and "source_record_id" in columns
    assert Bronze.primary_keys() == []  # raw ids may be null or duplicated
    assert Bronze.table_properties().get("delta.appendOnly") == "true"


def test_non_null_silver_columns_are_the_ones_the_quality_gate_guarantees():
    """A column may only be non-nullable if something upstream guarantees it.

    record_id and title are dropped by error-level rules; ingested_at always
    comes from Bronze. Anything else must stay Optional or the contract would
    fail validation on legitimate data.
    """
    rules = yaml.safe_load(
        (PRODUCT / "tables" / "silver" / "records" / "quality.yaml").read_text(encoding="utf-8")
    )
    dropped = {
        rule["check"]["arguments"]["column"]
        for rule in rules
        if rule.get("criticality") == "error"
    }
    assert dropped == {"record_id", "title"}

    required = {
        field.name
        for field in dataclasses.fields(Silver)
        if not isinstance(field.type, types.UnionType) and field.type is not list
    }
    assert required == dropped | {"ingested_at"}
