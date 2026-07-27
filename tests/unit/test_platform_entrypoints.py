"""Tests for the two notebook-facing entry points (no Spark required).

Notebooks only ever call ``read_table`` (reads) and ``process_job`` (writes), so
these assert the plumbing underneath: ACON variable resolution and dispatch into
the ACON reader registry.
"""
import pytest

from lakehouse_platform.engine import resolve_values
from lakehouse_platform.jobs import process_job, read_table


class _FakeCatalog:
    def currentCatalog(self):  # noqa: N802 - mirrors the Spark API
        return "fallback_catalog"


class _FakeSpark:
    """Records the table name that the reader ultimately asked Spark for."""

    def __init__(self):
        self.catalog = _FakeCatalog()
        self.requested = None

    def table(self, name):
        self.requested = name
        return f"dataframe:{name}"


def test_resolve_values_substitutes_and_rejects_unknown():
    assert resolve_values("${catalog}.silver.works", {"catalog": "dev"}) == "dev.silver.works"
    assert resolve_values({"table": "${catalog}.x"}, {"catalog": "dev"}) == {"table": "dev.x"}
    with pytest.raises(ValueError):
        resolve_values("${missing}.table", {"catalog": "dev"})


def test_read_table_qualifies_two_part_names_with_the_catalog():
    spark = _FakeSpark()
    result = read_table(spark, "bronze.gutenberg_catalog_raw", catalog="dev_lakehouse")
    assert spark.requested == "dev_lakehouse.bronze.gutenberg_catalog_raw"
    assert result == "dataframe:dev_lakehouse.bronze.gutenberg_catalog_raw"


def test_read_table_resolves_acon_style_variables():
    spark = _FakeSpark()
    read_table(spark, "${catalog}.silver.works", catalog="dev_lakehouse")
    assert spark.requested == "dev_lakehouse.silver.works"


def test_read_table_rejects_unsupported_reader():
    with pytest.raises(ValueError):
        read_table(_FakeSpark(), "bronze.x", catalog="dev", reader="carrier_pigeon")


def test_process_job_requires_one_of_acon_or_job_config():
    with pytest.raises(ValueError):
        process_job(_FakeSpark(), catalog="dev")
