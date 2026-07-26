import pytest

from lakehouse_platform.io.readers import read_input, uc_read


class Catalog:
    def currentCatalog(self):
        return "current_catalog"


class Spark:
    catalog = Catalog()

    def __init__(self):
        self.tables = []

    def table(self, name):
        self.tables.append(name)
        return name


def test_uc_read_qualifies_schema_table_with_explicit_catalog():
    spark = Spark()

    result = uc_read(spark, "silver.gutenberg_work", catalog="dev_lakehouse")

    assert result == "dev_lakehouse.silver.gutenberg_work"


def test_uc_read_accepts_fully_qualified_table():
    spark = Spark()

    result = uc_read(spark, "prod.silver.gutenberg_work")

    assert result == "prod.silver.gutenberg_work"


def test_acon_unity_catalog_reader_uses_uc_read():
    spark = Spark()

    result = read_input(
        spark,
        "unity_catalog_table",
        {"table": "silver.gutenberg_work", "catalog": "dev_lakehouse"},
    )

    assert result == "dev_lakehouse.silver.gutenberg_work"


def test_uc_read_rejects_invalid_table_name():
    with pytest.raises(ValueError, match="schema.table"):
        uc_read(Spark(), "gutenberg_work")
