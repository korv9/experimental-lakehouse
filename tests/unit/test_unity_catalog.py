import pytest

from lakehouse_platform.metadata.unity_catalog import (
    UnityCatalogLayout,
    create_unity_catalog_objects,
)


def test_layout_builds_three_part_names_and_governed_volume_paths():
    layout = UnityCatalogLayout("dev_lakehouse")

    assert layout.table("bronze", "philosophy_litterature_work_raw") == (
        "dev_lakehouse.bronze.philosophy_litterature_work_raw"
    )
    assert layout.source_path("philosophy_litterature", "gutenberg", "1497.txt") == (
        "/Volumes/dev_lakehouse/landing/source_files/"
        "philosophy_litterature/gutenberg/1497.txt"
    )
    assert layout.checkpoint_path("philosophy_litterature", "gutendex") == (
        "/Volumes/dev_lakehouse/platform/checkpoints/philosophy_litterature/gutendex"
    )


def test_layout_rejects_invalid_identifiers_and_path_traversal():
    with pytest.raises(ValueError, match="catalog"):
        UnityCatalogLayout("dev-lakehouse")
    with pytest.raises(ValueError, match="Unsafe"):
        UnityCatalogLayout("dev_lakehouse").source_path("books", "../secret")


class SparkRecorder:
    def __init__(self):
        self.statements = []

    def sql(self, statement):
        self.statements.append(statement)


def test_setup_creates_schemas_and_managed_volumes():
    spark = SparkRecorder()

    create_unity_catalog_objects(
        spark,
        UnityCatalogLayout("dev_lakehouse"),
        create_catalog=True,
    )

    statements = "\n".join(spark.statements)
    assert "CREATE CATALOG IF NOT EXISTS dev_lakehouse" in statements
    assert "CREATE SCHEMA IF NOT EXISTS dev_lakehouse.landing" in statements
    assert "CREATE VOLUME IF NOT EXISTS dev_lakehouse.landing.source_files" in statements
    assert "CREATE VOLUME IF NOT EXISTS dev_lakehouse.platform.checkpoints" in statements
