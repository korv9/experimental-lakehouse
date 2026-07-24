from products.example_works.tables.gold.dim_author.contract import (
    TableDefinition as DimAuthor,
)
from products.example_works.tables.gold.dim_category.contract import (
    TableDefinition as DimCategory,
)
from products.example_works.tables.gold.dim_date.contract import (
    TableDefinition as DimDate,
)
from products.example_works.tables.gold.dim_work.contract import (
    TableDefinition as DimWork,
)
from products.example_works.tables.gold.fact_work.contract import (
    TableDefinition as FactWork,
)


def test_gold_table_locations_are_explicit():
    assert {
        DimWork.object_location(),
        DimAuthor.object_location(),
        DimCategory.object_location(),
        DimDate.object_location(),
        FactWork.object_location(),
    } == {
        "gold.dim_work",
        "gold.dim_author",
        "gold.dim_category",
        "gold.dim_date",
        "gold.fact_work",
    }


def test_fact_grain_and_foreign_keys_are_documented():
    assert FactWork.Meta.grain == "One row per current work"
    assert FactWork.Meta.foreign_keys == {
        "work_key": "gold.dim_work.work_key",
        "author_key": "gold.dim_author.author_key",
        "category_key": "gold.dim_category.category_key",
        "date_key": "gold.dim_date.date_key",
    }
