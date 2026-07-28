"""Drug synergy contracts and ACON wiring (no Spark required).

The contracts here are deliberately Spark-free so the star schema can be checked
without a cluster: keys line up, foreign keys have somewhere to point, and the
grain is what the documentation claims.
"""
from pathlib import Path

import yaml

from products.drug_synergy.tables.gold.dim_cancer_type.contract import (
    TableDefinition as DimCancerType,
)
from products.drug_synergy.tables.gold.dim_cell_line.contract import (
    TableDefinition as DimCellLine,
)
from products.drug_synergy.tables.gold.dim_drug.contract import TableDefinition as DimDrug
from products.drug_synergy.tables.gold.fact_drug_synergy.contract import (
    TableDefinition as FactSynergy,
)
from products.drug_synergy.tables.silver.cell_line.contract import TableDefinition as SilverCellLine
from products.drug_synergy.tables.silver.drug_combination.contract import (
    TableDefinition as SilverCombination,
)

ROOT = Path(__file__).resolve().parents[2]
PIPELINES = ROOT / "products" / "drug_synergy" / "pipelines"


def _acon(name):
    return yaml.safe_load((PIPELINES / name).read_text(encoding="utf-8"))


def test_silver_combination_grain_is_the_canonical_pair_and_cell_line():
    assert SilverCombination.primary_keys() == ["drug_min", "drug_max", "cell_line_key"]
    assert SilverCombination.object_location() == "silver.drug_combination"


def test_silver_merge_keys_equal_the_declared_grain():
    output = next(
        spec for spec in _acon("bronze_to_silver.yaml")["outputs"]
        if spec["id"] == "silver_drug_combination"
    )
    assert output["options"]["keys"] == SilverCombination.primary_keys()


def test_every_fact_foreign_key_has_a_dimension_to_point_at():
    fact = set(FactSynergy.column_names())
    for dimension in (DimDrug, DimCellLine, DimCancerType):
        key = dimension.primary_keys()[0]
        # drug appears twice in the fact, once per side of the pair
        assert key in fact or {"drug_min_key", "drug_max_key"} <= fact, key
    assert {"drug_min_key", "drug_max_key", "cell_line_key", "cancer_type_key"} <= fact


def test_fact_holds_only_keys_and_additive_measures():
    """Descriptive attributes belong in dimensions, not in the fact."""
    descriptive = {"drug_id", "cell_line_id", "oncotree_lineage", "smiles", "cell_line_name"}
    assert descriptive.isdisjoint(FactSynergy.column_names())


def test_cell_line_key_is_the_cross_source_join():
    """DrugComb rows and DepMap rows must agree on one key."""
    assert "cell_line_key" in SilverCombination.column_names()
    assert SilverCellLine.primary_keys() == ["cell_line_key"]


def test_cancer_type_comes_from_the_depmap_lineage():
    assert "oncotree_lineage" in SilverCellLine.column_names()
    assert "oncotree_lineage" in DimCancerType.column_names()


def test_gold_dimensions_are_rebuilt_not_merged():
    """Gold is a deterministic rebuild from Silver, so overwrite is correct."""
    for output in _acon("silver_to_gold.yaml")["outputs"]:
        assert output["writer"] == "delta_table"
        assert output["options"]["mode"] == "overwrite"


def test_bronze_tables_are_append_only():
    from products.drug_synergy.tables.bronze.depmap_expression_raw.contract import (
        TableDefinition as ExpressionRaw,
    )
    from products.drug_synergy.tables.bronze.drugcomb_synergy_raw.contract import (
        TableDefinition as DrugcombRaw,
    )

    for contract in (DrugcombRaw, ExpressionRaw):
        assert contract.table_properties()["delta.appendOnly"] == "true"


def test_landing_reuses_one_callable_for_every_file_source():
    """Config over duplication: three sources, one transformation."""
    callables = {step["callable"] for step in _acon("land_bronze.yaml")["transformations"]}
    assert callables == {"products.drug_synergy.tables.bronze.landing:land"}
