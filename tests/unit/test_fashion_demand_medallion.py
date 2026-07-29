"""Bronze-to-Gold checks that do not need a Spark runtime.

The transformations themselves cannot be executed here, so these tests pin the
agreements between files that would otherwise only break on a cluster: promoted
columns versus the contract, the medallion chain versus the tables it claims to
write, and the merge keys that make a rerun idempotent rather than duplicating.
"""
import ast
from pathlib import Path

import yaml

from lakehouse_platform.core.acon import Acon
from products.fashion_demand.tables.gold.fact_daily_demand import (
    transform as fact_transform,
)
from products.fashion_demand.tables.silver.articles.transform import PROMOTED
from products.fashion_demand.tables.silver.transactions.transform import (
    IDENTIFYING,
    PAYLOAD_FIELDS,
)

ROOT = Path(__file__).resolve().parents[2]
PRODUCT = ROOT / "products/fashion_demand"


def contract_columns(relative: str) -> list[str]:
    tree = ast.parse((PRODUCT / relative / "contract.py").read_text(encoding="utf-8"))
    definition = next(
        node for node in tree.body
        if isinstance(node, ast.ClassDef) and node.name == "TableDefinition"
    )
    return [node.target.id for node in definition.body if isinstance(node, ast.AnnAssign)]


def acon(name: str) -> Acon:
    return Acon.from_yaml(PRODUCT / "pipelines" / f"{name}.yaml")


def test_the_medallion_chain_is_connected_end_to_end():
    """Each stage must read what the previous one writes, or nothing runs."""
    written = {
        spec.options["table"]
        for name in ("land_bronze", "bronze_to_silver", "silver_to_gold")
        for spec in acon(name).outputs
    }
    read = {
        spec.options["table"]
        for name in ("bronze_to_silver", "silver_to_gold", "gold_to_features")
        for spec in acon(name).inputs
        if spec.kind == "unity_catalog_table"
    }
    assert read <= written, f"reads a table nothing writes: {sorted(read - written)}"


def test_the_feature_pipeline_reads_the_fact_the_gold_stage_writes():
    features = next(spec for spec in acon("gold_to_features").inputs)
    gold = {spec.options["table"] for spec in acon("silver_to_gold").outputs}
    assert features.options["table"] in gold


def test_promoted_article_columns_match_the_silver_contract():
    """Promoting a column means editing two files; forgetting one fails here."""
    expected = set(PROMOTED.values()) | {"ingested_at"}
    assert set(contract_columns("tables/silver/articles")) == expected


def test_every_identifying_field_is_a_real_silver_column():
    """The surrogate key is only stable if it hashes columns that exist."""
    columns = set(contract_columns("tables/silver/transactions"))
    assert set(IDENTIFYING) <= columns


def test_the_payload_fields_cover_what_silver_parses():
    """Documents the source's five columns; a sixth would need a decision."""
    assert PAYLOAD_FIELDS == (
        "t_dat", "customer_id", "article_id", "price", "sales_channel_id"
    )


def test_transactions_merge_on_the_surrogate_key_not_the_natural_one():
    """Merging on (date, customer, article) would collapse two real sales."""
    output = next(
        spec for spec in acon("bronze_to_silver").outputs
        if spec.options["table"].endswith("silver.transactions")
    )
    assert output.options["keys"] == ["transaction_id"]


def test_the_fact_merges_on_its_full_grain():
    output = next(
        spec for spec in acon("silver_to_gold").outputs
        if spec.options["table"].endswith("fact_daily_demand")
    )
    assert output.options["keys"] == ["article_id", "sales_channel_id", "demand_date"]


def test_bronze_appends_and_never_merges():
    """Bronze is an immutable record of what arrived; merging would rewrite it."""
    for spec in acon("land_bronze").outputs:
        assert spec.kind == "delta_table"
        assert spec.options["mode"] == "append"


def test_bronze_has_no_quality_gate():
    """Rejecting rows in Bronze would discard the evidence of what went wrong."""
    assert acon("land_bronze").quality == ()


def test_transactions_are_quarantined_rather_than_dropped():
    gate = next(iter(acon("bronze_to_silver").quality))
    assert gate.on_failure == "quarantine"
    assert gate.quarantine_table.endswith("quarantine.hm_transactions")


def test_an_unknown_sales_channel_is_rejected_not_folded_in():
    """A third channel appearing silently would corrupt every series."""
    rules = yaml.safe_load(
        (PRODUCT / "tables/silver/transactions/quality.yaml").read_text(encoding="utf-8")
    )
    rule = next(r for r in rules if r["name"] == "sales_channel_is_known")
    assert rule["criticality"] == "error"
    assert rule["check"]["arguments"]["min_limit"] == 1
    assert rule["check"]["arguments"]["max_limit"] == 2


def test_the_volume_filter_is_off_by_default_in_the_acon():
    """Silently training on a subset of articles would flatter every metric."""
    spec = next(
        spec for spec in acon("silver_to_gold").transformations
        if spec.callable.endswith("fact_daily_demand.transform:transform")
    )
    assert spec.options["min_total_units"] == 0


def test_the_volume_filter_is_a_no_op_at_zero():
    """_busy_enough must return the frame untouched rather than join on nothing."""
    sentinel = object()
    assert fact_transform._busy_enough(sentinel, 0) is sentinel
    assert fact_transform._busy_enough(sentinel, -5) is sentinel
