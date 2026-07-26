from __future__ import annotations

from products.philosophy_litterature.tables.bronze.gutenberg_catalog_raw.contract import (
    TableDefinition as BronzeCatalog,
)
from products.philosophy_litterature.tables.silver.gutenberg_work.contract import (
    TableDefinition as SilverCatalog,
)
from products.philosophy_litterature.tables.silver.philosophy_litterature_work.contract import (
    TableDefinition as PhilosophyWork,
)


def test_bronze_contract_matches_ingestion_envelope():
    assert BronzeCatalog.object_location() == "bronze.gutenberg_catalog_raw"
    assert BronzeCatalog.primary_keys() == ["ingestion_id"]
    assert BronzeCatalog.column_names() == [
        "ingestion_id",
        "source_name",
        "source_url",
        "source_file",
        "source_checksum",
        "source_modified_at",
        "source_snapshot_date",
        "ingested_at",
        "run_id",
        "source_record_id",
        "raw_payload",
        "schema_version",
    ]
    assert SilverCatalog.object_location() == "silver.gutenberg_work"
    assert SilverCatalog.primary_keys() == ["gutenberg_id"]
    assert PhilosophyWork.object_location() == "silver.philosophy_litterature_work"
    assert PhilosophyWork.primary_keys() == ["corpus_work_id"]
