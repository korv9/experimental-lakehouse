"""Tests for the framework schema layer (no Spark required).

TableDefinitions are importable and introspectable without a Spark session,
which is the whole point of keeping the type markers lazy. These assert the
contract the notebooks and process_job rely on.
"""
from lakehouse_framework.schemas.bronze.messy.records import TableDefinition as Bronze
from lakehouse_framework.schemas.silver.messy.records import TableDefinition as Silver


def test_object_locations():
    assert Bronze.object_location() == "bronze.messy.records"
    assert Silver.object_location() == "silver.messy.records"


def test_silver_columns_and_primary_key():
    cols = Silver.column_names()
    assert cols[0] == "sk_record"
    assert "bk_record_id" in cols
    assert cols[-2:] == ["dp_ingestion_ts", "dp_refresh_ts"]  # audit columns last
    assert Silver.primary_keys() == ["sk_record"]


def test_bronze_is_append_only_raw_with_no_pk():
    cols = Bronze.column_names()
    assert "raw_payload" in cols and "bk_record_id" in cols
    assert Bronze.primary_keys() == []                       # bronze has no PK
    assert Bronze.table_properties().get("delta.appendOnly") == "true"


def test_silver_carries_every_clean_field():
    # the transform renames record_id -> bk_record_id and adds sk_record; every
    # other clean_record field must have a home in the silver contract
    silver_cols = set(Silver.column_names())
    clean_fields = {
        "title", "creators", "summary", "category", "labels", "year", "rating",
        "is_public", "price", "email", "url", "lat", "lon", "language", "updated_at",
    }
    assert clean_fields <= silver_cols
    assert "sk_record" in silver_cols and "bk_record_id" in silver_cols
