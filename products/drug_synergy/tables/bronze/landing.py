"""Shared landing step for this product's file sources.

DrugComb, DepMap Model and DepMap expression differ only in which fields make up
the source record id, so they share one callable and vary by ACON options rather
than by three near-identical modules.

Options:
    source_name    value for the source_name column
    source_file    file the batch came from
    id_fields      payload fields joined with "|" to form source_record_id
    schema_version source release, e.g. "24Q2"
"""
from __future__ import annotations

import uuid

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from lakehouse_platform.observability.progress import progress


def land(raw: DataFrame, options: dict | None = None) -> DataFrame:
    options = options or {}
    source_name = options["source_name"]
    id_fields = options.get("id_fields") or []
    batch_id = options.get("batch_id") or str(uuid.uuid4())
    progress("DRUG_SYNERGY", "Landing raw rows", source=source_name, batch_id=batch_id)

    if id_fields:
        parts = [F.get_json_object("raw_payload", f"$['{field}']") for field in id_fields]
        record_id = F.concat_ws("|", *parts)
    else:
        record_id = F.lit(None).cast("string")

    return raw.select(
        F.lit(source_name).alias("source_name"),
        F.lit(options.get("source_file", "")).alias("source_file"),
        F.current_timestamp().alias("ingested_at"),
        F.lit(batch_id).alias("batch_id"),
        record_id.alias("source_record_id"),
        F.col("raw_payload"),
        F.lit(options.get("schema_version", "v1")).alias("schema_version"),
    )
