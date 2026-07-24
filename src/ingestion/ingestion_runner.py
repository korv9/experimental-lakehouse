"""Configuration-driven ingestion: API -> Bronze (append-only).

Reads a source YAML from config/sources/, pulls every page from the API, and
lands the raw JSON records in the bronze table alongside the standard
technical-metadata columns. No cleaning happens here: bronze preserves the raw
payload verbatim so silver and gold can always be rebuilt from it.

The runner is generic across sources. Two pagination styles are supported via
``pagination.type``:
  * ``page_number`` (default) — walk ?page=1,2,... until ``total_pages``
  * ``offset``               — walk ?_offset=0,limit,... until ``totalItems``
and the response shape is configurable so different APIs fit without new code:
  * ``response.items_key``  where the record list lives (default "results")
  * ``response.total_key``  where the total lives (defaults per pagination type)
  * ``response.id_key``     the record's id field (default "id"; Libris: "@id")
  * ``request.headers``     e.g. {Accept: application/ld+json}
  * ``request.params``      constant query params merged into every page
"""
from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone

import yaml
from pyspark.sql import SparkSession

from src.ingestion.authentication.none_auth import NoAuth
from src.ingestion.clients.rest_client import RestClient
from src.ingestion.pagination.offset import offset_params
from src.ingestion.pagination.page_number import page_params
from src.metadata.control_tables import finish_run, set_watermark, start_run


def load_source_config(path: str) -> dict:
    with open(path) as f:
        return yaml.safe_load(f)


def _to_rows(items, *, source, endpoint, ingested_at, batch_id, params, id_key, schema_version):
    """One bronze row per source record: raw payload + technical metadata."""
    rows = []
    for rec in items:
        rec_id = rec.get(id_key) if isinstance(rec, dict) else None
        rows.append({
            "source_name": source,
            "source_endpoint": endpoint,
            "ingested_at": ingested_at,
            "batch_id": batch_id,
            "request_parameters": json.dumps(params),
            "http_status": 200,
            "source_record_id": None if rec_id is None else str(rec_id),
            "raw_payload": json.dumps(rec),   # the whole record, verbatim
            "schema_version": schema_version,
        })
    return rows


def ingest(spark: SparkSession, config_path: str, catalog: str = "dev_lakehouse") -> str:
    cfg = load_source_config(config_path)
    source = cfg["source_name"]
    endpoint = cfg["endpoint"]
    pcfg = cfg["pagination"]
    req = cfg.get("request", {})
    resp = cfg.get("response", {})

    base_params = req.get("params", {})
    items_key = resp.get("items_key", "results")
    id_key = resp.get("id_key", "id")
    schema_version = cfg.get("schema_version", "v1")
    is_offset = pcfg.get("type") == "offset"
    total_key = resp.get("total_key", "totalItems" if is_offset else "total_pages")

    # metadata is data: every run is recorded in a control table
    run_id = start_run(spark, catalog, pipeline_name=f"ingest_{source}", source_name=source)

    client = RestClient(cfg["base_url"], auth=NoAuth(), default_headers=req.get("headers", {}))
    batch_id = str(uuid.uuid4())
    ingested_at = datetime.now(timezone.utc)
    rows: list[dict] = []

    def collect(items, params):
        rows.extend(_to_rows(items, source=source, endpoint=endpoint,
                             ingested_at=ingested_at, batch_id=batch_id, params=params,
                             id_key=id_key, schema_version=schema_version))

    try:
        if is_offset:
            offset, total = 0, None
            while total is None or offset < total:
                params = {**base_params, **offset_params(offset, pcfg)}
                body = client.get(endpoint, params=params)
                total = body.get(total_key, 0)
                items = body.get(items_key, [])
                if not items:
                    break
                collect(items, params)
                offset += pcfg["page_size"]
        else:
            page, total_pages = 1, 1
            while page <= total_pages:
                params = {**base_params, **page_params(page, pcfg)}
                body = client.get(endpoint, params=params)
                total_pages = body.get(total_key, 1)
                collect(body.get(items_key, []), params)
                page += 1

        target = f"{catalog}.bronze.{source}_records"
        spark.createDataFrame(rows).write.mode("append").saveAsTable(target)  # append-only

        set_watermark(spark, catalog, source, "ingested_at", ingested_at.isoformat())
        finish_run(spark, catalog, run_id, status="success",
                   read=len(rows), written=len(rows))
    except Exception as exc:  # noqa: BLE001 - record failure, then re-raise
        finish_run(spark, catalog, run_id, status="failed", error=str(exc))
        raise

    return run_id
