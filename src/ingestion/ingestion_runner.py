"""Configuration-driven ingestion: API -> Bronze (append-only).

Reads a source YAML from config/sources/, pulls every page from the API, and
lands the raw JSON records in the bronze table alongside the standard
technical-metadata columns. No cleaning happens here: bronze preserves the raw
payload verbatim so silver and gold can always be rebuilt from it.

Flow:
    load config -> open a pipeline run -> page through the API
    -> append raw rows to bronze -> advance the watermark -> close the run
"""
from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone

import yaml
from pyspark.sql import SparkSession

from src.ingestion.authentication.none_auth import NoAuth
from src.ingestion.clients.rest_client import RestClient
from src.ingestion.pagination.page_number import page_params
from src.metadata.control_tables import finish_run, set_watermark, start_run


def load_source_config(path: str) -> dict:
    with open(path) as f:
        return yaml.safe_load(f)


def ingest(spark: SparkSession, config_path: str, catalog: str = "dev_lakehouse") -> str:
    cfg = load_source_config(config_path)
    source = cfg["source_name"]

    # metadata is data: every run is recorded in a control table
    run_id = start_run(spark, catalog, pipeline_name=f"ingest_{source}", source_name=source)

    client = RestClient(cfg["base_url"], auth=NoAuth())
    batch_id = str(uuid.uuid4())
    ingested_at = datetime.now(timezone.utc)
    rows: list[dict] = []
    page, total_pages = 1, 1

    try:
        while page <= total_pages:
            params = page_params(page, cfg["pagination"])
            body = client.get(cfg["endpoint"], params=params)
            total_pages = body.get("total_pages", 1)

            for rec in body.get("results", []):
                # one bronze row per source record: raw payload + bookkeeping
                rows.append({
                    "source_name": source,
                    "source_endpoint": cfg["endpoint"],
                    "ingested_at": ingested_at,
                    "batch_id": batch_id,
                    "request_parameters": json.dumps(params),
                    "http_status": 200,
                    "source_record_id": str(rec.get("id")),
                    "raw_payload": json.dumps(rec),   # the whole record, verbatim
                    "schema_version": cfg.get("schema_version", "v1"),
                })
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
