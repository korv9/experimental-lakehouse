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

from lakehouse_platform.ingestion.authentication.none import NoAuth
from lakehouse_platform.ingestion.clients.rest import RestClient
from lakehouse_platform.ingestion.pagination.page_number import page_params
from lakehouse_platform.metadata.control_tables import finish_run, set_watermark, start_run
from lakehouse_platform.observability.progress import progress


def load_source_config(path: str) -> dict:
    with open(path) as f:
        return yaml.safe_load(f)


def ingest(spark: SparkSession, config_path: str, catalog: str = "dev_lakehouse") -> str:
    cfg = load_source_config(config_path)
    source = cfg["source_name"]
    progress("INGEST", "Source configuration loaded", source=source, catalog=catalog)

    # metadata is data: every run is recorded in a control table
    run_id = start_run(spark, catalog, pipeline_name=f"ingest_{source}", source_name=source)
    progress("INGEST", "Pipeline run opened", run_id=run_id)

    client = RestClient(cfg["base_url"], auth=NoAuth())
    batch_id = str(uuid.uuid4())
    ingested_at = datetime.now(timezone.utc)
    rows: list[dict] = []
    page, total_pages = 1, 1

    try:
        while page <= total_pages:
            progress("INGEST", "Requesting API page", source=source, page=page)
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
        progress("INGEST", "Appending Bronze rows", table=target, rows=len(rows))
        spark.createDataFrame(rows).write.mode("append").saveAsTable(target)  # append-only

        set_watermark(spark, catalog, source, "ingested_at", ingested_at.isoformat())
        finish_run(spark, catalog, run_id, status="success",
                   read=len(rows), written=len(rows))
        progress("INGEST", "Ingestion completed", run_id=run_id, rows=len(rows))
    except Exception as exc:
        progress("INGEST", "Ingestion failed", run_id=run_id, error=str(exc))
        finish_run(spark, catalog, run_id, status="failed", error=str(exc))
        raise

    return run_id
