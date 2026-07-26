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
import math
import uuid
from datetime import datetime, timezone

import yaml
from pyspark.sql import SparkSession

from lakehouse_platform.ingestion.authentication.none import NoAuth
from lakehouse_platform.ingestion.clients.rest import RestClient
from lakehouse_platform.ingestion.identity import stable_ingestion_id
from lakehouse_platform.ingestion.pagination.cursor import cursor_params, nested_value, paginate
from lakehouse_platform.ingestion.pagination.page_number import page_params
from lakehouse_platform.ingestion.rate_limit import RateLimiter
from lakehouse_platform.metadata.control_tables import (
    IngestionCheckpoint,
    finish_run,
    get_checkpoint,
    set_checkpoint,
    set_watermark,
    start_run,
)
from lakehouse_platform.observability.progress import progress


def load_source_config(path: str) -> dict:
    with open(path, encoding="utf-8") as f:
        return yaml.safe_load(f)


def _bronze_rows(
    records: list[dict],
    *,
    source: str,
    endpoint: str,
    params: dict,
    run_id: str,
    batch_id: str,
    ingested_at: datetime,
    schema_version: str,
) -> list[dict]:
    rows = []
    for record in records:
        payload = json.dumps(record, ensure_ascii=False, sort_keys=True)
        record_id = str(record.get("id") or record.get("key") or "")
        rows.append(
            {
                "ingestion_id": stable_ingestion_id(source, record_id, payload),
                "source_name": source,
                "source_endpoint": endpoint,
                "ingested_at": ingested_at,
                "batch_id": batch_id,
                "run_id": run_id,
                "request_parameters": json.dumps(params, ensure_ascii=False, sort_keys=True),
                "http_status": 200,
                "source_record_id": record_id,
                "raw_payload": payload,
                "schema_version": schema_version,
            }
        )
    return rows


def _target_name(catalog: str, configured: str) -> str:
    parts = configured.split(".")
    if len(parts) == 2:
        return f"{catalog}.{configured}"
    if len(parts) == 3:
        return configured
    raise ValueError("destination.bronze_table must be schema.table or catalog.schema.table")


def _rest_client(config: dict, rate_limiter: RateLimiter | None, base_url: str | None = None):
    """Build the shared HTTP client entirely from source configuration."""
    request = dict(config.get("request", {}))
    return RestClient(
        base_url or str(config["base_url"]),
        timeout=int(request.get("timeout_seconds", 30)),
        max_retries=int(request.get("max_retries", 3)),
        auth=NoAuth(),
        rate_limiter=rate_limiter,
        default_headers={
            str(name): str(value)
            for name, value in dict(request.get("headers", {})).items()
        },
    )


def _commit_bronze_page(
    spark: SparkSession,
    target: str,
    rows: list[dict],
    *,
    contract=None,
) -> None:
    """Insert only unseen source versions, making page replay idempotent."""
    if not rows:
        return
    schema = contract.spark_schema() if contract else None
    frame = spark.createDataFrame(rows, schema=schema).dropDuplicates(["ingestion_id"])
    if contract:
        contract.validate(frame)
    if not spark.catalog.tableExists(target):
        frame.write.format("delta").mode("append").saveAsTable(target)
        return
    view = f"_bronze_page_{uuid.uuid4().hex}"
    frame.createOrReplaceTempView(view)
    try:
        spark.sql(f"""
            MERGE INTO {target} t
            USING {view} s
              ON t.ingestion_id = s.ingestion_id
            WHEN NOT MATCHED THEN INSERT *
        """)
    finally:
        spark.catalog.dropTempView(view)


def ingest(spark: SparkSession, config_path: str, catalog: str = "dev_lakehouse") -> str:
    cfg = load_source_config(config_path)
    source = cfg["source_name"]
    progress("INGEST", "Source configuration loaded", source=source, catalog=catalog)

    # metadata is data: every run is recorded in a control table
    run_id = start_run(spark, catalog, pipeline_name=f"ingest_{source}", source_name=source)
    progress("INGEST", "Pipeline run opened", run_id=run_id)

    rate_cfg = cfg.get("rate_limit", {})
    rate_limiter = (
        RateLimiter(float(rate_cfg["requests_per_second"]))
        if rate_cfg.get("requests_per_second")
        else None
    )
    client = _rest_client(cfg, rate_limiter)
    batch_id = str(uuid.uuid4())
    ingested_at = datetime.now(timezone.utc)
    target = _target_name(
        catalog,
        cfg.get("destination", {}).get("bronze_table", f"bronze.{source}_records"),
    )
    pagination = dict(cfg.get("pagination", {"type": "page_number"}))
    request_params = dict(cfg.get("request", {}).get("params", {}))
    if pagination.get("type") == "cursor":
        pagination["base_params"] = {
            **request_params,
            **dict(pagination.get("base_params", {})),
        }
    checkpoint = get_checkpoint(
        spark,
        catalog,
        pipeline_name=f"ingest_{source}",
        source_name=source,
    )
    records_read = 0
    last_page = checkpoint.page_number if checkpoint else 0
    resume_cursor = checkpoint.cursor if checkpoint and checkpoint.status != "completed" else None

    try:
        if pagination.get("type") == "cursor":
            pages = paginate(
                client,
                cfg["endpoint"],
                pagination,
                initial_cursor=resume_cursor,
            )
            for cursor_page in pages:
                params = cursor_params(cursor_page.cursor_used, pagination)
                rows = _bronze_rows(
                    list(cursor_page.records),
                    source=source,
                    endpoint=cfg["endpoint"],
                    params=params,
                    run_id=run_id,
                    batch_id=batch_id,
                    ingested_at=ingested_at,
                    schema_version=cfg.get("schema_version", "v1"),
                )
                progress(
                    "INGEST",
                    "Committing cursor page",
                    page=cursor_page.number,
                    rows=len(rows),
                    target=target,
                )
                _commit_bronze_page(spark, target, rows)
                records_read += len(rows)
                last_page += 1
                resume_cursor = cursor_page.next_cursor
                set_checkpoint(
                    spark,
                    catalog,
                    IngestionCheckpoint(
                        f"ingest_{source}",
                        source,
                        "default",
                        resume_cursor,
                        None,
                        last_page,
                        "completed" if resume_cursor is None else "running",
                        run_id,
                    ),
                )
        elif pagination.get("type") == "page_number":
            page = last_page + 1 if checkpoint and checkpoint.status != "completed" else 1
            total_pages = page
            while page <= total_pages:
                progress("INGEST", "Requesting API page", source=source, page=page)
                params = {**request_params, **page_params(page, pagination)}
                body = client.get(cfg["endpoint"], params=params)
                if "total_pages" in body:
                    total_pages = int(body["total_pages"])
                elif "count" in body:
                    total_pages = max(1, math.ceil(int(body["count"]) / pagination["page_size"]))
                records_path = str(pagination.get("records_path", "results"))
                records = nested_value(body, records_path, [])
                if not isinstance(records, list):
                    raise TypeError(
                        f"Page records path {records_path!r} did not resolve to a list"
                    )
                rows = _bronze_rows(
                    records,
                    source=source,
                    endpoint=cfg["endpoint"],
                    params=params,
                    run_id=run_id,
                    batch_id=batch_id,
                    ingested_at=ingested_at,
                    schema_version=cfg.get("schema_version", "v1"),
                )
                progress("INGEST", "Committing numbered page", page=page, rows=len(rows), target=target)
                _commit_bronze_page(spark, target, rows)
                records_read += len(rows)
                last_page = page
                set_checkpoint(
                    spark,
                    catalog,
                    IngestionCheckpoint(
                        f"ingest_{source}",
                        source,
                        "default",
                        None,
                        None,
                        page,
                        "completed" if page >= total_pages else "running",
                        run_id,
                    ),
                )
                page += 1
        else:
            raise ValueError(f"Unsupported pagination type: {pagination.get('type')!r}")

        set_watermark(spark, catalog, source, "ingested_at", ingested_at.isoformat())
        finish_run(spark, catalog, run_id, status="success",
                   read=records_read, written=records_read)
        progress("INGEST", "Ingestion completed", run_id=run_id, rows=records_read)
    except Exception as exc:
        progress("INGEST", "Ingestion failed", run_id=run_id, error=str(exc))
        set_checkpoint(
            spark,
            catalog,
            IngestionCheckpoint(
                f"ingest_{source}",
                source,
                "default",
                resume_cursor,
                None,
                last_page,
                "failed",
                run_id,
            ),
        )
        finish_run(spark, catalog, run_id, status="failed", error=str(exc))
        raise

    return run_id
