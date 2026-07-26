"""Configuration-driven runner for governed batch jobs."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from lakehouse_platform.io.writers import write_output
from lakehouse_platform.metadata.control_tables import finish_run, start_run
from lakehouse_platform.observability.progress import progress


@dataclass(frozen=True)
class JobContext:
    spark: Any
    catalog: str
    run_id: str


def _qualified_table(catalog: str, path: str) -> str:
    parts = path.split(".")
    if len(parts) == 2:
        return f"{catalog}.{path}"
    if len(parts) == 3:
        return path
    raise ValueError("target.path must be schema.table or catalog.schema.table")


def _write_target(spark: Any, result: Any, target: dict[str, Any], table: str) -> None:
    mode = str(target["mode"])
    file_format = str(target.get("format", "delta"))

    if mode == "merge":
        keys = list(target.get("keys", []))
        if not keys:
            raise ValueError("target.keys is required when target.mode is 'merge'")
        write_output(
            spark,
            result,
            "delta_merge",
            {
                "table": table,
                "keys": keys,
                "format": file_format,
                "when_matched": target.get("when_matched", "update"),
            },
        )
        return

    if mode in {"overwrite", "append"}:
        write_output(
            spark,
            result,
            "delta_table",
            {
                "table": table,
                "format": file_format,
                "mode": mode,
                "overwrite_schema": target.get("overwrite_schema", True),
            },
        )
        return

    raise ValueError("target.mode must be 'merge', 'overwrite' or 'append'")


def _validate(result: Any, validation: Any) -> None:
    if hasattr(validation, "validate"):
        validation.validate(result)
        return

    contract = validation["contract"]
    contract.validate(result)
    for check in validation.get("checks", []):
        check(result)


def process_job(spark: Any, job_config: dict[str, Any], *, catalog: str) -> str:
    """Transform, validate and write one table from a readable job configuration."""
    pipeline_name = str(job_config["pipeline_name"])
    source_name = str(job_config["source_name"])
    target_config = dict(job_config["target"])
    target_table = _qualified_table(catalog, str(target_config["path"]))
    run_id = start_run(
        spark,
        catalog,
        pipeline_name=pipeline_name,
        source_name=source_name,
    )
    context = JobContext(spark=spark, catalog=catalog, run_id=run_id)
    progress("JOB", "Job started", pipeline=pipeline_name, target=target_table)

    try:
        result = job_config["transformation"](context)
        _validate(result, job_config["validation"])
        rows = result.count()
        progress(
            "JOB",
            "Writing table",
            target=target_table,
            mode=target_config["mode"],
            rows=rows,
        )
        _write_target(spark, result, target_config, target_table)

        on_success = job_config.get("on_success")
        if on_success is not None:
            on_success(context)

        finish_run(spark, catalog, run_id, status="success", read=rows, written=rows)
        progress("JOB", "Job completed", target=target_table, rows=rows, run_id=run_id)
        return run_id
    except Exception as error:
        finish_run(spark, catalog, run_id, status="failed", error=str(error))
        progress("JOB", "Job failed", target=target_table, error=str(error))
        raise
