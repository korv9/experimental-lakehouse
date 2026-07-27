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


def read_table(
    spark: Any,
    table: str,
    *,
    catalog: str,
    variables: dict[str, str] | None = None,
    reader: str = "unity_catalog_table",
):
    """Single notebook-facing read — the same reader layer ACON uses.

    Notebooks that build a DataFrame imperatively call this instead of touching
    spark.table directly, so every read in the platform goes through one path:
    ACON variable resolution (``${catalog}`` and friends) followed by the ACON
    reader registry in ``io.readers.read_input``. Declarative notebooks get the
    identical behaviour through their ACON ``inputs`` section.

        df = read_table(spark, "bronze.gutenberg_catalog_raw", catalog=catalog)
        df = read_table(spark, "${catalog}.silver.works", catalog=catalog)
    """
    from lakehouse_platform.engine import resolve_values
    from lakehouse_platform.io.readers import read_input

    merged_variables = {"catalog": catalog, **(variables or {})}
    options = resolve_values({"table": table, "catalog": catalog}, merged_variables)
    return read_input(spark, reader, options)


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


def _validate(result: Any, contract: type, expectations: dict[str, Any]) -> None:
    progress(
        "SCHEMA",
        "Validating output contract",
        table=contract.object_location(),
        expected_columns=",".join(contract.column_names()),
    )
    try:
        contract.validate(result)
    except Exception as error:
        progress("SCHEMA", "Contract validation failed", error=str(error))
        print(f"[SCHEMA] Expected: {contract.spark_schema().simpleString()}")
        print("[SCHEMA] Actual:")
        result.printSchema()
        raise

    expected_rows = expectations.get("row_count")
    if expected_rows is not None:
        actual_rows = result.count()
        if actual_rows != int(expected_rows):
            raise ValueError(f"Expected {expected_rows} rows, produced {actual_rows}")

    minimum_rows = expectations.get("min_rows")
    if minimum_rows is not None:
        actual_rows = result.count()
        if actual_rows < int(minimum_rows):
            raise ValueError(f"Expected at least {minimum_rows} rows, produced {actual_rows}")

    for column, expected in expectations.get("array_contains", {}).items():
        from pyspark.sql import functions as F

        invalid = result.filter(~F.array_contains(F.col(column), expected)).limit(1)
        if invalid.count():
            raise ValueError(f"Column '{column}' must contain {expected!r} in every row")

    for columns in expectations.get("unique", []):
        keys = [columns] if isinstance(columns, str) else list(columns)
        duplicate = result.groupBy(*keys).count().filter("count > 1").limit(1)
        if duplicate.count():
            raise ValueError(f"Expected unique values for columns {keys}")

    progress("SCHEMA", "Output contract passed", table=contract.object_location())


def process_job(
    spark: Any,
    job_config: dict[str, Any] | None = None,
    *,
    catalog: str,
    dataframe: Any | None = None,
    acon: Any | None = None,
    variables: dict[str, str] | None = None,
    pipeline_name: str | None = None,
    source_name: str | None = None,
) -> str:
    """Single governed entry point for batch jobs — one call, one engine.

    Two forms:
    * ACON form (``acon=`` path/dict/Acon): runs the declarative ACON engine
      (inputs -> transforms -> quality -> writes) wrapped in run logging. This is
      how the product notebooks execute, so every pipeline goes through one path.
    * Imperative form (``job_config=`` + optional ``dataframe``): validates one
      DataFrame against its contract and writes a single target.

    Both record a row in platform.pipeline_runs.
    """
    if acon is not None:
        return _run_acon_job(
            spark,
            acon,
            catalog=catalog,
            variables=variables,
            pipeline_name=pipeline_name,
            source_name=source_name,
        )
    if job_config is None:
        raise ValueError("process_job requires either acon= or job_config=")
    pipeline_name = str(job_config["pipeline_name"])
    source_name = str(job_config["source_name"])
    target_config = dict(job_config["target"])
    contract = job_config["contract"]
    target_table = _qualified_table(catalog, str(target_config["path"]))
    run_id = start_run(
        spark,
        catalog,
        pipeline_name=pipeline_name,
        source_name=source_name,
    )
    context = JobContext(spark=spark, catalog=catalog, run_id=run_id)
    progress("JOB", "Job started", pipeline=pipeline_name, target=target_table, run_id=run_id)

    try:
        result = dataframe
        if result is None:
            result = job_config["transformation"](context)
        _validate(result, contract, job_config.get("expectations", {}))
        rows = result.count()
        progress("JOB", "Writing table", target=target_table, mode=target_config["mode"], rows=rows)
        _write_target(spark, result, target_config, target_table)

        on_success = job_config.get("on_success")
        if on_success is not None:
            on_success(context)

        finish_run(spark, catalog, run_id, status="success", read=rows, written=rows)
        progress("JOB", "Job completed", target=target_table, rows=rows, run_id=run_id)
        return run_id
    except Exception as error:
        finish_run(spark, catalog, run_id, status="failed", error=str(error))
        progress(
            "JOB",
            "Job failed",
            pipeline=pipeline_name,
            target=target_table,
            run_id=run_id,
            error_type=type(error).__name__,
            error=str(error),
        )
        raise


def _run_acon_job(
    spark: Any,
    acon: Any,
    *,
    catalog: str,
    variables: dict[str, str] | None = None,
    pipeline_name: str | None = None,
    source_name: str | None = None,
) -> str:
    """Run an ACON pipeline through the engine, wrapped in pipeline-run logging.

    Adds the governance the raw engine lacks (a platform.pipeline_runs record and
    failure reporting) so the declarative products get the same guarantees as the
    imperative jobs — without a second execution path in the notebooks.
    """
    from lakehouse_platform.core.acon import Acon
    from lakehouse_platform.engine import run_pipeline

    if isinstance(acon, Acon):
        config = acon
    elif isinstance(acon, dict):
        config = Acon.from_dict(acon)
    else:
        config = Acon.from_yaml(acon)

    name = pipeline_name or config.pipeline.id
    source = source_name or name
    merged_variables = {"catalog": catalog, **(variables or {})}

    run_id = start_run(spark, catalog, pipeline_name=name, source_name=source)
    progress("JOB", "ACON job started", pipeline=name, run_id=run_id)
    try:
        result = run_pipeline(spark, config, merged_variables)
        finish_run(spark, catalog, run_id, status="success", written=len(result.outputs))
        progress(
            "JOB",
            "ACON job completed",
            pipeline=name,
            outputs=len(result.outputs),
            run_id=run_id,
        )
        return run_id
    except Exception as error:
        finish_run(spark, catalog, run_id, status="failed", error=str(error))
        progress(
            "JOB",
            "ACON job failed",
            pipeline=name,
            run_id=run_id,
            error_type=type(error).__name__,
            error=str(error),
        )
        raise
