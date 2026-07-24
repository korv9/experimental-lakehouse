"""process_job — the batch orchestrator.

Given a job_config, it runs the transformation, injects the platform audit
columns (``dp_*``) that transformations never produce themselves, validates the
result against the TableDefinition, enforces column order, and writes the table
— then applies the declared table properties and column comments.

job_config shape:
    {
        "target": {"path": <schema.table>, "format": "delta", "mode": "overwrite"|"append"},
        "transformation": <callable returning a DataFrame>,   # takes no args; uses notebook spark
        "validation": <TableDefinition class>,
    }
"""
from __future__ import annotations

from pyspark.sql import functions as F


def process_job(spark, job_config) -> None:
    schema = job_config["validation"]
    target = job_config["target"]

    # 1. run the transformation (uses the notebook's global spark, like the real framework)
    df = job_config["transformation"]()

    # 2. inject platform audit columns — transforms don't produce these
    cols = schema.column_names()
    if "dp_ingestion_ts" in cols and "dp_ingestion_ts" not in df.columns:
        df = df.withColumn("dp_ingestion_ts", F.current_timestamp())
    if "dp_refresh_ts" in cols:
        df = df.withColumn("dp_refresh_ts", F.current_timestamp())

    # 3. validate against the contract, then enforce declared column order
    schema.validate(df)
    df = df.select(*cols)

    # 4. write
    writer = (
        df.write.format(target.get("format", "delta"))
        .mode(target.get("mode", "overwrite"))
        .option("overwriteSchema", "true")
    )
    for key, value in schema.table_properties().items():
        writer = writer.option(key, value)  # Delta accepts table properties as write options
    writer.saveAsTable(target["path"])

    # 5. best-effort: apply column comments (documentation lives with the data)
    _apply_column_comments(spark, target["path"], schema)


def _apply_column_comments(spark, table: str, schema) -> None:
    for column, comment in schema.column_comments().items():
        safe = comment.replace("'", "''")
        try:
            spark.sql(f"ALTER TABLE {table} ALTER COLUMN {column} COMMENT '{safe}'")
        except Exception:  # noqa: BLE001 - comments are non-critical
            pass
