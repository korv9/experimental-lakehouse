"""Built-in ACON writers."""
from lakehouse_platform.observability.progress import progress


def write_output(spark, df, kind: str, options: dict) -> None:
    table = options["table"]
    mode = options.get("mode", "overwrite")
    fmt = options.get("format", "delta")

    if kind == "delta_table":
        progress("WRITER", "Saving Delta table", table=table, mode=mode)
        (
            df.write.format(fmt)
            .mode(mode)
            .option("overwriteSchema", str(options.get("overwrite_schema", True)).lower())
            .saveAsTable(table)
        )
        progress("WRITER", "Delta table saved", table=table)
        return
    if kind == "delta_merge":
        from delta.tables import DeltaTable

        keys = options.get("keys") or []
        if not keys:
            raise ValueError("delta_merge requires at least one key")
        progress("WRITER", "Merging Delta table", table=table, keys=",".join(keys))
        if spark.catalog.tableExists(table):
            predicate = " AND ".join(f"target.{key} = source.{key}" for key in keys)
            (
                DeltaTable.forName(spark, table)
                .alias("target")
                .merge(df.alias("source"), predicate)
                .whenMatchedUpdateAll()
                .whenNotMatchedInsertAll()
                .execute()
            )
        else:
            df.write.format(fmt).mode("overwrite").saveAsTable(table)
        progress("WRITER", "Delta merge completed", table=table)
        return
    raise ValueError(f"unsupported writer: {kind}")
