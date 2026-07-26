"""Built-in ACON readers."""
from lakehouse_platform.observability.progress import progress


def uc_read(spark, table: str, *, catalog: str | None = None):
    """Read a Unity Catalog table from either schema.table or catalog.schema.table."""
    parts = table.split(".")
    if len(parts) == 2:
        selected_catalog = catalog or spark.catalog.currentCatalog()
        table = f"{selected_catalog}.{table}"
    elif len(parts) != 3:
        raise ValueError("Unity Catalog table must be schema.table or catalog.schema.table")
    progress("READER", "Loading Unity Catalog table", table=table)
    return spark.table(table)


def read_input(spark, kind: str, options: dict):
    if kind == "unity_catalog_table":
        return uc_read(spark, options["table"], catalog=options.get("catalog"))
    if kind == "json":
        progress("READER", "Loading JSON", path=options["path"])
        return spark.read.options(**options.get("read_options", {})).json(options["path"])
    if kind == "text":
        progress("READER", "Loading text", path=options["path"])
        return spark.read.text(options["path"])
    raise ValueError(f"unsupported reader: {kind}")
