"""Built-in ACON readers."""
import json

from lakehouse_platform.core.imports import import_callable
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


def read_json_records(spark, path: str):
    """A JSON array file -> one row per element, each kept verbatim as a string.

    Landing readers must not interpret the data: this feed is heterogeneous, so
    parsing it here would force a schema on Bronze and lose the raw form Silver
    is supposed to clean. Reading happens on the driver because this reader is
    for small seed files; use Auto Loader for real volumes.
    """
    with open(path, encoding="utf-8") as handle:
        records = json.load(handle)
    if not isinstance(records, list):
        raise ValueError(f"json_records expects a JSON array at {path}")
    progress("READER", "Loading JSON records", path=path, records=len(records))
    rows = [(json.dumps(record, ensure_ascii=False),) for record in records]
    return spark.createDataFrame(rows, ["raw_payload"])


def read_product_callable(spark, options: dict):
    """An input produced by product code rather than by a file or a table.

    Some inputs are neither: a curated selection, a fixture, a generated
    calendar. Rather than pushing that work into a transformation (where it
    would be invisible to the ACON graph), the product exposes
    ``callable(spark, options) -> DataFrame`` and declares it as an input.
    """
    reference = options["callable"]
    progress("READER", "Building input from product callable", callable=reference)
    return import_callable(reference)(spark, options)


def read_input(spark, kind: str, options: dict):
    if kind == "unity_catalog_table":
        return uc_read(spark, options["table"], catalog=options.get("catalog"))
    if kind == "json":
        progress("READER", "Loading JSON", path=options["path"])
        return spark.read.options(**options.get("read_options", {})).json(options["path"])
    if kind == "json_records":
        return read_json_records(spark, options["path"])
    if kind == "text":
        progress("READER", "Loading text", path=options["path"])
        return spark.read.text(options["path"])
    if kind == "product_callable":
        return read_product_callable(spark, options)
    raise ValueError(f"unsupported reader: {kind}")
