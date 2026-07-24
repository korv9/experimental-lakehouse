"""Built-in ACON readers."""
from lakehouse_platform.observability.progress import progress


def read_input(spark, kind: str, options: dict):
    if kind == "unity_catalog_table":
        progress("READER", "Loading Unity Catalog table", table=options["table"])
        return spark.table(options["table"])
    if kind == "json":
        progress("READER", "Loading JSON", path=options["path"])
        return spark.read.options(**options.get("read_options", {})).json(options["path"])
    if kind == "text":
        progress("READER", "Loading text", path=options["path"])
        return spark.read.text(options["path"])
    raise ValueError(f"unsupported reader: {kind}")
