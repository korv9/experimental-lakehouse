"""Read layer — the framework's data-access helpers.

``uc_read`` is the everyday one: read a Unity Catalog table by name. The rest
handle landing raw source data into bronze.
"""
from __future__ import annotations

import json


def uc_read(spark, table: str):
    """Read a Unity Catalog table by 'schema.table' (or catalog.schema.table)."""
    return spark.read.table(table)


def read_json_records(spark, path: str):
    """Land a JSON array file as one ``raw_payload`` string per element.

    Kept driver-side because the demo landing is tiny; a real source would use
    Auto Loader / a streaming read. The point is that bronze receives each source
    record verbatim as a string, so silver can always re-parse it.
    """
    with open(path) as f:
        records = json.load(f)
    rows = [(json.dumps(r, ensure_ascii=False),) for r in records]
    return spark.createDataFrame(rows, ["raw_payload"])
