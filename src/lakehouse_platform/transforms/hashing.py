"""Deterministic platform identifiers."""
from pyspark.sql import functions as F


def internal_id_hash(*columns: str):
    values = [F.coalesce(F.col(column).cast("string"), F.lit("∅")) for column in columns]
    return F.xxhash64(*values)
