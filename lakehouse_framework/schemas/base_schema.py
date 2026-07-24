"""BaseSchema — the contract every TableDefinition inherits.

A TableDefinition is a ``@dataclass`` whose annotations describe the table's
columns and whose nested ``Meta`` carries the table's location, description,
constraints, properties and column comments. BaseSchema turns that declaration
into things the framework needs: the ordered column list, a Spark StructType,
the primary keys, and a validation check against a produced DataFrame.

pyspark is imported lazily (only inside the methods that need it) so schema
files stay importable without a Spark session.
"""
from __future__ import annotations

import dataclasses
import datetime as _dt
import typing


class BaseSchema:
    # --- Meta accessors -------------------------------------------------
    @classmethod
    def object_location(cls) -> str:
        return cls.Meta.object_location

    @classmethod
    def object_name(cls) -> str:
        return cls.Meta.object_name

    @classmethod
    def column_comments(cls) -> dict:
        return getattr(cls.Meta, "column_comments", {})

    @classmethod
    def table_properties(cls) -> dict:
        return getattr(cls.Meta, "custom_table_properties", {})

    @classmethod
    def primary_keys(cls) -> list[str]:
        constraints = getattr(cls.Meta, "column_constraints", {})
        return [c for c, spec in constraints.items() if spec.get("PK")]

    # --- Column introspection (no Spark needed) -------------------------
    @classmethod
    def column_names(cls) -> list[str]:
        return [f.name for f in dataclasses.fields(cls)]

    # --- Spark-dependent helpers (lazy import) --------------------------
    @classmethod
    def spark_schema(cls):
        from pyspark.sql import types as T

        fields = []
        for f in dataclasses.fields(cls):
            spark_type, nullable = _resolve(f.type, T)
            fields.append(T.StructField(f.name, spark_type, nullable))
        return T.StructType(fields)

    @classmethod
    def validate(cls, df) -> bool:
        """Check the produced DataFrame matches the contract.

        Structural: exactly the declared columns (no missing, no extras).
        Integrity: primary-key columns contain no nulls.
        Raises ValueError on any violation so process_job fails loudly.
        """
        from pyspark.sql import functions as F

        expected = set(cls.column_names())
        actual = set(df.columns)
        missing, extra = expected - actual, actual - expected
        if missing:
            raise ValueError(f"{cls.object_location()}: missing columns {sorted(missing)}")
        if extra:
            raise ValueError(f"{cls.object_location()}: unexpected columns {sorted(extra)}")
        for pk in cls.primary_keys():
            if df.where(F.col(pk).isNull()).limit(1).count() > 0:
                raise ValueError(f"{cls.object_location()}: primary key '{pk}' contains nulls")
        return True


def _resolve(anno, T):
    """(annotation) -> (spark_type, nullable). Handles Optional, custom and native."""
    nullable = False
    if typing.get_origin(anno) is typing.Union:
        args = [a for a in typing.get_args(anno) if a is not type(None)]  # noqa: E721
        nullable, anno = True, args[0]

    # arrays: bare `list` or list[...] -> array<string> (our arrays are string arrays)
    if anno is list or typing.get_origin(anno) is list:
        return T.ArrayType(T.StringType()), True

    # custom marker types from types.py
    if isinstance(anno, type) and getattr(anno, "spark_name", None):
        return _by_name(anno.spark_name, T), nullable

    # native Python types
    if anno is _dt.datetime:
        return T.TimestampType(), nullable
    if anno is _dt.date:
        return T.DateType(), nullable
    native = {int: T.IntegerType, float: T.DoubleType, str: T.StringType, bool: T.BooleanType}
    if anno in native:
        return native[anno](), nullable

    return T.StringType(), True  # safe fallback


def _by_name(name, T):
    return {
        "long": T.LongType, "integer": T.IntegerType, "string": T.StringType,
        "boolean": T.BooleanType, "double": T.DoubleType,
        "timestamp": T.TimestampType, "date": T.DateType,
    }[name]()
