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
import types
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

        annotations = typing.get_type_hints(cls)
        fields = []
        for f in dataclasses.fields(cls):
            spark_type, nullable = _resolve(annotations[f.name], T)
            fields.append(T.StructField(f.name, spark_type, nullable))
        return T.StructType(fields)

    @classmethod
    def validate(cls, df) -> bool:
        """Check the produced DataFrame matches the contract.

        Structural: exact columns and Spark data types.
        Integrity: required columns are non-null and primary keys are unique.
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

        expected_schema = cls.spark_schema()
        expected_types = {
            field.name: field.dataType.simpleString() for field in expected_schema.fields
        }
        actual_types = {
            field.name: field.dataType.simpleString() for field in df.schema.fields
        }
        wrong_types = {
            name: {"expected": expected_types[name], "actual": actual_types[name]}
            for name in expected_types
            if expected_types[name] != actual_types[name]
        }
        if wrong_types:
            raise ValueError(f"{cls.object_location()}: wrong column types {wrong_types}")

        required = [field.name for field in expected_schema.fields if not field.nullable]
        if required:
            null_counts = df.agg(
                *[
                    F.sum(F.when(F.col(name).isNull(), 1).otherwise(0)).alias(name)
                    for name in required
                ]
            ).collect()[0]
            invalid = {name: null_counts[name] for name in required if null_counts[name]}
            if invalid:
                raise ValueError(
                    f"{cls.object_location()}: required columns contain nulls {invalid}"
                )

        constraints = getattr(cls.Meta, "column_constraints", {})
        not_blank = [name for name, spec in constraints.items() if spec.get("NOT_BLANK")]
        for name in not_blank:
            invalid = df.filter(F.length(F.trim(F.col(name))) == 0).limit(1)
            if invalid.count():
                raise ValueError(
                    f"{cls.object_location()}: column '{name}' contains blank values"
                )

        keys = cls.primary_keys()
        if keys:
            duplicate = df.groupBy(*keys).count().filter(F.col("count") > 1).limit(1)
            if duplicate.count():
                raise ValueError(
                    f"{cls.object_location()}: duplicate primary key values for {keys}"
                )
        return True


def _resolve(anno, T):
    """(annotation) -> (spark_type, nullable). Handles Optional, custom and native."""
    nullable = False
    if typing.get_origin(anno) in (typing.Union, types.UnionType):
        args = [a for a in typing.get_args(anno) if a is not type(None)]
        nullable, anno = True, args[0]

    # arrays: bare `list` or list[...] -> array<string> (our arrays are string arrays)
    if anno is list or typing.get_origin(anno) is list:
        return T.ArrayType(T.StringType()), nullable

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
