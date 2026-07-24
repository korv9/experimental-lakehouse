"""Column type markers used in schema TableDefinitions.

These are lightweight sentinels — they carry a Spark type *name* but do NOT
import pyspark, so schema files can be imported and introspected anywhere
(tests, tooling) without a Spark session. ``base_schema`` resolves the names to
real Spark types lazily when it builds a StructType.

Native Python annotations are also understood by the resolver:
    int -> integer, float -> double, str -> string, bool -> boolean,
    datetime -> timestamp, date -> date, list -> array<string>.
"""


class _Type:
    spark_name: str | None = None


class Bigint(_Type):
    spark_name = "long"


class Int(_Type):
    spark_name = "integer"


class String(_Type):
    spark_name = "string"


class Boolean(_Type):
    spark_name = "boolean"


class Double(_Type):
    spark_name = "double"


class Timestamp(_Type):
    spark_name = "timestamp"


class Date(_Type):
    spark_name = "date"
