"""Bronze schema for ``bronze.example_data_records``.

Bronze = raw + technical metadata. The whole source record is kept verbatim as
a JSON string in ``raw_payload``; the other columns are ingestion bookkeeping so
every row is traceable back to the batch that produced it. These nine columns
are the project's standard bronze metadata contract.
"""
from pyspark.sql.types import (
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

BRONZE_EXAMPLE_DATA = StructType([
    StructField("source_name", StringType()),
    StructField("source_endpoint", StringType()),
    StructField("ingested_at", TimestampType()),
    StructField("batch_id", StringType()),
    StructField("request_parameters", StringType()),
    StructField("http_status", IntegerType()),
    StructField("source_record_id", StringType()),
    StructField("raw_payload", StringType()),   # full source JSON, unparsed
    StructField("schema_version", StringType()),
])

TABLE = "bronze.example_data_records"
