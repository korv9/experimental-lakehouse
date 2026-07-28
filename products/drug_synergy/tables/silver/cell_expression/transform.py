"""Bronze -> Silver: unpivot the wide DepMap expression matrix to long format.

Bronze holds one JSON payload per cell line containing every gene column. The
gene list is discovered from the data rather than declared, because DepMap adds
and renames genes between releases — a hard-coded list would break every quarter.

``from_json`` with a MapType parses the payload without naming 19,000 columns,
and ``explode`` turns the map into rows. That keeps the whole step distributed;
nothing is collected to the driver.
"""
from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T

from lakehouse_platform.observability.progress import progress

KEY_FIELD = "ModelID"
PAYLOAD_MAP = T.MapType(T.StringType(), T.StringType())


def transform(bronze: DataFrame, options: dict | None = None) -> DataFrame:
    options = options or {}
    key_field = options.get("key_field", KEY_FIELD)
    progress("DRUG_SYNERGY", "Unpivoting DepMap expression", key_field=key_field)

    parsed = bronze.withColumn("genes", F.from_json("raw_payload", PAYLOAD_MAP)).select(
        F.col("genes").getItem(key_field).alias("model_id"),
        F.map_filter("genes", lambda key, _value: key != F.lit(key_field)).alias("genes"),
        F.col("schema_version").alias("depmap_release"),
        F.col("ingested_at"),
    )

    long_form = parsed.select(
        "model_id",
        F.explode("genes").alias("gene_symbol", "raw_value"),
        "depmap_release",
        "ingested_at",
    )

    return long_form.select(
        "model_id",
        "gene_symbol",
        # blanks and 'NA' become null rather than 0, which would look like silence
        F.when(
            F.trim("raw_value").rlike(r"^-?\d+(\.\d+)?$"), F.col("raw_value").cast("double")
        ).otherwise(F.lit(None).cast("double")).alias("expression_log1p_tpm"),
        "depmap_release",
        "ingested_at",
    ).where(F.col("model_id").isNotNull())
