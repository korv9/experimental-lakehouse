"""Bronze -> Silver for PubChem compound lookups.

Ports ``fetch_cid.py`` and ``fetch_smile.py``. Those wrote CSVs and kept a local
JSON cache; here the raw responses are already in Bronze, so this step only has
to read them. Bronze *is* the cache — and unlike a local file it is queryable,
shared, and survives a lost laptop.

PubChem answers a CID lookup as ``{"IdentifierList": {"CID": [2244]}}`` and a
property lookup as ``{"PropertyTable": {"Properties": [{...}]}}``. Both shapes
are parsed here; a failed lookup keeps its row with null identifiers so coverage
can be measured instead of silently shrinking the drug list.
"""
from __future__ import annotations

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

from lakehouse_platform.observability.progress import progress
from products.drug_synergy.normalisation import normalise_drug

RESPONSE_SCHEMA = T.StructType([
    T.StructField("IdentifierList", T.StructType([
        T.StructField("CID", T.ArrayType(T.LongType())),
    ])),
    T.StructField("PropertyTable", T.StructType([
        T.StructField("Properties", T.ArrayType(T.StructType([
            T.StructField("CID", T.LongType()),
            T.StructField("CanonicalSMILES", T.StringType()),
            T.StructField("IsomericSMILES", T.StringType()),
            T.StructField("ConnectivitySMILES", T.StringType()),
        ]))),
    ])),
])


def transform(bronze: DataFrame, options: dict | None = None) -> DataFrame:
    progress("DRUG_SYNERGY", "Parsing PubChem responses")

    parsed = bronze.withColumn("p", F.from_json("raw_payload", RESPONSE_SCHEMA))
    properties = F.element_at(F.col("p.PropertyTable.Properties"), 1)

    resolved = parsed.select(
        normalise_drug(F.col("source_record_id")).alias("drug_name"),
        F.coalesce(
            F.element_at(F.col("p.IdentifierList.CID"), 1),
            properties.getField("CID"),
        ).alias("pubchem_cid"),
        # prefer the isomeric form: stereochemistry changes the fingerprint
        F.coalesce(
            properties.getField("IsomericSMILES"),
            properties.getField("CanonicalSMILES"),
            properties.getField("ConnectivitySMILES"),
        ).alias("smiles"),
        F.get_json_object("request_parameters", "$.lookup").alias("resolved_by"),
        F.col("http_status"),
        F.col("ingested_at"),
    ).where(F.col("drug_name").isNotNull())

    # one row per drug: the newest response that actually resolved something wins
    ranked = Window.partitionBy("drug_name").orderBy(
        F.col("smiles").isNotNull().desc(),
        F.col("pubchem_cid").isNotNull().desc(),
        F.col("ingested_at").desc(),
    )
    return (
        resolved.withColumn("_rn", F.row_number().over(ranked))
        .where("_rn = 1")
        .select("drug_name", "pubchem_cid", "smiles", "resolved_by", "ingested_at")
    )
