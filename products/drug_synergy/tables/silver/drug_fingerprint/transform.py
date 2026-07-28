"""Silver drug -> Morgan fingerprints.

Ports ``fetch_fingerprints.py``. The chemistry lives in
``products.drug_synergy.fingerprints`` so it can be tested without Spark; this
module is only the Spark wiring around it.

Requires ``rdkit`` as a cluster library. Without it the job fails loudly on the
first row rather than silently producing empty fingerprints.
"""
from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.functions import udf

from lakehouse_platform.observability.progress import progress
from products.drug_synergy.fingerprints import N_BITS, RADIUS, morgan_bits

_bits_udf = udf(morgan_bits, T.ArrayType(T.IntegerType()))


def transform(drug: DataFrame, options: dict | None = None) -> DataFrame:
    progress("DRUG_SYNERGY", "Computing Morgan fingerprints", n_bits=N_BITS, radius=RADIUS)
    return (
        drug.where(F.col("smiles").isNotNull())
        .withColumn("active_bits", _bits_udf(F.col("smiles")))
        .where(F.size("active_bits") > 0)  # unparseable structures are not fingerprints
        .select(
            "drug_name",
            "active_bits",
            F.size("active_bits").alias("n_active_bits"),
            F.lit(N_BITS).alias("n_bits"),
            F.lit(RADIUS).alias("radius"),
            "ingested_at",
        )
    )
