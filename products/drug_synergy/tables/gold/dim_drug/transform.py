"""Drug dimension: structure from PubChem, density from the fingerprint.

Multi-input: silver.drug and silver.drug_fingerprint arrive in the order the
ACON declares them. The join is a left join — a drug PubChem could not resolve
still belongs in the dimension, otherwise the fact would lose rows for reasons
that have nothing to do with the screening data.
"""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from lakehouse_platform.observability.progress import progress
from lakehouse_platform.transforms.hashing import internal_id_hash


def build(drug: DataFrame, fingerprint: DataFrame, options: dict | None = None) -> DataFrame:
    progress("DIM_DRUG", "Building dimension", grain="one row per drug")
    return (
        drug.join(fingerprint.select("drug_name", "n_active_bits"), on="drug_name", how="left")
        .select(
            internal_id_hash("drug_name").alias("drug_key"),
            F.col("drug_name").alias("drug_id"),
            "pubchem_cid",
            "smiles",
            "n_active_bits",
        )
        .dropDuplicates(["drug_key"])
    )
