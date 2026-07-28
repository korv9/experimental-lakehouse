"""Bronze -> Silver for DrugComb combination screens.

This is the platform port of the original project's ``BRAclean.py``. Read top to
bottom; each step is one of its cleaning stages:

  1. _parse        pull the source columns out of raw_payload
  2. _normalise    case/whitespace on drugs, alphanumeric key on cell lines,
                   numeric coercion so "N/A" becomes null instead of zero
  3. _canonicalise A+B and B+A become the same pair (drug_min, drug_max)
  4. _aggregate    repeated screens of a pair average into one row
  5. _label        the conventional +/-10 ZIP cutoff

Self-pairs (a drug combined with itself) are dropped here rather than
quarantined: they are not a data error, they simply are not combinations. Rows
missing a drug or a cell line *are* an error and the ACON quality gate
quarantines them, so they stay visible.
"""
from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T

from lakehouse_platform.observability.progress import progress
from products.drug_synergy.normalisation import normalise_cell_line, normalise_drug, to_double

# Column names as they appear in the DrugComb export. Verify against your
# download; a rename here is the only change a new export layout needs.
SOURCE_SCHEMA = T.StructType([
    T.StructField("Drug1", T.StringType()),
    T.StructField("Drug2", T.StringType()),
    T.StructField("Cell line", T.StringType()),
    T.StructField("ZIP", T.StringType()),
    T.StructField("Bliss", T.StringType()),
    T.StructField("Loewe", T.StringType()),
    T.StructField("HSA", T.StringType()),
])

SYNERGY_CUTOFF = 10.0
SCORES = ["synergy_zip", "synergy_bliss", "synergy_loewe", "synergy_hsa"]


def _parse(bronze: DataFrame) -> DataFrame:
    parsed = bronze.withColumn("p", F.from_json("raw_payload", SOURCE_SCHEMA))
    return parsed.select(
        F.col("p.Drug1").alias("raw_drug1"),
        F.col("p.Drug2").alias("raw_drug2"),
        F.col("p.`Cell line`").alias("cell_line_raw"),
        F.col("p.ZIP").alias("raw_zip"),
        F.col("p.Bliss").alias("raw_bliss"),
        F.col("p.Loewe").alias("raw_loewe"),
        F.col("p.HSA").alias("raw_hsa"),
        F.col("ingested_at"),
    )


def _normalise(df: DataFrame) -> DataFrame:
    return df.select(
        normalise_drug(F.col("raw_drug1")).alias("drug1"),
        normalise_drug(F.col("raw_drug2")).alias("drug2"),
        normalise_cell_line(F.col("cell_line_raw")).alias("cell_line_key"),
        F.col("cell_line_raw"),
        to_double(F.col("raw_zip")).alias("synergy_zip"),
        to_double(F.col("raw_bliss")).alias("synergy_bliss"),
        to_double(F.col("raw_loewe")).alias("synergy_loewe"),
        to_double(F.col("raw_hsa")).alias("synergy_hsa"),
        F.col("ingested_at"),
    )


def _canonicalise(df: DataFrame) -> DataFrame:
    """Order the pair so A+B and B+A aggregate together; drop self-pairs.

    Ordering is applied only when both drugs are present: Spark's least/greatest
    ignore nulls, so a row missing one drug would otherwise come out with
    drug_min == drug_max and be discarded as a self-pair. Incomplete rows must
    survive to the quality gate, which quarantines them visibly.
    """
    both_present = F.col("drug1").isNotNull() & F.col("drug2").isNotNull()
    ordered = df.withColumn(
        "drug_min", F.when(both_present, F.least("drug1", "drug2")).otherwise(F.col("drug1"))
    ).withColumn(
        "drug_max", F.when(both_present, F.greatest("drug1", "drug2")).otherwise(F.col("drug2"))
    )
    is_self_pair = both_present & (F.col("drug1") == F.col("drug2"))
    return ordered.where(~is_self_pair).drop("drug1", "drug2")


def _aggregate(df: DataFrame) -> DataFrame:
    """Average repeated screens of the same pair and cell line."""
    return df.groupBy("drug_min", "drug_max", "cell_line_key").agg(
        F.first("cell_line_raw", ignorenulls=True).alias("cell_line_raw"),
        *[F.avg(score).alias(score) for score in SCORES],
        F.count(F.lit(1)).cast("int").alias("n_measurements"),
        F.max("ingested_at").alias("ingested_at"),
    )


def _label(df: DataFrame) -> DataFrame:
    zip_score = F.col("synergy_zip")
    return df.withColumn(
        "is_synergistic",
        F.when(zip_score.isNull(), None).otherwise(zip_score > SYNERGY_CUTOFF),
    ).withColumn(
        "is_antagonistic",
        F.when(zip_score.isNull(), None).otherwise(zip_score < -SYNERGY_CUTOFF),
    )


def transform(bronze: DataFrame, options: dict | None = None) -> DataFrame:
    progress("DRUG_SYNERGY", "Cleaning DrugComb screens")
    result = _label(_aggregate(_canonicalise(_normalise(_parse(bronze)))))
    progress("DRUG_SYNERGY", "Silver combination graph created")
    return result.select(
        "drug_min", "drug_max", "cell_line_key", "cell_line_raw",
        *SCORES, "is_synergistic", "is_antagonistic", "n_measurements", "ingested_at",
    )
