"""Analytical views over the synergy star — the platform port of the notebooks.

The original project's `main_eda`, `umap_depmap.py` and `classification.ipynb`
worked on flat CSVs. Here they read Gold, which means the numbers they report
are the same numbers the pipeline published and validated.

These are aggregations, not models: anything that trains belongs in a sandbox
notebook writing to the ``sandbox`` schema, so an experiment can never mutate a
production layer. What lives here is the reusable, tested part.
"""
from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from lakehouse_platform.observability.progress import progress


def synergy_by_cancer_type(fact: DataFrame, cancer_type: DataFrame) -> DataFrame:
    """Which tissues show synergy most often — the headline question."""
    progress("EXPERIMENT", "Aggregating synergy by cancer type")
    return (
        fact.join(cancer_type, on="cancer_type_key", how="inner")
        .groupBy("oncotree_lineage")
        .agg(
            F.sum("combination_count").alias("n_combinations"),
            F.round(F.avg("synergy_zip"), 3).alias("mean_synergy_zip"),
            F.sum(F.col("is_synergistic").cast("int")).alias("n_synergistic"),
        )
        .withColumn(
            "synergistic_rate",
            F.round(F.col("n_synergistic") / F.col("n_combinations"), 3),
        )
        .orderBy(F.desc("synergistic_rate"))
    )


def top_synergistic_pairs(fact: DataFrame, drug: DataFrame, limit: int = 20) -> DataFrame:
    """Highest mean ZIP score per drug pair, aggregated across cell lines."""
    progress("EXPERIMENT", "Ranking drug pairs", limit=limit)
    names = drug.select(
        F.col("drug_key"), F.col("drug_id").alias("drug_name")
    )
    return (
        fact.groupBy("drug_min_key", "drug_max_key")
        .agg(
            F.round(F.avg("synergy_zip"), 3).alias("mean_synergy_zip"),
            F.count(F.lit(1)).alias("n_cell_lines"),
        )
        .where(F.col("n_cell_lines") >= 1)
        .join(names.withColumnRenamed("drug_key", "drug_min_key")
                   .withColumnRenamed("drug_name", "drug_min_name"),
              on="drug_min_key", how="left")
        .join(names.withColumnRenamed("drug_key", "drug_max_key")
                   .withColumnRenamed("drug_name", "drug_max_name"),
              on="drug_max_key", how="left")
        .select("drug_min_name", "drug_max_name", "mean_synergy_zip", "n_cell_lines")
        .orderBy(F.desc("mean_synergy_zip"))
        .limit(limit)
    )


def structural_coverage(drug: DataFrame) -> DataFrame:
    """How much of the drug list PubChem and RDKit actually resolved.

    Coverage is a data-quality question, not a modelling one: a model trained on
    the 60% of drugs that happen to have fingerprints is silently biased.
    """
    progress("EXPERIMENT", "Measuring structural coverage")
    return drug.select(
        F.count(F.lit(1)).alias("n_drugs"),
        F.sum(F.col("pubchem_cid").isNotNull().cast("int")).alias("n_with_cid"),
        F.sum(F.col("smiles").isNotNull().cast("int")).alias("n_with_smiles"),
        F.sum(F.col("n_active_bits").isNotNull().cast("int")).alias("n_with_fingerprint"),
    )
