# Databricks notebook source
"""PUBCHEM -> BRONZE | resolve drug names to compound identifiers.

Reads the distinct drug names already in Silver and asks PubChem for the ones
that are still unresolved, so a rerun costs only the new drugs. This replaces
the original project's local JSON cache: Bronze is the cache.

Run order: land_bronze -> bronze_to_silver (combinations) -> this -> rerun
bronze_to_silver so silver.drug and the fingerprints pick up the new responses.
"""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from lakehouse_platform.ingestion.runner import ingest
from lakehouse_platform.jobs import read_table

CATALOG = "dev_lakehouse"
SOURCE_CONFIG = "config/sources/pubchem_compound.yaml"

spark = SparkSession.builder.getOrCreate()

# Distinct drugs in the screens, minus the ones PubChem already answered for.
combinations = read_table(spark, "silver.drug_combination", catalog=CATALOG)
wanted = (
    combinations.select(F.col("drug_min").alias("drug_name"))
    .union(combinations.select(F.col("drug_max").alias("drug_name")))
    .where(F.col("drug_name").isNotNull())
    .distinct()
)

if spark.catalog.tableExists(f"{CATALOG}.bronze.pubchem_compound_raw"):
    known = read_table(spark, "bronze.pubchem_compound_raw", catalog=CATALOG).select(
        F.col("source_record_id").alias("drug_name")
    )
    wanted = wanted.join(known.distinct(), on="drug_name", how="left_anti")

names = [row["drug_name"] for row in wanted.collect()]
print(f"[PUBCHEM] {len(names)} drug names to resolve")

if names:
    run_id = ingest(spark, SOURCE_CONFIG, catalog=CATALOG, records=names)
    print(f"[PUBCHEM] Completed run: {run_id}")
else:
    print("[PUBCHEM] Nothing to resolve — Bronze already covers every drug")
