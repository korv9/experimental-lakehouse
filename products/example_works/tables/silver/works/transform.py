"""Bronze -> Silver for the example source.

Read top to bottom — each helper is one step of the transformation:
  1. _read_incremental  : only bronze rows newer than the last run (watermark)
  2. _parse_and_flatten : parse raw_payload against RAW_WORK  -> schema enforcement
  3. _dedupe_latest     : keep the latest row per business key
  4. apply_quality (DQX): quarantine bad rows, persist quality results
  5. _upsert            : MERGE good rows into silver (idempotent)

Persons are extracted from the same parsed frame into ``silver.persons``.
"""
from __future__ import annotations

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from lakehouse_platform.metadata.control_tables import get_watermark
from lakehouse_platform.observability.progress import progress
from lakehouse_platform.quality.dqx import apply_quality
from products.example_works.tables.silver.works.contract import BUSINESS_KEY, RAW_WORK, TABLE


def _read_incremental(spark: SparkSession, catalog: str, source: str = "example_data") -> DataFrame:
    # incremental read: cheap re-runs, only the new rows since the last watermark
    since = get_watermark(spark, catalog, source)
    bronze = f"{catalog}.bronze.{source}_records"
    return spark.table(bronze).where(F.col("ingested_at") > F.to_timestamp(F.lit(since)))


def _parse_and_flatten(df: DataFrame) -> DataFrame:
    # from_json enforces the schema: bad/missing fields -> null, extras dropped
    parsed = df.withColumn("p", F.from_json("raw_payload", RAW_WORK))
    return parsed.select(
        F.col("p.id").alias("work_id"),
        F.col("p.title").alias("title"),
        F.col("p.category").alias("category"),
        F.col("p.year").alias("year"),
        F.col("p.language").alias("language"),
        F.col("p.tags").alias("tags"),
        F.col("p.author.id").alias("author_id"),
        F.col("p.author.name").alias("author_name"),
        F.to_timestamp("p.updated_at").alias("updated_at"),
        F.col("ingested_at"),
    )


def _dedupe_latest(df: DataFrame, key: str) -> DataFrame:
    # one row per business key: the most-recently-ingested version wins
    w = Window.partitionBy(key).orderBy(F.col("ingested_at").desc())
    return df.withColumn("_rn", F.row_number().over(w)).where("_rn = 1").drop("_rn")


def transform(bronze: DataFrame, options: dict | None = None) -> DataFrame:
    """Pure ACON entrypoint: raw Bronze rows to deduplicated Works rows."""
    progress("EXAMPLE_WORKS", "Parsing and deduplicating Bronze works")
    result = _dedupe_latest(_parse_and_flatten(bronze), BUSINESS_KEY)
    progress("EXAMPLE_WORKS", "Silver Works transformation graph created")
    return result


def _upsert(spark: SparkSession, df: DataFrame, table: str, key: str) -> None:
    # MERGE = idempotent write: re-running never duplicates rows
    if spark.catalog.tableExists(table):
        (DeltaTable.forName(spark, table).alias("t")
            .merge(df.alias("s"), f"t.{key} = s.{key}")
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute())
    else:
        df.write.saveAsTable(table)


def run(spark: SparkSession, catalog: str = "dev_lakehouse", run_id: str | None = None) -> int:
    works = _dedupe_latest(
        _parse_and_flatten(_read_incremental(spark, catalog)),
        BUSINESS_KEY,
    )

    # DQX splits the frame into rows that pass and rows to quarantine
    good, _quarantine = apply_quality(spark, works, table=TABLE, run_id=run_id, catalog=catalog)
    _upsert(spark, good, f"{catalog}.{TABLE}", BUSINESS_KEY)

    return good.count()
