"""Bronze -> Silver: type the transaction lines and give them a stable key.

The source has **no transaction id**. A customer buying the same article twice
on the same day at the same price is two legitimate rows that are identical in
every field, so there is nothing to merge on — and a Delta MERGE without a key
either collapses real sales or duplicates them on every rerun.

The fix is a deterministic surrogate: hash the identifying fields, then number
the occurrences within that group. Rerunning the same batch produces exactly the
same keys, so the merge is idempotent; two genuine same-day purchases stay two
rows. That is the whole reason this module is more than a set of casts.

Reading order:

``_parse``       payload JSON -> typed columns
``_key``         the deterministic surrogate described above
``_select``      contract shape
"""
from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

# Payload fields, exactly as the Kaggle export names them.
PAYLOAD_FIELDS = ("t_dat", "customer_id", "article_id", "price", "sales_channel_id")

# What makes a transaction line distinguishable, before the occurrence counter.
IDENTIFYING = ("transaction_date", "customer_id", "article_id", "sales_channel_id", "price")


def _parse(df: DataFrame) -> DataFrame:
    """Pull typed columns out of the raw JSON payload.

    ``article_id`` stays a string. It is numeric in the export, but it is a
    natural key, and the moment it becomes an integer somewhere it starts losing
    leading zeros and failing to join. Keys are strings.

    ``price`` is scaled and anonymised by H&M — it is not a currency amount.
    Revenue built from it is comparable across articles, not reportable.
    """
    from pyspark.sql import functions as F

    def field(name: str):
        return F.get_json_object("raw_payload", f"$['{name}']")

    return df.select(
        F.to_date(field("t_dat")).alias("transaction_date"),
        F.trim(field("customer_id")).alias("customer_id"),
        F.trim(field("article_id")).alias("article_id"),
        field("price").cast("double").alias("price"),
        field("sales_channel_id").cast("int").alias("sales_channel_id"),
        F.col("ingested_at"),
    )


def _key(df: DataFrame) -> DataFrame:
    """Deterministic surrogate key: hash of the identifying fields + occurrence.

    ``row_number`` over a window partitioned by the hash needs a deterministic
    ordering to be reproducible. Every column in the partition is identical by
    construction, so ordering by the hash itself is stable and arbitrary in the
    only way that is safe: the rows it distinguishes are indistinguishable.
    """
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    from lakehouse_platform.transforms.hashing import internal_id_hash

    fingerprint = internal_id_hash(*IDENTIFYING)
    window = Window.partitionBy("_fingerprint").orderBy("_fingerprint")

    return (
        df.withColumn("_fingerprint", fingerprint)
        .withColumn("_occurrence", F.row_number().over(window))
        .withColumn(
            "transaction_id",
            F.concat_ws("-", F.col("_fingerprint").cast("string"), F.col("_occurrence")),
        )
        .drop("_fingerprint", "_occurrence")
    )


def _select(df: DataFrame) -> DataFrame:
    from pyspark.sql import functions as F

    return df.select(
        "transaction_id",
        "transaction_date",
        "customer_id",
        "article_id",
        "sales_channel_id",
        "price",
        F.col("ingested_at"),
    )


def transform(df: DataFrame, options: dict | None = None) -> DataFrame:
    """Raw payloads -> one typed, uniquely keyed row per transaction line."""
    return _select(_key(_parse(df)))
