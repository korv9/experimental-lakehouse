"""Governed contract for ``silver.transactions`` — one typed transaction line.

Everything here is non-nullable except ``price``, because the error-level rules
in quality.yaml quarantine rows missing any of it before the write. A
transaction with no date or no article cannot be aggregated into demand, and
silently averaging over it would understate every article it touches.
"""
from dataclasses import dataclass
from datetime import date, datetime

from lakehouse_platform.schemas.base import BaseSchema
from lakehouse_platform.schemas.types import Double, Int, String


@dataclass
class TableDefinition(BaseSchema):
    transaction_id: String
    transaction_date: date
    customer_id: String
    article_id: String
    sales_channel_id: Int
    price: Double | None
    ingested_at: datetime

    class Meta:
        object_name = "transactions"
        object_location = "silver.transactions"
        object_description = (
            "Typed, validated H&M transaction lines. One row per purchased "
            "item. transaction_id is a deterministic surrogate: the source has "
            "no key, so reruns must reproduce it exactly for MERGE to be "
            "idempotent."
        )
        column_constraints = {
            "transaction_id": {"PK": True},
            "customer_id": {"NOT_BLANK": True},
            "article_id": {"NOT_BLANK": True},
        }
        custom_table_properties = {"delta.enableChangeDataFeed": "true"}
        column_comments = {
            "transaction_id": (
                "Deterministic surrogate: hash of the identifying fields plus "
                "the occurrence number within that group."
            ),
            "transaction_date": "Date of purchase (source column t_dat).",
            "customer_id": "Hashed customer identifier from the source.",
            "article_id": "Natural key of the purchased article. String, not numeric.",
            "sales_channel_id": "1 = store, 2 = online.",
            "price": (
                "Scaled and anonymised by the source. Comparable across "
                "articles, not a currency amount."
            ),
            "ingested_at": "When the underlying Bronze row landed.",
        }
