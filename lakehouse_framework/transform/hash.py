"""Surrogate-key hashing (the dp_fk_hash convention).

A business key (string) hashed to a stable bigint, so the same key yields the
same surrogate everywhere — facts and dimensions line up on join without a
central key table. ``xxhash64`` is deterministic and fast.
"""
from __future__ import annotations

from pyspark.sql import functions as F


def dp_fk_hash(column: str):
    """Return a Column: deterministic bigint surrogate key from a business key."""
    return F.xxhash64(F.col(column).cast("string"))
