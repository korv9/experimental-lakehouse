"""Stable identifiers for idempotent source ingestion."""
from __future__ import annotations

import hashlib


def stable_ingestion_id(source: str, source_record_id: str, raw_payload: str) -> str:
    """Identify one source version so replayed pages do not duplicate Bronze."""
    value = f"{source}\x00{source_record_id}\x00{raw_payload}"
    return hashlib.sha256(value.encode("utf-8")).hexdigest()
