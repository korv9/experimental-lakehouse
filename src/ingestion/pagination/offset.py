"""Offset/limit pagination strategy (e.g. Libris /find: ``_offset`` + ``_limit``).

Like page-number pagination, but the API walks by row offset rather than page
index. The runner drives the loop (offset += page_size) and stops when it has
covered ``totalItems``. Kept as a simple param-builder so it composes with the
generic ingestion runner.
"""
from __future__ import annotations


def offset_params(offset: int, cfg: dict) -> dict:
    """Return the query params for one page, from the source's pagination config."""
    return {
        cfg["offset_parameter"]: offset,
        cfg["limit_parameter"]: cfg["page_size"],
    }
