"""Cleaning helpers for the framework.

The pure-Python cleaning core is shared with ``src.transformations.cleaning``
(single source of truth), re-exported here so framework code has one import
site. ``CLEAN_RECORD`` is the Spark struct the cleaning UDF returns.

(In a standalone deployment this core would live inside the framework package;
here it is shared to avoid drift between the tutorial and framework layers.)
"""
from __future__ import annotations

from src.schemas.silver.records import CLEAN_RECORD  # noqa: F401
from src.transformations.cleaning import clean_record  # noqa: F401
