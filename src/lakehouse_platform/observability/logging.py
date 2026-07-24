"""Minimal logging helper.

A single place to get a consistently-formatted logger. Imported as
``src.logging.logger`` so it never collides with Python's stdlib ``logging``
(the collision would only arise if ``src/`` itself were put on sys.path as the
import root — we import via the ``src`` package instead).
"""
from __future__ import annotations

import logging


def get_logger(name: str) -> logging.Logger:
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler()
        handler.setFormatter(logging.Formatter(
            "%(asctime)s %(levelname)s %(name)s: %(message)s"))
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
    return logger
