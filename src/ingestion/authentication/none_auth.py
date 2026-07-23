"""'No authentication' strategy (example.com/data is public).

All auth strategies share one shape: ``apply(headers) -> headers``. Swapping in
an API-key or OAuth strategy later doesn't touch the generic REST client — it
just receives a different auth object.
"""
from __future__ import annotations


class NoAuth:
    def apply(self, headers: dict) -> dict:
        return headers
