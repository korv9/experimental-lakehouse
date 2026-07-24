"""Generic REST client — source-agnostic HTTP with retries.

Deliberately generic: it knows nothing about the shape of any specific API.
Source-specific parsing happens later, in the bronze->silver transform. That
separation is what lets a new source reuse this client unchanged.
"""
from __future__ import annotations

import time
from typing import Any

import requests


class RestClient:
    def __init__(self, base_url: str, *, timeout: int = 30, max_retries: int = 3,
                 auth=None, default_headers: dict[str, str] | None = None):
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.max_retries = max_retries
        self.auth = auth  # an auth strategy with .apply(headers) -> headers
        self.default_headers = default_headers or {}  # e.g. {"Accept": "application/ld+json"}

    def get(self, endpoint: str, params: dict[str, Any] | None = None) -> dict:
        """GET one page and return parsed JSON.

        Retries transient failures (429/5xx) with exponential backoff, then
        raises so the pipeline run is marked *failed* rather than silently empty.
        """
        url = f"{self.base_url}/{endpoint.lstrip('/')}"
        headers = dict(self.default_headers)
        if self.auth:
            headers = self.auth.apply(headers)

        for attempt in range(1, self.max_retries + 1):
            resp = requests.get(url, params=params, headers=headers, timeout=self.timeout)
            if resp.status_code == 200:
                return resp.json()
            transient = resp.status_code in (429, 500, 502, 503)
            if transient and attempt < self.max_retries:
                time.sleep(2 ** attempt)  # back off: 2s, 4s, 8s
                continue
            resp.raise_for_status()
        raise RuntimeError(f"GET {url} failed after {self.max_retries} attempts")
