"""Generic REST client — source-agnostic HTTP with retries.

Deliberately generic: it knows nothing about the shape of any specific API.
Source-specific parsing happens later, in the bronze->silver transform. That
separation is what lets a new source reuse this client unchanged.
"""
from __future__ import annotations

import time
from collections.abc import Mapping
from typing import Any

import requests

from lakehouse_platform.ingestion.rate_limit import RateLimiter


class RestClient:
    def __init__(
        self,
        base_url: str,
        *,
        timeout: int = 30,
        max_retries: int = 3,
        auth=None,
        rate_limiter: RateLimiter | None = None,
        session: requests.Session | None = None,
        default_headers: Mapping[str, str] | None = None,
    ):
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.max_retries = max_retries
        self.auth = auth  # an auth strategy with .apply(headers) -> headers
        self.rate_limiter = rate_limiter
        self.session = session or requests.Session()
        self.default_headers = dict(default_headers or {})

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
            if self.rate_limiter:
                self.rate_limiter.wait()
            resp = self.session.get(
                url,
                params=params,
                headers=headers,
                timeout=self.timeout,
            )
            if resp.status_code == 200:
                return resp.json()
            transient = resp.status_code in (429, 500, 502, 503, 504)
            if transient and attempt < self.max_retries:
                retry_after = resp.headers.get("Retry-After")
                try:
                    delay = float(retry_after) if retry_after else 2 ** attempt
                except ValueError:
                    delay = 2 ** attempt
                time.sleep(max(0, delay))
                continue
            if resp.status_code == 403:
                response_text = str(getattr(resp, "text", "")).strip().replace("\n", " ")
                excerpt = response_text[:200]
                detail = f" Response: {excerpt!r}." if excerpt else ""
                raise requests.HTTPError(
                    "HTTP 403: the remote API denied this request. "
                    "Verify the configured User-Agent and whether the compute egress IP "
                    f"is allowed.{detail}",
                    response=resp,
                )
            resp.raise_for_status()
        raise RuntimeError(f"GET {url} failed after {self.max_retries} attempts")
