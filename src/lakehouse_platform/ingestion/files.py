"""Reliable HTTP file downloads for Unity Catalog volume landing zones."""
from __future__ import annotations

import hashlib
import os
import time
from dataclasses import dataclass
from pathlib import Path

import requests

from lakehouse_platform.ingestion.rate_limit import RateLimiter
from lakehouse_platform.observability.progress import progress


class ChecksumMismatch(ValueError):
    """Raised when downloaded bytes do not match the expected SHA-256."""


@dataclass(frozen=True)
class DownloadResult:
    path: Path
    sha256: str
    size_bytes: int
    downloaded: bool
    resumed: bool


def sha256_file(path: str | Path, chunk_size: int = 1024 * 1024) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        while chunk := handle.read(chunk_size):
            digest.update(chunk)
    return digest.hexdigest()


def _retry_delay(response: requests.Response | None, attempt: int) -> float:
    if response is not None:
        retry_after = response.headers.get("Retry-After")
        if retry_after:
            try:
                return max(0.0, float(retry_after))
            except ValueError:
                pass
    return float(2 ** (attempt - 1))


def download_file(
    url: str,
    destination: str | Path,
    *,
    expected_sha256: str | None = None,
    headers: dict[str, str] | None = None,
    timeout: float = 60,
    max_retries: int = 3,
    chunk_size: int = 1024 * 1024,
    session: requests.Session | None = None,
    rate_limiter: RateLimiter | None = None,
) -> DownloadResult:
    """Download atomically with resume support and optional checksum validation.

    ``destination`` should normally be beneath a fully qualified Unity Catalog
    volume path such as ``/Volumes/catalog/landing/source_files/gutenberg/...``.
    """
    if max_retries < 1:
        raise ValueError("max_retries must be at least one")
    target = Path(destination)
    target.parent.mkdir(parents=True, exist_ok=True)

    if target.exists():
        digest = sha256_file(target, chunk_size)
        if expected_sha256 and digest.lower() != expected_sha256.lower():
            raise ChecksumMismatch(
                f"Existing file {target} has SHA-256 {digest}, expected {expected_sha256}"
            )
        progress("DOWNLOAD", "Using existing verified file", path=target, sha256=digest)
        return DownloadResult(target, digest, target.stat().st_size, False, False)

    partial = target.with_name(f"{target.name}.part")
    client = session or requests.Session()
    base_headers = dict(headers or {})
    last_error: Exception | None = None

    for attempt in range(1, max_retries + 1):
        resumed_from = partial.stat().st_size if partial.exists() else 0
        request_headers = dict(base_headers)
        if resumed_from:
            request_headers["Range"] = f"bytes={resumed_from}-"
        if rate_limiter:
            waited = rate_limiter.wait()
            if waited:
                progress("DOWNLOAD", "Rate limit wait", seconds=round(waited, 3))

        response: requests.Response | None = None
        try:
            progress(
                "DOWNLOAD",
                "Requesting file",
                url=url,
                attempt=attempt,
                resume_bytes=resumed_from,
            )
            response = client.get(
                url,
                headers=request_headers,
                timeout=timeout,
                stream=True,
            )
            if response.status_code in {429, 500, 502, 503, 504}:
                response.raise_for_status()
            response.raise_for_status()

            append = resumed_from > 0 and response.status_code == 206
            if resumed_from and not append:
                progress("DOWNLOAD", "Server ignored Range; restarting file", path=partial)
            mode = "ab" if append else "wb"
            bytes_from_response = 0
            with partial.open(mode) as handle:
                for chunk in response.iter_content(chunk_size=chunk_size):
                    if chunk:
                        handle.write(chunk)
                        bytes_from_response += len(chunk)

            content_length = response.headers.get("Content-Length")
            content_encoding = response.headers.get("Content-Encoding", "identity")
            if (
                content_length
                and content_encoding == "identity"
                and bytes_from_response != int(content_length)
            ):
                raise OSError(
                    f"Incomplete response: received {bytes_from_response} of "
                    f"{content_length} bytes"
                )

            digest = sha256_file(partial, chunk_size)
            if expected_sha256 and digest.lower() != expected_sha256.lower():
                partial.unlink(missing_ok=True)
                raise ChecksumMismatch(
                    f"Downloaded SHA-256 {digest}, expected {expected_sha256}"
                )

            os.replace(partial, target)
            size = target.stat().st_size
            progress("DOWNLOAD", "File committed", path=target, bytes=size, sha256=digest)
            return DownloadResult(target, digest, size, True, append)
        except ChecksumMismatch:
            raise
        except (OSError, requests.RequestException) as error:
            last_error = error
            if attempt == max_retries:
                break
            delay = _retry_delay(response, attempt)
            progress("DOWNLOAD", "Retrying download", delay_seconds=delay, error=str(error))
            time.sleep(delay)
        finally:
            close = getattr(response, "close", None)
            if close:
                close()

    raise RuntimeError(f"Download failed after {max_retries} attempts: {url}") from last_error
