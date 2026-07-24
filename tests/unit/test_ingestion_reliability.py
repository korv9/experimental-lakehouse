from __future__ import annotations

import hashlib

import pytest
import requests

from lakehouse_platform.ingestion.files import ChecksumMismatch, download_file
from lakehouse_platform.ingestion.identity import stable_ingestion_id
from lakehouse_platform.ingestion.pagination.cursor import paginate
from lakehouse_platform.ingestion.rate_limit import RateLimiter


class FakeResponse:
    def __init__(self, content: bytes, status_code: int = 200, headers=None):
        self.content = content
        self.status_code = status_code
        self.headers = {
            "Content-Length": str(len(content)),
            **(headers or {}),
        }

    def iter_content(self, chunk_size: int):
        for offset in range(0, len(self.content), chunk_size):
            yield self.content[offset : offset + chunk_size]

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.HTTPError(f"HTTP {self.status_code}", response=self)


class FakeDownloadSession:
    def __init__(self, response: FakeResponse):
        self.response = response
        self.calls = []

    def get(self, url, **kwargs):
        self.calls.append((url, kwargs))
        return self.response


def _sha256(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()


def test_download_commits_verified_file_atomically(tmp_path):
    content = b"the republic\n" * 10
    session = FakeDownloadSession(FakeResponse(content))
    target = tmp_path / "gutenberg" / "1497.txt"

    result = download_file(
        "https://example.test/1497.txt",
        target,
        expected_sha256=_sha256(content),
        session=session,
        chunk_size=7,
    )

    assert result.downloaded
    assert not result.resumed
    assert result.sha256 == _sha256(content)
    assert target.read_bytes() == content
    assert not target.with_name("1497.txt.part").exists()


def test_download_resumes_partial_file_when_server_supports_ranges(tmp_path):
    initial = b"already downloaded "
    remaining = b"and now complete"
    target = tmp_path / "book.txt"
    partial = target.with_name("book.txt.part")
    partial.write_bytes(initial)
    session = FakeDownloadSession(FakeResponse(remaining, status_code=206))

    result = download_file(
        "https://example.test/book.txt",
        target,
        expected_sha256=_sha256(initial + remaining),
        session=session,
    )

    assert result.resumed
    assert target.read_bytes() == initial + remaining
    assert session.calls[0][1]["headers"]["Range"] == f"bytes={len(initial)}-"


def test_download_rejects_bad_checksum_and_does_not_publish_file(tmp_path):
    target = tmp_path / "book.txt"

    with pytest.raises(ChecksumMismatch):
        download_file(
            "https://example.test/book.txt",
            target,
            expected_sha256="0" * 64,
            session=FakeDownloadSession(FakeResponse(b"unexpected")),
        )

    assert not target.exists()
    assert not target.with_name("book.txt.part").exists()


def test_existing_verified_file_is_idempotent_without_network_call(tmp_path):
    target = tmp_path / "book.txt"
    target.write_bytes(b"stable")
    session = FakeDownloadSession(FakeResponse(b"must not be used"))

    result = download_file(
        "https://example.test/book.txt",
        target,
        expected_sha256=_sha256(b"stable"),
        session=session,
    )

    assert not result.downloaded
    assert session.calls == []


def test_rate_limiter_spaces_calls_with_injected_clock():
    state = {"now": 10.0}
    sleeps = []

    def clock():
        return state["now"]

    def sleep(seconds):
        sleeps.append(seconds)
        state["now"] += seconds

    limiter = RateLimiter(2, clock=clock, sleeper=sleep)

    assert limiter.wait() == 0
    assert limiter.wait() == 0.5
    assert sleeps == [0.5]


class CursorClient:
    def __init__(self):
        self.params = []

    def get(self, endpoint, params=None):
        self.params.append(params)
        cursor = (params or {}).get("after")
        if cursor is None:
            return {"data": {"items": [{"id": 1}], "next": "cursor-2"}}
        return {"data": {"items": [{"id": 2}], "next": None}}


def test_cursor_pagination_checkpoints_after_each_page():
    client = CursorClient()
    checkpoints = []
    config = {
        "cursor_parameter": "after",
        "page_size_parameter": "limit",
        "page_size": 100,
        "records_path": "data.items",
        "next_cursor_path": "data.next",
    }

    pages = list(
        paginate(
            client,
            "/works",
            config,
            checkpoint=lambda cursor, page: checkpoints.append((cursor, page)),
        )
    )

    assert [page.records[0]["id"] for page in pages] == [1, 2]
    assert client.params == [{"limit": 100}, {"after": "cursor-2", "limit": 100}]
    assert checkpoints == [("cursor-2", 1), (None, 2)]


def test_cursor_pagination_rejects_repeated_cursor():
    class RepeatingClient:
        def get(self, endpoint, params=None):
            return {"results": [], "next_cursor": "same"}

    with pytest.raises(RuntimeError, match="repeated cursor"):
        list(paginate(RepeatingClient(), "/", {}, initial_cursor="same"))


def test_ingestion_id_is_stable_for_replayed_content_and_changes_with_payload():
    first = stable_ingestion_id("gutendex", "1497", '{"title":"The Republic"}')
    replay = stable_ingestion_id("gutendex", "1497", '{"title":"The Republic"}')
    changed = stable_ingestion_id("gutendex", "1497", '{"title":"Republic"}')

    assert first == replay
    assert first != changed
