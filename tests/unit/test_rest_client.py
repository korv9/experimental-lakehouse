import requests

from lakehouse_platform.ingestion.clients import rest
from lakehouse_platform.ingestion.clients.rest import RestClient


class Response:
    def __init__(self, status_code, body=None, headers=None):
        self.status_code = status_code
        self.body = body or {}
        self.headers = headers or {}

    def json(self):
        return self.body

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.HTTPError(f"HTTP {self.status_code}", response=self)


class Session:
    def __init__(self, responses):
        self.responses = iter(responses)
        self.calls = []

    def get(self, url, **kwargs):
        self.calls.append((url, kwargs))
        return next(self.responses)


class Limiter:
    def __init__(self):
        self.calls = 0

    def wait(self):
        self.calls += 1


def test_rest_client_retries_429_and_honors_retry_after(monkeypatch):
    session = Session(
        [
            Response(429, headers={"Retry-After": "0.25"}),
            Response(200, {"results": [{"id": 1}]}),
        ]
    )
    limiter = Limiter()
    sleeps = []
    monkeypatch.setattr(rest.time, "sleep", sleeps.append)
    client = RestClient(
        "https://api.example.test",
        session=session,
        rate_limiter=limiter,
    )

    result = client.get("/works", params={"page": 1})

    assert result == {"results": [{"id": 1}]}
    assert len(session.calls) == 2
    assert limiter.calls == 2
    assert sleeps == [0.25]


def test_rest_client_does_not_retry_non_transient_error():
    client = RestClient(
        "https://api.example.test",
        session=Session([Response(404)]),
    )

    try:
        client.get("/missing")
    except requests.HTTPError as error:
        assert error.response.status_code == 404
    else:
        raise AssertionError("Expected an HTTPError")
