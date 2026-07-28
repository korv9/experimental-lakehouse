"""Lookup-mode ingestion, driven with a fake client (no Spark, no network).

Lookup mode is what identifier-resolution APIs need: one request per value, no
pages. The behaviour worth pinning down is what happens when a lookup *fails* —
the row must still be written, carrying its HTTP status, so an unresolved drug
stays visible instead of quietly shrinking the drug list.
"""
import json

import pytest

from lakehouse_platform.ingestion.runner import _lookup_rows

CONFIG = {
    "lookup": {
        "name": "pubchem_name",
        "endpoint_template": "/compound/name/{value}/property/JSON",
        "fallback_endpoint_templates": ["/compound/xref/rn/{value}/cids/JSON"],
    }
}

COMMON = {
    "source": "pubchem_compound",
    "run_id": "run-1",
    "batch_id": "batch-1",
    "ingested_at": "2026-07-28T00:00:00Z",
    "schema_version": "v1",
}


class _FakeResponse:
    def __init__(self, status_code):
        self.status_code = status_code


class _FakeError(Exception):
    def __init__(self, status_code):
        super().__init__(f"HTTP {status_code}")
        self.response = _FakeResponse(status_code)


class _FakeClient:
    """Answers from a dict of endpoint -> payload; anything else raises 404."""

    def __init__(self, answers, fail_status=404):
        self.answers = answers
        self.fail_status = fail_status
        self.calls = []

    def get(self, endpoint, params=None):
        self.calls.append(endpoint)
        if endpoint in self.answers:
            return self.answers[endpoint]
        raise _FakeError(self.fail_status)


def test_resolved_lookup_records_the_payload_and_status():
    client = _FakeClient({
        "/compound/name/aspirin/property/JSON": {"PropertyTable": {"Properties": [{"CID": 2244}]}}
    })
    rows = _lookup_rows(client, CONFIG, ["aspirin"], **COMMON)

    assert len(rows) == 1
    row = rows[0]
    assert row["http_status"] == 200
    assert row["source_record_id"] == "aspirin"
    assert json.loads(row["raw_payload"])["PropertyTable"]["Properties"][0]["CID"] == 2244
    assert json.loads(row["request_parameters"])["lookup"] == "pubchem_name"


def test_failed_lookup_is_still_written_with_its_status():
    """A miss is data. Dropping it would make coverage look better than it is."""
    client = _FakeClient({}, fail_status=404)
    rows = _lookup_rows(client, CONFIG, ["not-a-drug"], **COMMON)

    assert len(rows) == 1
    assert rows[0]["http_status"] == 404
    assert rows[0]["raw_payload"] == "{}"
    assert rows[0]["source_record_id"] == "not-a-drug"


def test_fallback_template_is_tried_when_the_first_misses():
    client = _FakeClient({"/compound/xref/rn/50-78-2/cids/JSON": {"IdentifierList": {"CID": [2244]}}})
    rows = _lookup_rows(client, CONFIG, ["50-78-2"], **COMMON)

    assert rows[0]["http_status"] == 200
    assert client.calls == [
        "/compound/name/50-78-2/property/JSON",       # primary, misses
        "/compound/xref/rn/50-78-2/cids/JSON",        # fallback, hits
    ]


def test_values_are_url_encoded():
    """Drug names contain spaces, commas and slashes; a raw name breaks the path."""
    client = _FakeClient({})
    _lookup_rows(client, CONFIG, ["5-fluorouracil, sodium/salt"], **COMMON)

    assert " " not in client.calls[0]
    assert "%2C" in client.calls[0] and "%2F" in client.calls[0]


def test_each_value_produces_exactly_one_row():
    client = _FakeClient({"/compound/name/a/property/JSON": {"ok": True}})
    rows = _lookup_rows(client, CONFIG, ["a", "b", "c"], **COMMON)

    assert [row["source_record_id"] for row in rows] == ["a", "b", "c"]
    assert [row["http_status"] for row in rows] == [200, 404, 404]


def test_ingestion_id_is_stable_for_the_same_payload():
    """Re-running a resolved lookup must not create a second Bronze row."""
    answers = {"/compound/name/aspirin/property/JSON": {"CID": 2244}}
    first = _lookup_rows(_FakeClient(answers), CONFIG, ["aspirin"], **COMMON)
    second = _lookup_rows(_FakeClient(answers), CONFIG, ["aspirin"], **COMMON)

    assert first[0]["ingestion_id"] == second[0]["ingestion_id"]


def test_a_changed_payload_produces_a_new_version():
    old = _lookup_rows(
        _FakeClient({"/compound/name/x/property/JSON": {"CID": 1}}), CONFIG, ["x"], **COMMON
    )
    new = _lookup_rows(
        _FakeClient({"/compound/name/x/property/JSON": {"CID": 2}}), CONFIG, ["x"], **COMMON
    )
    assert old[0]["ingestion_id"] != new[0]["ingestion_id"]


def test_lookup_mode_requires_a_lookup_section():
    from lakehouse_platform.ingestion.runner import _ingest_lookups

    with pytest.raises(ValueError, match="lookup"):
        _ingest_lookups(None, {"source_name": "x"}, "dev", ["a"])
