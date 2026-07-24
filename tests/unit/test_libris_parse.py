"""Unit tests for the Libris JSON-LD parser (no Spark required).

Runs against the bundled representative /find response, so it documents exactly
what the parser extracts from a real-shaped record.
"""
import json
from pathlib import Path

from src.transformations.libris_parse import join_name, parse_libris_item

SAMPLE = Path(__file__).resolve().parents[2] / "datasets" / "libris" / "sample_find_response.json"


def _items():
    return json.loads(SAMPLE.read_text())["items"]


def test_join_name_combines_given_and_family():
    assert join_name({"givenName": "Astrid", "familyName": "Lindgren"}) == "Astrid Lindgren"
    assert join_name({"familyName": "Lagerlöf"}) == "Lagerlöf"
    assert join_name({"label": "Rabén & Sjögren"}) == "Rabén & Sjögren"
    assert join_name(None) is None


def test_parse_pippi_record():
    rec = parse_libris_item(_items()[0])
    assert rec["record_id"] == "s93ns5m41x9c2gd"          # from meta.@id
    assert rec["title"] == "Pippi Långstrump"
    assert rec["creators"] == ["Astrid Lindgren", "Ingrid Nyman"]   # joined + both roles
    assert rec["subjects"] == ["Barnböcker", "Bilderböcker"]
    assert rec["year"] == 1945
    assert rec["language"] == "swe"                        # last segment of the id.kb.se URI
    assert rec["isbn"] == "9789129697285"
    assert rec["publisher"] == "Rabén & Sjögren"
    assert rec["updated_at"] == "2023-04-12"               # date part of meta.modified


def test_parse_second_record_single_author():
    rec = parse_libris_item(_items()[1])
    assert rec["record_id"] == "abc123def456"
    assert rec["creators"] == ["Selma Lagerlöf"]
    assert rec["year"] == 1906
    assert rec["title"].startswith("Nils Holgersson")


def test_schema_keys_present_even_when_sparse():
    rec = parse_libris_item({"@id": "https://libris.kb.se/x#it"})
    assert rec["record_id"] == "x"
    assert rec["creators"] == [] and rec["subjects"] == []
    assert set(rec) == {
        "record_id", "title", "creators", "subjects", "year",
        "language", "isbn", "publisher", "updated_at",
    }
