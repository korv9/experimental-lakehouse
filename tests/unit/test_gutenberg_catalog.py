from __future__ import annotations

import gzip
import json
from datetime import date

import pytest

from lakehouse_platform.ingestion.catalog_files import (
    validate_gzip_csv,
    write_artifact_manifest,
)
from lakehouse_platform.ingestion.files import DownloadResult
from products.philosophy_litterature.selection import load_selection


def _catalog(path, header="Text#,Type,Issued,Title,Language,Authors,Subjects,LoCC,Bookshelves"):
    with gzip.open(path, "wt", encoding="utf-8", newline="") as handle:
        handle.write(header + "\n")
        handle.write("1656,Text,1999-05-01,Apology,en,Plato,Philosophy,B,Classical Antiquity\n")


def test_compressed_catalog_validation_reads_header_and_entire_stream(tmp_path):
    path = tmp_path / "pg_catalog.csv.gz"
    _catalog(path)

    header = validate_gzip_csv(path, ["Text#", "Title", "Authors"])

    assert header[0] == "Text#"
    assert header[-1] == "Bookshelves"


def test_compressed_catalog_validation_rejects_schema_drift(tmp_path):
    path = tmp_path / "pg_catalog.csv.gz"
    _catalog(path, header="Text#,Title")

    with pytest.raises(ValueError, match="missing required columns"):
        validate_gzip_csv(path, ["Text#", "Title", "Authors"])


def test_artifact_manifest_is_idempotent_and_checksum_guarded(tmp_path):
    path = tmp_path / "pg_catalog.csv.gz"
    path.write_bytes(b"snapshot")
    result = DownloadResult(path, "a" * 64, 8, True, False, '"etag"', "today")

    manifest = write_artifact_manifest(
        result,
        source_name="project_gutenberg_catalog",
        source_url="https://example.test/catalog.csv.gz",
        snapshot_date=date(2026, 7, 26),
    )
    replay = write_artifact_manifest(
        result,
        source_name="project_gutenberg_catalog",
        source_url="https://example.test/catalog.csv.gz",
        snapshot_date=date(2026, 7, 26),
    )

    assert replay == manifest
    assert json.loads(manifest.read_text(encoding="utf-8"))["sha256"] == "a" * 64


def test_reviewed_report_selects_54_corpus_works_and_53_source_ids():
    rows = load_selection("datasets/api_samples/philosophy_corpus_report.json")

    assert len(rows) == 54
    assert len({row["corpus_work_id"] for row in rows}) == 54
    assert len({row["gutenberg_id"] for row in rows}) == 53
    assert {row["match_status"] for row in rows} == {
        "matched",
        "matched_without_plain_text",
    }
