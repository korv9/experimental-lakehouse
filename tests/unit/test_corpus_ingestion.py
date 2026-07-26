from __future__ import annotations

from pathlib import Path

import pytest

from lakehouse_platform.ingestion.corpus import (
    id_batches,
    load_corpus_selection,
    resolve_product_path,
)
from products.philosophy_litterature.tables.bronze.philosophy_litterature_work_raw.contract import (
    TableDefinition,
)

REPORT = Path("datasets/api_samples/philosophy_corpus_report.json")


def test_report_becomes_deduplicated_approved_gutendex_ids():
    selection = load_corpus_selection(REPORT)

    assert selection.corpus_id == "philosophy_foundations_v1"
    assert len(selection.source_record_ids) == 53
    assert len(set(selection.source_record_ids)) == 53
    assert selection.duplicate_source_ids == ("205",)
    assert "55201" in selection.source_record_ids
    assert "5740" in selection.source_record_ids


def test_review_and_not_found_candidates_cannot_enter_bronze_selection():
    selection = load_corpus_selection(REPORT, accepted_statuses=("matched",))

    assert len(selection.source_record_ids) == 52
    assert "54672" not in selection.source_record_ids  # Diderot false-positive review
    assert "5740" not in selection.source_record_ids  # no plain-text format


def test_gutendex_batches_are_complete_and_bounded():
    source_ids = tuple(str(number) for number in range(53))

    batches = id_batches(source_ids, 25)

    assert tuple(item for batch in batches for item in batch) == source_ids
    assert [len(batch) for batch in batches] == [25, 25, 3]
    with pytest.raises(ValueError, match="between 1 and 32"):
        id_batches(source_ids, 33)


def test_source_relative_report_path_resolves_to_versioned_artifact():
    path = resolve_product_path(
        "config/sources/philosophy_gutendex.yaml",
        "../../datasets/api_samples/philosophy_corpus_report.json",
    )

    assert path == REPORT.resolve()
    assert path.exists()


def test_bronze_contract_matches_ingestion_envelope():
    assert TableDefinition.object_location() == (
        "bronze.philosophy_litterature_work_raw"
    )
    assert TableDefinition.primary_keys() == ["ingestion_id"]
    assert TableDefinition.column_names() == [
        "ingestion_id",
        "source_name",
        "source_endpoint",
        "ingested_at",
        "batch_id",
        "run_id",
        "request_parameters",
        "http_status",
        "source_record_id",
        "raw_payload",
        "schema_version",
    ]
