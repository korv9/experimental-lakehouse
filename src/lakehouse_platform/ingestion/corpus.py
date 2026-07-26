"""Pure helpers for turning an approved discovery report into API ID batches."""
from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class CorpusSelection:
    corpus_id: str
    source_record_ids: tuple[str, ...]
    accepted_statuses: tuple[str, ...]
    duplicate_source_ids: tuple[str, ...]


def load_corpus_selection(
    report_path: str | Path,
    *,
    accepted_statuses: tuple[str, ...] = ("matched", "matched_without_plain_text"),
) -> CorpusSelection:
    """Load reviewed Gutendex IDs without coupling the platform to product YAML."""
    path = Path(report_path)
    document = json.loads(path.read_text(encoding="utf-8"))
    corpus = document.get("corpus") or {}
    works = document.get("works")
    if not corpus.get("id") or not isinstance(works, list):
        raise ValueError(f"{path} is not a valid corpus discovery report")

    selected: list[str] = []
    duplicates: set[str] = set()
    seen: set[str] = set()
    for work in works:
        if not isinstance(work, dict) or work.get("status") not in accepted_statuses:
            continue
        source_id = work.get("gutendex_id")
        if source_id is None:
            raise ValueError(f"Accepted work {work.get('id')!r} has no gutendex_id")
        normalized = str(source_id)
        if normalized in seen:
            duplicates.add(normalized)
            continue
        seen.add(normalized)
        selected.append(normalized)

    if not selected:
        raise ValueError(f"{path} contains no works with statuses {accepted_statuses}")
    return CorpusSelection(
        corpus_id=str(corpus["id"]),
        source_record_ids=tuple(selected),
        accepted_statuses=accepted_statuses,
        duplicate_source_ids=tuple(sorted(duplicates)),
    )


def id_batches(source_record_ids: tuple[str, ...], batch_size: int) -> tuple[tuple[str, ...], ...]:
    if batch_size < 1 or batch_size > 32:
        raise ValueError("Gutendex batch_size must be between 1 and 32")
    return tuple(
        source_record_ids[offset : offset + batch_size]
        for offset in range(0, len(source_record_ids), batch_size)
    )


def resolve_product_path(config_path: str | Path, configured_path: str) -> Path:
    """Resolve a source-config path relative to that YAML file."""
    path = Path(configured_path)
    return path if path.is_absolute() else (Path(config_path).resolve().parent / path).resolve()


def selection_options(config: dict[str, Any]) -> tuple[tuple[str, ...], int, str]:
    selection = config.get("selection") or {}
    if selection.get("type") != "corpus_report":
        raise ValueError("selection.type must be 'corpus_report'")
    statuses = tuple(selection.get("accepted_statuses", ["matched"]))
    if not statuses or not all(isinstance(status, str) for status in statuses):
        raise ValueError("selection.accepted_statuses must contain strings")
    return statuses, int(selection.get("batch_size", 25)), str(selection.get("id_parameter", "ids"))
