"""Pure-Python loading of reviewed Philosophy corpus intent."""
from __future__ import annotations

import json
from pathlib import Path

APPROVED_STATUSES = {"matched", "matched_without_plain_text"}


def load_selection(path: str | Path) -> list[dict]:
    report = json.loads(Path(path).read_text(encoding="utf-8"))
    corpus_id = str(report["corpus"]["id"])
    rows = []
    for work in report["works"]:
        if work.get("status") not in APPROVED_STATUSES:
            continue
        rows.append(
            {
                "corpus_id": corpus_id,
                "corpus_work_id": str(work["id"]),
                "gutenberg_id": str(work["gutendex_id"]),
                "period": str(work["period"]),
                "canonical_author": str(work["author"]),
                "canonical_title": str(work["title"]),
                "match_status": str(work["status"]),
                "text_url": work.get("text_url"),
            }
        )
    if len({row["corpus_work_id"] for row in rows}) != len(rows):
        raise ValueError("Approved corpus contains duplicate corpus_work_id values")
    return rows
