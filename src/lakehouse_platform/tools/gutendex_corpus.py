"""Gutendex discovery checks for a versioned product corpus."""
from __future__ import annotations

import json
import re
import time
import unicodedata
from collections import Counter
from dataclasses import replace
from pathlib import Path
from typing import Any

import requests
import yaml

from lakehouse_platform.tools.api_explorer import execute_request, load_request

IGNORED_TITLE_WORDS = {"a", "an", "and", "of", "on", "or", "the", "to"}
REQUIRED_WORK_FIELDS = {"id", "period", "author", "title", "query"}


def normalized_words(value: str) -> set[str]:
    normalized = unicodedata.normalize("NFKD", value).encode("ascii", "ignore").decode()
    return {
        word
        for word in re.findall(r"[a-z0-9]+", normalized.lower())
        if word not in IGNORED_TITLE_WORDS
    }


def plain_text_url(book: dict[str, Any]) -> str | None:
    """Prefer a UTF-8/plain text download candidate from Gutendex formats."""
    formats = book.get("formats") or {}
    ordered = sorted(
        formats.items(),
        key=lambda item: (
            "utf-8" not in item[0].lower(),
            not item[0].lower().startswith("text/plain"),
        ),
    )
    for mime_type, url in ordered:
        if mime_type.lower().startswith("text/plain") and isinstance(url, str):
            return url
    return None


def match_work(work: dict[str, Any], results: list[Any]) -> dict[str, Any]:
    """Rank candidates and retain evidence for a human edition review."""
    expected_titles = [str(work["title"]), *map(str, work.get("title_aliases", []))]
    expected_author = normalized_words(str(work["author"]))
    if str(work["author"]).casefold() == "anonymous":
        expected_author = set()

    ranked: list[tuple[float, bool, float, dict[str, Any]]] = []
    for candidate in results:
        if not isinstance(candidate, dict):
            continue
        candidate_title = normalized_words(str(candidate.get("title", "")))
        title_score = max(
            (
                len(candidate_title & normalized_words(title))
                / max(1, len(normalized_words(title)))
                for title in expected_titles
            ),
            default=0.0,
        )
        candidate_authors = normalized_words(
            " ".join(
                str(author.get("name", ""))
                for author in candidate.get("authors", [])
                if isinstance(author, dict)
            )
        )
        author_match = not expected_author or bool(expected_author & candidate_authors)
        ranked.append((title_score + (0.25 if author_match else 0), author_match, title_score, candidate))

    if not ranked:
        return {"status": "not_found", "candidate_count": 0}

    _, author_match, title_score, best = max(ranked, key=lambda item: item[0])
    text_url = plain_text_url(best)
    confident = author_match and title_score >= 0.6
    status = "matched" if confident else "review"
    if confident and text_url is None:
        status = "matched_without_plain_text"
    return {
        "status": status,
        "candidate_count": len(ranked),
        "title_score": round(title_score, 3),
        "gutendex_id": best.get("id"),
        "matched_title": best.get("title"),
        "matched_authors": [
            author.get("name")
            for author in best.get("authors", [])
            if isinstance(author, dict)
        ],
        "languages": best.get("languages", []),
        "copyright": best.get("copyright"),
        "text_url": text_url,
        "landing_page": f"https://www.gutenberg.org/ebooks/{best.get('id')}",
    }


def load_corpus(path: Path) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    document = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    metadata = document.get("corpus")
    works = document.get("works")
    if not isinstance(metadata, dict) or not isinstance(works, list) or not works:
        raise ValueError(f"{path} must contain corpus metadata and a non-empty works list")
    for number, work in enumerate(works, start=1):
        if not isinstance(work, dict) or not REQUIRED_WORK_FIELDS <= work.keys():
            raise ValueError(
                f"Corpus work {number} must contain {sorted(REQUIRED_WORK_FIELDS)}"
            )
    ids = [str(work["id"]) for work in works]
    if len(ids) != len(set(ids)):
        raise ValueError(f"{path} contains duplicate work IDs")
    return metadata, works


def _report(metadata: dict[str, Any], findings: list[dict[str, Any]]) -> dict[str, Any]:
    counts = {
        status: sum(finding["status"] == status for finding in findings)
        for status in sorted({finding["status"] for finding in findings})
    }
    id_counts = Counter(
        finding.get("gutendex_id")
        for finding in findings
        if finding.get("gutendex_id") is not None
    )
    return {
        "corpus": metadata,
        "summary": counts,
        "diagnostics": {
            "duplicate_gutendex_ids": sorted(
                gutenberg_id for gutenberg_id, count in id_counts.items() if count > 1
            )
        },
        "works": findings,
    }


def write_report(report: dict[str, Any], path: Path) -> None:
    """Atomically checkpoint a serializable report after each API request."""
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".part")
    temporary.write_text(
        json.dumps(report, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    temporary.replace(path)


def test_corpus(
    manifest_path: Path,
    api_config_path: Path,
    *,
    endpoint: str = "gutendex_plato",
    delay_seconds: float = 1.0,
    limit: int | None = None,
    request_timeout: float = 10.0,
    checkpoint_path: Path | None = None,
) -> dict[str, Any]:
    """Call Gutendex for each work and return a serializable review report."""
    metadata, works = load_corpus(manifest_path)
    selected = works[:limit] if limit is not None else works
    base_request = replace(
        load_request(api_config_path, endpoint),
        timeout=request_timeout,
    )
    session = requests.Session()
    findings: list[dict[str, Any]] = []
    completed: dict[str, dict[str, Any]] = {}
    if checkpoint_path and checkpoint_path.exists():
        previous = json.loads(checkpoint_path.read_text(encoding="utf-8"))
        completed = {
            str(finding["id"]): finding
            for finding in previous.get("works", [])
            if finding.get("status") != "request_failed"
        }

    print(f"Testing corpus {metadata['id']}: {len(selected)} of {len(works)} works")
    for number, work in enumerate(selected, start=1):
        print(f"[{number}/{len(selected)}] {work['author']} — {work['title']}")
        if str(work["id"]) in completed:
            finding = completed[str(work["id"])]
            findings.append(finding)
            print(f"  -> cached {finding['status']} | id={finding.get('gutendex_id')}")
            continue
        request = replace(
            base_request,
            name=f"corpus_{work['id']}",
            params={"search": work["query"], "languages": metadata.get("language", "en")},
        )
        try:
            response = execute_request(request, session=session)
            if not response.ok or not isinstance(response.body, dict):
                match = {"status": "request_failed", "http_status": response.status_code}
            else:
                results = response.body.get("results", [])
                match = match_work(work, results if isinstance(results, list) else [])
                match["http_status"] = response.status_code
        except OSError as error:
            match = {
                "status": "request_failed",
                "http_status": None,
                "error": str(error),
            }
        finding = {**work, **match}
        findings.append(finding)
        print(
            f"  -> {finding['status']} | id={finding.get('gutendex_id')} "
            f"| {finding.get('matched_title', 'no candidate')}"
        )
        if checkpoint_path:
            write_report(_report(metadata, findings), checkpoint_path)
        if number < len(selected) and delay_seconds:
            time.sleep(delay_seconds)

    report = _report(metadata, findings)
    print("CORPUS AVAILABILITY SUMMARY")
    for status, count in report["summary"].items():
        print(f"{status:28} {count}")
    print("A match is discovery evidence; review edition and rights before ingestion.")
    if checkpoint_path:
        write_report(report, checkpoint_path)
    return report
