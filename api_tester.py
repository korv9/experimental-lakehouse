"""Small Gutendex API smoke test for local use and Databricks terminals.

Gutendex is the JSON metadata API for Project Gutenberg. This script explores
the response only; it does not write to Bronze or require Spark.
"""
from __future__ import annotations

import argparse
import sys
from collections.abc import Sequence
from dataclasses import replace
from pathlib import Path
from typing import Any

REPOSITORY_ROOT = Path(__file__).resolve().parent
SOURCE_ROOT = REPOSITORY_ROOT / "src"
if str(SOURCE_ROOT) not in sys.path:
    sys.path.insert(0, str(SOURCE_ROOT))

from lakehouse_platform.tools.api_explorer import (
    execute_request,
    load_request,
    save_response,
)
from lakehouse_platform.tools.gutendex_corpus import test_corpus

DEFAULT_CONFIG = REPOSITORY_ROOT / "config" / "api" / "humanities.yaml"
DEFAULT_ENDPOINT = "gutendex_plato"
DEFAULT_CORPUS = (
    REPOSITORY_ROOT / "products" / "philosophy_litterature" / "corpus.yaml"
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Call Gutendex and print the response shape plus sample books."
    )
    parser.add_argument(
        "--query",
        default="plato",
        help="Gutendex search text (default: plato)",
    )
    parser.add_argument(
        "--language",
        help="Optional ISO language code, for example en, sv or fr",
    )
    parser.add_argument(
        "--show",
        type=int,
        default=3,
        metavar="N",
        help="Number of sample books to print (default: 3)",
    )
    parser.add_argument(
        "--save",
        type=Path,
        help="Optional path for saving the exact JSON response",
    )
    parser.add_argument("--corpus", action="store_true", help="Test the full corpus manifest")
    parser.add_argument("--manifest", type=Path, default=DEFAULT_CORPUS)
    parser.add_argument("--delay", type=float, default=1.0)
    parser.add_argument("--limit", type=int, help="Test only the first N corpus works")
    parser.add_argument(
        "--request-timeout",
        type=float,
        default=10.0,
        help="Per-request timeout in corpus mode (default: 10 seconds)",
    )
    parser.add_argument(
        "--save-report",
        type=Path,
        help="Checkpoint and resume the corpus report as JSON",
    )
    return parser


def _repository_path(path: Path) -> Path:
    return path if path.is_absolute() else REPOSITORY_ROOT / path


def _author_names(book: dict[str, Any]) -> str:
    authors = book.get("authors") or []
    names = [str(author.get("name")) for author in authors if author.get("name")]
    return ", ".join(names) if names else "Unknown"


def describe_gutendex_payload(body: Any, *, sample_size: int = 3) -> None:
    """Validate and print the parts that matter for a future Bronze contract."""
    if not isinstance(body, dict):
        raise TypeError(f"Expected a JSON object, received {type(body).__name__}")
    results = body.get("results")
    if not isinstance(results, list):
        raise TypeError("Gutendex response does not contain a results list")

    print()
    print("=" * 76)
    print("GUTENDEX RESPONSE STRUCTURE")
    print("=" * 76)
    print(f"Top-level fields: {', '.join(sorted(body))}")
    print(f"Matching books:   {body.get('count', 'unknown')}")
    print(f"Rows this page:   {len(results)}")
    print(f"Has next page:    {bool(body.get('next'))}")
    if results and isinstance(results[0], dict):
        print(f"Book fields:      {', '.join(sorted(results[0]))}")

    print()
    print("SAMPLE BOOKS")
    for number, book in enumerate(results[: max(0, sample_size)], start=1):
        if not isinstance(book, dict):
            continue
        formats = book.get("formats") or {}
        text_url = formats.get("text/plain; charset=utf-8") or formats.get("text/plain")
        print(f"{number}. id={book.get('id')} | {book.get('title', 'Untitled')}")
        print(f"   authors={_author_names(book)}")
        print(f"   languages={book.get('languages', [])} downloads={book.get('download_count')}")
        print(f"   text_url={text_url or 'No plain-text file in this record'}")


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.show < 0:
        print("--show must be zero or greater", file=sys.stderr)
        return 2
    if (
        args.delay < 0
        or args.request_timeout <= 0
        or (args.limit is not None and args.limit < 1)
    ):
        print("delay/timeout/limit values are invalid", file=sys.stderr)
        return 2

    try:
        if args.corpus:
            report = test_corpus(
                _repository_path(args.manifest),
                DEFAULT_CONFIG,
                delay_seconds=args.delay,
                limit=args.limit,
                request_timeout=args.request_timeout,
                checkpoint_path=(
                    _repository_path(args.save_report) if args.save_report else None
                ),
            )
            if args.save_report:
                print(f"Saved corpus report to: {_repository_path(args.save_report)}")
            return 1 if report["summary"].get("request_failed", 0) else 0

        request = load_request(DEFAULT_CONFIG, DEFAULT_ENDPOINT)
        params = {**request.params, "search": args.query}
        if args.language:
            params["languages"] = args.language
        request = replace(request, name="gutendex_test", params=params)

        print("Testing the public Gutendex endpoint (no API key required).")
        response = execute_request(request)
        if not response.ok:
            print(f"Gutendex returned HTTP {response.status_code}", file=sys.stderr)
            return 1

        describe_gutendex_payload(response.body, sample_size=args.show)
        if args.save:
            target = args.save if args.save.is_absolute() else REPOSITORY_ROOT / args.save
            save_response(response, target)
            print(f"Saved raw response to: {target}")
        return 0
    except (OSError, TypeError, ValueError) as error:
        print(f"Gutendex test failed: {error}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
