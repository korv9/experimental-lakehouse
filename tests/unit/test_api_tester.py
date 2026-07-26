from __future__ import annotations

from pathlib import Path

from api_tester import build_parser, describe_gutendex_payload
from lakehouse_platform.tools.gutendex_corpus import load_corpus, match_work


def test_parser_exposes_small_gutendex_test_controls():
    args = build_parser().parse_args(["--query", "kant", "--language", "en", "--show", "2"])

    assert args.query == "kant"
    assert args.language == "en"
    assert args.show == 2


def test_describe_payload_prints_shape_and_books(capsys):
    describe_gutendex_payload(
        {
            "count": 1,
            "next": None,
            "previous": None,
            "results": [
                {
                    "id": 1497,
                    "title": "The Republic",
                    "authors": [{"name": "Plato"}],
                    "languages": ["en"],
                    "download_count": 123,
                    "formats": {
                        "text/plain; charset=utf-8": "https://example.test/1497.txt"
                    },
                }
            ],
        },
        sample_size=1,
    )

    output = capsys.readouterr().out
    assert "count, next, previous, results" in output
    assert "authors, download_count, formats, id, languages, title" in output
    assert "The Republic" in output
    assert "https://example.test/1497.txt" in output


def test_corpus_mode_and_manifest_are_explicit():
    args = build_parser().parse_args(["--corpus", "--delay", "0.5", "--limit", "4"])
    metadata, works = load_corpus(
        Path("products/philosophy_litterature/corpus.yaml")
    )

    assert args.corpus is True
    assert args.delay == 0.5
    assert args.limit == 4
    assert metadata["id"] == "philosophy_foundations_v1"
    assert len(works) == 82
    assert len({work["id"] for work in works}) == 82


def test_match_work_returns_download_evidence_for_human_review():
    match = match_work(
        {
            "id": "plato_republic",
            "period": "ancient_greece",
            "author": "Plato",
            "title": "Republic",
            "query": "plato republic",
        },
        [
            {
                "id": 1497,
                "title": "The Republic of Plato",
                "authors": [{"name": "Plato"}],
                "languages": ["en"],
                "copyright": False,
                "formats": {
                    "text/plain; charset=utf-8": "https://example.test/1497.txt"
                },
            }
        ],
    )

    assert match["status"] == "matched"
    assert match["gutendex_id"] == 1497
    assert match["text_url"] == "https://example.test/1497.txt"
