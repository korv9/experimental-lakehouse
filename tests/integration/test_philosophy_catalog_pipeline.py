"""DataFrame-level integration test for the new file-based metadata path."""
from datetime import date, datetime, timezone

import pytest

pytest.importorskip("pyspark")

from pyspark.sql import Row, SparkSession

from products.philosophy_litterature.tables.silver.gutenberg_work.transform import (
    transform as normalize_catalog,
)
from products.philosophy_litterature.tables.silver.philosophy_litterature_work.transform import (
    SELECTION_SCHEMA,
)
from products.philosophy_litterature.tables.silver.philosophy_litterature_work.transform import (
    transform as select_corpus,
)


@pytest.fixture(scope="module")
def spark():
    return SparkSession.builder.master("local[1]").appName("philosophy-catalog-it").getOrCreate()


def test_bronze_payload_normalizes_and_joins_to_corpus_intent(spark):
    payload = (
        '{"Text#":"1656","Type":"Text","Issued":"1999-05-01",'
        '"Title":"Apology","Language":"en","Authors":"Plato",'
        '"Subjects":"Socrates; Philosophy","LoCC":"B",'
        '"Bookshelves":"Classical Antiquity"}'
    )
    bronze = spark.createDataFrame(
        [
            Row(
                raw_payload=payload,
                source_snapshot_date=date(2026, 7, 26),
                source_checksum="a" * 64,
                source_file="/Volumes/test/pg_catalog.csv.gz",
                ingested_at=datetime(2026, 7, 26, tzinfo=timezone.utc),
            )
        ]
    )
    catalog = normalize_catalog(bronze)
    selection = spark.createDataFrame(
        [
            {
                "corpus_id": "philosophy_foundations_v1",
                "corpus_work_id": "plato_apology",
                "gutenberg_id": "1656",
                "period": "ancient_greece",
                "canonical_author": "Plato",
                "canonical_title": "Apology",
                "match_status": "matched",
                "text_url": "https://www.gutenberg.org/ebooks/1656.txt.utf-8",
            }
        ],
        schema=SELECTION_SCHEMA,
    )

    row = select_corpus(catalog, selection).collect()[0]

    assert row["corpus_work_id"] == "plato_apology"
    assert row["gutenberg_id"] == "1656"
    assert row["title"] == "Apology"
    assert row["authors"] == ["Plato"]
    assert row["subjects"] == ["Socrates", "Philosophy"]
