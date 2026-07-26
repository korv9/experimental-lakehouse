# Philosophy Books

The first planned data product for the Atlas of Human Thought.

Scope for the candidate corpus:

- English-language editions of philosophy and political-theory works.
- Reference works from antiquity onward, with the primary longitudinal
  analysis still beginning in 1700.
- Gutenberg full text with Gutendex discovery.
- Open Library, Libris and Wikidata enrichment.
- Decade-level coverage, semantic diversity, novelty and drift.

The candidate source list is versioned in [`corpus.yaml`](corpus.yaml). Test it
against Gutendex from the repository root:

```powershell
python api_tester.py --corpus `
  --save-report datasets/api_samples/philosophy_corpus_report.json
```

The report separates confident metadata matches, candidates needing review and
works absent from Gutendex. A match is not a rights clearance; edition,
translation and regional copyright still require review.

## API handling structure

```text
api_tester.py
    -> config/api/humanities.yaml
    -> lakehouse_platform.tools.api_explorer
    -> lakehouse_platform.tools.gutendex_corpus
    -> corpus.yaml
    -> datasets/api_samples/philosophy_corpus_report.json
```

- `api_tester.py` is the small command-line entrypoint. It contains no Spark
  ingestion logic.
- `config/api/humanities.yaml` owns endpoint URL, headers and timeout defaults.
- `api_explorer.py` is the source-agnostic HTTP client and response preview.
- `gutendex_corpus.py` contains Gutendex-specific matching, checkpoint and
  resume behavior.
- `corpus.yaml` is product-owned intent: the works we want, independent of
  whether Gutenberg contains them.
- The generated JSON report is discovery evidence. It is not a Bronze table.

Production ingestion is a separate path:

```text
notebooks/products/philosophy_litterature/ingest_metadata_to_bronze.py
    -> config/sources/philosophy_gutendex.yaml
    -> datasets/api_samples/philosophy_corpus_report.json
    -> lakehouse_platform.ingestion.runner.ingest_corpus
    -> products/.../philosophy_litterature_work_raw/contract.py
    -> dev_lakehouse.bronze.philosophy_litterature_work_raw
```

The separation is intentional: exploration discovers and reviews sources;
ingestion writes approved source records into governed, replayable storage.
The current report selects 54 approved work entries which resolve to 53 unique
Gutenberg IDs because `Walden` and `Civil Disobedience` share source ID `205`.
The runner requests those IDs in batches, validates exact API coverage, commits
each batch with Delta `MERGE`, and advances its durable checkpoint only after a
successful commit.

Run `demo_databricks.py` once per environment, then run the metadata notebook
with the `catalog` widget set to the intended Unity Catalog catalog. This job
lands metadata only; Gutenberg full-text acquisition is a separate job.

Product logic follows the repository's table-first convention:

```text
tables/<layer>/<physical_table>/{contract.py, transform.py, quality.yaml}
```

The actionable backlog and Unity Catalog object model are maintained in the
repository root [`IDEAS.md`](../../IDEAS.md).
