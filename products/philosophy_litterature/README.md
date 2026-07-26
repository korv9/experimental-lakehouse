# Philosophy Books

The first data product for the Atlas of Human Thought. The MVP combines a
reviewed Philosophy corpus with the complete official Project Gutenberg catalog.

## Source roles

- Project Gutenberg `pg_catalog.csv.gz`: production metadata source.
- Gutendex: local discovery and human review only.
- Project Gutenberg files or an official mirror: future full-text source.
- Open Library, Libris and Wikidata: later bibliographic enrichment.

The candidate intent is versioned in [`corpus.yaml`](corpus.yaml). Its existing
Gutendex review evidence is stored in
[`datasets/api_samples/philosophy_corpus_report.json`](../../datasets/api_samples/philosophy_corpus_report.json).
A discovery match is not a rights clearance; edition, translation and regional
copyright still require review.

## Production metadata flow

```text
official pg_catalog.csv.gz
    -> /Volumes/<catalog>/landing/source_files/gutenberg/catalog/<date>/
    -> bronze.gutenberg_catalog_raw
    -> silver.gutenberg_work
    -> reviewed corpus join
    -> silver.philosophy_litterature_work
```

The complete catalog is preserved because the compressed file is small and can
serve future data products. Bronze retains every source row as JSON plus file
checksum, snapshot, run and Volume lineage. Silver parses the known fields,
keeps the latest snapshot per Gutenberg ID and splits multi-valued metadata
into arrays. The product table retains both canonical corpus intent and source
metadata; 54 approved corpus entries currently map to 53 unique Gutenberg IDs.

## Databricks Workflow

Run [`demo_databricks.py`](../../demo_databricks.py) once to create Unity
Catalog schemas, Volumes and control tables. Then create a Workflow containing:

1. [`01_ingest_gutenberg_catalog.py`](../../notebooks/products/philosophy_litterature/01_ingest_gutenberg_catalog.py)
2. [`02_normalize_gutenberg_catalog.py`](../../notebooks/products/philosophy_litterature/02_normalize_gutenberg_catalog.py), dependent on task 1
3. [`03_select_philosophy_corpus.py`](../../notebooks/products/philosophy_litterature/03_select_philosophy_corpus.py), dependent on task 2

Set `catalog=dev_lakehouse` on all three tasks. Task 1 optionally accepts
`snapshot_date=YYYY-MM-DD`; blank means the current UTC date.

Each module prints its source, target, row counts and control-table run ID.
Landing is atomic and resumable, gzip integrity and the CSV contract are
validated, SHA-256 is written beside the file and to
`platform.download_manifest`, and Delta MERGE makes every task safe to rerun.

## API exploration without production coupling

```text
api_tester.py
    -> config/api/humanities.yaml
    -> lakehouse_platform.tools.api_explorer
    -> lakehouse_platform.tools.gutendex_corpus
    -> corpus.yaml
    -> philosophy_corpus_report.json
```

This path exists only to evaluate candidates and formats. The generic
`lakehouse-api` CLI remains the single ad-hoc endpoint explorer. No Gutendex
reader remains in the production ingestion engine.

Product logic follows the table-first convention:

```text
tables/<layer>/<physical_table>/{contract.py, transform.py, quality.yaml}
```

The remaining text, NLP and Gold backlog is maintained in
[`IDEAS.md`](../../IDEAS.md).
