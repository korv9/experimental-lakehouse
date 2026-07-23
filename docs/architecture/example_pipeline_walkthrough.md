# Example pipeline walkthrough

**Read this first.** It threads one tiny dataset through every layer so you can
see how the folders fit together. The code is real but barebones — enough to
read and reason about, not production-hardened.

## The flow

```
example.com/data (paginated JSON)
        │  src/ingestion/  (generic REST client + pagination + auth)
        ▼
bronze.example_data_records          raw payload + metadata, append-only
        │  src/transformations/bronze_to_silver/  (parse + enforce schema)
        │  src/quality/  (DQX rules → quarantine bad rows)
        ▼
silver.works  +  silver.persons      cleaned, deduplicated, MERGE-upserted
        │  src/transformations/silver_to_gold/
        ▼
gold.analytics_works_by_category     a data product with a named consumer
        │  src/exports/
        ▼
exports/portfolio/featured_works.json   small cached file for the portfolio site
```

Operational state is written to `platform.*` control tables throughout
(`pipeline_runs`, `ingestion_state`, `data_quality_results`).

## Read the files in this order

| # | File | What to notice |
|---|------|----------------|
| 1 | `datasets/example_source/README.md` | the fake API + record shape |
| 2 | `config/sources/example_data.yaml` | a source is **config**, not code |
| 3 | `src/ingestion/clients/rest_client.py` | generic HTTP, knows no source |
| 4 | `src/ingestion/ingestion_runner.py` | ties config → client → **bronze** (append-only, raw preserved) |
| 5 | `src/metadata/control_tables.py` | runs + watermark = **metadata is data** |
| 6 | `src/schemas/silver/works.py` | the `RAW_WORK` StructType = **schema enforcement** |
| 7 | `src/transformations/bronze_to_silver/example_works.py` | parse → dedup → DQX → **MERGE** (idempotent) |
| 8 | `src/quality/dqx_checks.py` | **DQX** rules, quarantine, persisted results |
| 9 | `src/transformations/silver_to_gold/works_by_category.py` | build a **gold** product |
| 10 | `src/exports/portfolio_export.py` | gold → JSON artifact |
| 11 | `pipelines/transformations/example_medallion_dlt.py` | the same medallion, **declarative in DLT** |
| 12 | `notebooks/experiments/03_analysis_use_cases.py` | where analysis lives (sandbox), + use-case list |

## Two ways to run the transform

- **Imperative** (`notebooks/transformations/02_run_transforms.py`): calls the
  `src/` functions step by step. Easiest to read and unit-test.
- **Declarative DLT** (`pipelines/transformations/example_medallion_dlt.py`):
  DLT manages incremental processing, table creation and ordering, and gives
  inline `@dlt.expect_*` quality gates. Most efficient for the medallion once
  the shape is stable. API ingestion stays imperative because DLT can't page a
  REST API.

## Data quality: DQX vs DLT expectations

- **DQX** (`src/quality/`) — richer, reusable, config-driven rules that split a
  DataFrame into passing rows and a quarantine, with results persisted for
  run-over-run comparison. Used in the imperative path.
- **DLT expectations** — lightweight inline gates (`expect_or_drop`, `expect`).
  Used in the DLT path. DQX can also be applied *inside* a DLT table if you want
  its richer rules there.

## What's intentionally left out

Relationship tables (`silver.rel_person_work`), real auth/pagination variants,
incremental gold, and the generic ingestion engine's production concerns
(rate-limit tuning, `failed_records` capture). Those are noted where relevant so
the example stays small.
