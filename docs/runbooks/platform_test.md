# Runbook: platform test on Databricks

Run this after deploying a platform change, before trusting any product output.

## What to run

**`databricks_test.py`** in the repository root — that is the single file.

Open the repository as a Databricks Git folder, attach `databricks_test.py` to
Unity Catalog-enabled compute and run the whole file. Or run the
`platform_smoke_test` job from `orchestration.yml`:

```bash
databricks bundle deploy -t dev
databricks bundle run platform_smoke_test -t dev
```

Widgets: `catalog` (default `dev_lakehouse`), `create_catalog`,
`setup_platform`.

## Why this product

It drives `messy_records`, whose seed file is checked into the repository, so
the test needs no external API and no prior ingestion. That matters while the
Gutenberg feed is not wired up: the platform can still be proven end to end.

## What it asserts

| Check | Expectation |
| --- | --- |
| Bronze landed | a whole multiple of 11 rows (Bronze appends per run) |
| Silver cleaned and deduplicated | exactly 8 rows |
| Quality gate | at least 2 rows in `quarantine.messy_records` |
| Run metadata | at least 2 successful rows in `platform.pipeline_runs` |
| Quality results | at least 1 row in `platform.data_quality_results` |

The seed has 11 records; one duplicate id collapses to 10, then a null id and a
null title are quarantined, leaving 8. Those numbers are deterministic, so a
regression in cleaning, deduplication, the quality gate or the contract gate
fails the test instead of quietly changing the data.

It raises on failure, so a Workflow task turns red.

## When it fails

```sql
SELECT * FROM dev_lakehouse.platform.pipeline_runs ORDER BY started_at DESC;
SELECT * FROM dev_lakehouse.platform.data_quality_results ORDER BY checked_at DESC;
SELECT * FROM dev_lakehouse.quarantine.messy_records;
SELECT * FROM dev_lakehouse.silver.records;
```

- **Silver row count wrong** — cleaning or deduplication changed. Compare with
  `python demo_local.py`, which runs the same `clean_record` logic without Spark.
- **Contract validation error** — the transformation output drifted from
  `products/messy_records/tables/silver/records/contract.py`. The message names
  the offending columns or types.
- **Nothing quarantined** — the quality gate stopped rejecting rows; check
  `products/messy_records/tables/silver/records/quality.yaml`.
- **Permission errors on setup** — rerun with `setup_platform=false` against
  objects an administrator created.
