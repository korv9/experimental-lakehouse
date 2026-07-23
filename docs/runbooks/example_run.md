# Runbook: run the example pipeline

Prerequisites: a Databricks workspace with Unity Catalog, this repo attached, and
`pip install -r requirements.txt` on the cluster (for `requests`, `pyyaml`, DQX).

## One-time setup

Run `notebooks/setup/00_create_platform.py` — creates the catalog, the
`platform/bronze/silver/gold/sandbox` schemas, and the control tables.

## Each run

1. **Ingest** — `notebooks/ingestion/01_run_ingestion.py`
   Pulls `example.com/data` into `bronze.example_data_records`, records a row in
   `platform.pipeline_runs`, advances the watermark.
2. **Transform** — either:
   - imperative: `notebooks/transformations/02_run_transforms.py`, or
   - declarative: deploy `pipelines/transformations/example_medallion_dlt.py` as
     a DLT pipeline.
3. **Export** — the transform notebook writes
   `exports/portfolio/featured_works.json`.

Or wire all three as a Job with `pipelines/orchestration/example_workflow.yaml`.

## Checks

- `SELECT * FROM dev_lakehouse.platform.pipeline_runs ORDER BY started_at DESC`
- `SELECT * FROM dev_lakehouse.platform.data_quality_results ORDER BY checked_at DESC`
- Re-run any step: bronze appends, silver/gold MERGE/overwrite → no duplicates.

## Troubleshooting

- Empty silver? Check the watermark in `platform.ingestion_state` — a stale
  watermark skips already-seen rows. Reset it to reprocess.
- High `quarantine_rate` in DQX results → inspect the quarantined rows before
  loosening a rule.
