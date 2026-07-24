# Runbook: Example Works

## Local reference run

```powershell
$env:PYTHONPATH='src;.'
python -m products.example_works.local.reference_pipeline
python -m pytest -q
```

This uses the checked-in response and validates Bronze, Silver, Kimball Gold
foreign keys and the experiment aggregation without requiring Spark.

## Databricks run

Install the built wheel, attach the repo and run:

1. `notebooks/setup/00_create_platform.py`
2. `notebooks/ingestion/01_run_ingestion.py`
3. `notebooks/products/example_works/bronze_to_silver.py`
4. `notebooks/products/example_works/silver_to_gold.py`
5. `notebooks/products/example_works/experiments.py`

Expected targets:

- `silver.works`
- `quarantine.example_works`
- `gold.fact_work`
- `gold.dim_work`
- `gold.dim_author`
- `gold.dim_category`
- `gold.dim_date`

Silver uses Delta MERGE keyed by `work_id`; Gold is a deterministic rebuild.

## Operational checks

```sql
SELECT * FROM dev_lakehouse.platform.pipeline_runs ORDER BY started_at DESC;
SELECT * FROM dev_lakehouse.quarantine.example_works;
SELECT COUNT(*) FROM dev_lakehouse.gold.fact_work;
```
