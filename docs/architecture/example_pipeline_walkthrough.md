# Example Works pipeline walkthrough

The checked-in API response is the reference dataset for both local execution
and the Databricks/Spark implementation.

```text
sample_response.json
        |
        v
Bronze raw records
        |
        | products/example_works/pipelines/bronze_to_silver.yaml
        v
Silver Works
        |
        | products/example_works/pipelines/silver_to_gold.yaml
        v
Gold Kimball star
  |-- dim_work
  |-- dim_author
  |-- dim_category
  |-- dim_date
  `-- fact_work
        |
        v
Experiment aggregation by category
```

## Fact grain

`fact_work` has exactly one row per current work. Its additive measures are
`work_count` (always 1) and `tag_count`. Foreign keys point to Work, Author,
Category and Date dimensions. Tests assert that every fact key resolves.

## Execution paths

Local validation:

```powershell
$env:PYTHONPATH='src;.'
python -m products.example_works.local.reference_pipeline
```

Databricks runs these thin notebooks in order:

1. `notebooks/ingestion/01_run_ingestion.py`
2. `notebooks/products/example_works/bronze_to_silver.py`
3. `notebooks/products/example_works/silver_to_gold.py`
4. `notebooks/products/example_works/experiments.py`

The notebooks only start work. ACON owns the graph, product modules own domain
logic, and `lakehouse_platform` owns execution mechanics.
