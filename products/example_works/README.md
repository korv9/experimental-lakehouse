# Example Works — reference dataset, not a real data product

A fictional API response used to exercise the platform end to end. It exists to
demonstrate and test the medallion path — ingestion, contracts, quality gate,
Delta MERGE and a Kimball star — **not** to answer any real question.

Treat it as a fixture. `drug_synergy` and `philosophy_litterature` are the
actual data products; this one is the reference implementation they are modelled
on.

```text
pipelines/
├── bronze_to_silver.yaml
└── silver_to_gold.yaml

tables/
├── bronze/works_raw/
├── silver/works/
└── gold/
    ├── dim_work/
    ├── dim_author/
    ├── dim_category/
    ├── dim_date/
    └── fact_work/
```

## Why it is worth keeping

- It is the smallest complete example of every platform feature, so it is the
  fastest place to see the intended shape of a new product.
- `local/reference_pipeline.py` runs the whole flow in pure Python, without
  Spark, which makes the star schema testable in CI.
- Its Gold layer is the reference for how a fact and its dimensions should be
  split: `fact_work` holds one row per current Work plus foreign keys and
  additive measures only.

## What it is not

- Not a source anyone should build analysis on — the data is invented.
- Its Bronze table is fed by `config/sources/example_data.yaml`, which points at
  `https://example.com/data`. That endpoint does not exist, so the Spark
  ingestion path cannot run against it. Use the checked-in
  `datasets/example_source/sample_response.json` instead.

Every physical table owns its `contract.py` and `transform.py`; quality
configuration sits beside the Silver table; notebooks only start the two ACON
pipelines.
