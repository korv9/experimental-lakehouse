# Example Works

Table-first reference product built from the checked-in fictional API response.

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

Every physical table owns its `contract.py` and `transform.py`. Table-specific
quality configuration sits beside the Silver table. Notebooks only start the
two ACON pipelines.

`fact_work` has one row per current Work and joins to the four dimensions.
