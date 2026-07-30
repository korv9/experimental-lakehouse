# Example Works Lakehouse

A small, realistic Databricks medallion product built on
[adidas/lakehouse-engine](https://github.com/adidas/lakehouse-engine).

The notebooks keep the business logic visible in ordinary PySpark. Lakehouse Engine reads each
source into a DataFrame, then validates and writes the transformed DataFrame to Delta tables in
Unity Catalog. There is no local framework or wrapper around the engine.

## Structure

```text
products/example_works/
|-- bronze_example_works/notebook.py
|-- silver_example_works/notebook.py
`-- gold_example_works/notebook.py
datasets/example_works/works.json
tests/test_example_works.py
databricks.yml
```

Each notebook follows the same short pattern:

```python
df_source = load_data(acon=READ_ACON)["source_response"]

df_result = df_source.select(...).where(...)

load_data(acon={
    "input_specs": [{"data_format": "dataframe", "df_name": df_result, ...}],
    "dq_specs": [...],
    "output_specs": [{"data_format": "delta", "db_table": TARGET_TABLE, ...}],
})
```

## Demo flow

```text
messy JSON (16 rows)
        |
        v
Bronze raw records (16 rows)
        |
        v
Silver clean works (6 rows) ----> Silver rejected works (10 rows)
        |
        v
Gold category + decade summary (5 rows)
```

The sample deliberately includes duplicate IDs, whitespace, inconsistent casing, alternative
date and decimal formats, currency suffixes, empty keys, invalid years and ratings, and
unpublished records. Bronze preserves those values. Silver makes cleaning, normalization,
validation, rejection and latest-record deduplication explicit. Gold produces a compact summary
suitable for a dashboard.

| Layer | Unity Catalog table | Purpose |
| --- | --- | --- |
| Bronze | `<catalog>.bronze_example_works.works` | Immutable, source-shaped raw records |
| Silver | `<catalog>.silver_example_works.works` | Valid, normalized and deduplicated works |
| Silver | `<catalog>.silver_example_works.rejected_works` | Rejected rows with reasons |
| Gold | `<catalog>.gold_example_works.category_summary` | Category and decade metrics |

Create the three schemas before the first run. The job identity needs `USE CATALOG`,
`USE SCHEMA`, `CREATE TABLE`, `MODIFY` and `SELECT` on the relevant objects.

## Local development

Lakehouse Engine 2.1.1 requires Python 3.12. The Spark-backed test also requires JDK 17;
without Java it is skipped while the fast structure and data checks still run.

```powershell
python -m pip install -e ".[dev]"
python -m ruff check .
python -m pytest -q
```

## Databricks

The bundle uses Databricks Runtime 17.3 LTS, with Python 3.12 and Spark 4.0. Deploy and run with:

```powershell
databricks bundle deploy -t dev
databricks bundle run example_works -t dev
```

Set `EXAMPLE_WORKS_SOURCE` to a cloud or volume URI when testing a different JSON source. The
checked-in sample is used by default. Set `EXAMPLE_WORKS_PREVIEW=false` to suppress notebook
previews.
