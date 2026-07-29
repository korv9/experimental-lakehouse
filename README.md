# Example Works Lakehouse

A minimal Databricks data product built on
[adidas/lakehouse-engine](https://github.com/adidas/lakehouse-engine).

The repository contains product code only. Reading, transformations, data-quality checks,
Delta writes and merge handling are delegated directly to `lakehouse_engine.engine.load_data`.
There is no local framework or wrapper around the engine.

## Structure

```text
products/example_works/
├── bronze_example_works/notebook.py
├── silver_example_works/notebook.py
└── gold_example_works/notebook.py
datasets/example_works/works.json
tests/test_example_works.py
databricks.yml
```

Each notebook contains one ACON with the standard Lakehouse Engine sections:

- `input_specs` reads the source.
- `transform_specs` describes the Spark transformations.
- `dq_specs` validates the result before publication.
- `output_specs` writes Delta tables in Unity Catalog.

The only execution call is:

```python
from lakehouse_engine.engine import load_data

load_data(acon=ACON)
```

## Tables

| Layer | Unity Catalog table | Purpose |
| --- | --- | --- |
| Bronze | `<catalog>.bronze_example_works.works` | Source-shaped records |
| Silver | `<catalog>.silver_example_works.works` | Typed, cleaned works |
| Gold | `<catalog>.gold_example_works.category_summary` | Category metrics |

Create the three schemas before the first run. The job identity needs `USE CATALOG`,
`USE SCHEMA`, `CREATE TABLE`, `MODIFY` and `SELECT` on the relevant objects.

## Local development

Lakehouse Engine 2.1.1 requires Python 3.12. The Spark-backed test also requires JDK 17;
without Java it is skipped while the structural ACON checks still run. Install the project with
Spark, Delta and test dependencies, then run the single product test module:

```powershell
python -m pip install -e ".[dev]"
python -m ruff check .
python -m pytest -q
```

## Databricks

The bundle uses Databricks Runtime 17.3 LTS because it provides Python 3.12 and Spark 4.0,
matching Lakehouse Engine 2.1.1. Deploy and run with:

```powershell
databricks bundle deploy -t dev
databricks bundle run example_works -t dev
```

Set `EXAMPLE_WORKS_SOURCE` only when testing a different JSON source. The checked-in sample is
used by default.
