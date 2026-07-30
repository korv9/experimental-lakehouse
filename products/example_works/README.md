# Example Works

This is the repository's only data product. Its three folders map directly to the medallion
layers, while rejected Silver records stay beside the clean Silver table.

```text
example_works/
|-- bronze_example_works/   preserve raw values and source metadata
|-- silver_example_works/   normalize, validate, deduplicate and quarantine
`-- gold_example_works/     aggregate by category and publication decade
```

The transformation steps are plain PySpark DataFrame expressions. Lakehouse Engine handles
source reads, data-quality execution and Delta/Unity Catalog output before and after those steps.
There are no product helper functions or pipeline wrappers to navigate.

Environment variables:

- `EXAMPLE_WORKS_CATALOG`: target catalog, default `dev_lakehouse`
- `EXAMPLE_WORKS_SOURCE`: JSON source URI, default checked-in sample
- `EXAMPLE_WORKS_DQ_ROOT`: Great Expectations metadata directory
- `EXAMPLE_WORKS_PREVIEW`: show layer outputs in the notebook, default `true`
