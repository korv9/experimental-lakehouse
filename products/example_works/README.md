# Example Works

`example_works` is the repository's only data product. It is intentionally small but exercises
the complete Lakehouse Engine data-load flow: input, transformations, DQ and Delta output.

```text
example_works/
├── bronze_example_works/   JSON -> source-shaped Delta
├── silver_example_works/   cleanup, typing and deduplication
└── gold_example_works/     business aggregation
```

All infrastructure behavior comes from `lakehouse-engine==2.1.1`. Product notebooks contain
only ACON configuration and a call to `load_data`; there are no local readers, writers, contract
classes or pipeline wrappers.

Set these environment variables when needed:

- `EXAMPLE_WORKS_CATALOG`: target catalog, default `dev_lakehouse`
- `EXAMPLE_WORKS_SOURCE`: JSON source URI, default checked-in sample
- `EXAMPLE_WORKS_DQ_ROOT`: Great Expectations metadata directory
