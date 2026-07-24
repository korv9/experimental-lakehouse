# lakehouse_framework

An enterprise-style framework layer, modelled on a Kimball/Databricks setup:
declarative **schema contracts** + a **process_job** orchestrator + a thin
**read** layer. This mirrors the "`TableDefinition` + notebook" pattern used in
production dimensional pipelines, applied here to the messy demo source.

It sits alongside `src/` (the lightweight tutorial style). Same ideas, more
structure and governance.

## Layout

```
lakehouse_framework/
├── read.py                     uc_read(table), read_json_records(path)
├── transform/
│   ├── hash.py                 dp_fk_hash — deterministic surrogate keys
│   └── cleaning.py             shared cleaning core (+ CLEAN_RECORD struct)
├── orchestration/
│   └── process_batch.py        process_job(spark, job_config)
└── schemas/
    ├── types.py                Bigint, String, Int, Double, Boolean, ...
    ├── base_schema.py          BaseSchema: column_names, spark_schema, validate
    ├── bronze/messy/records.py TableDefinition -> bronze.messy.records
    └── silver/messy/records.py TableDefinition -> silver.messy.records

lakehouse_notebooks/
├── bronze/messy_records.py     raw JSON -> bronze.messy.records
└── silver/messy_records.py     bronze -> silver.messy.records (clean + dedup + keys)
```

## How a job works

A notebook defines a `build_*()` transformation and a `job_config`, then calls
`process_job(spark, job_config)`. The orchestrator:

1. runs the transformation (a DataFrame),
2. **injects the `dp_*` audit columns** (`dp_ingestion_ts`, `dp_refresh_ts`) that
   transformations never produce themselves,
3. **validates** the result against the `TableDefinition` (exact columns; no
   nulls in the primary key),
4. enforces the declared column order, and
5. writes the Delta table, applying its table properties and column comments.

## Conventions (same as the fact tables)

- `sk_` surrogate key (hashed business key via `dp_fk_hash`)
- `bk_` business key (for lineage)
- `dp_` platform metadata (injected by the framework)
- bronze = raw + append-only, no keys; silver = typed + deduped + `sk_`/`bk_`.

## Running

On Databricks with the repo attached (adjust the `sys.path` in each notebook):

1. `lakehouse_notebooks/bronze/messy_records.py` → builds `bronze.messy.records`
2. `lakehouse_notebooks/silver/messy_records.py` → builds `silver.messy.records`

The schema layer is importable and testable without Spark — see
`tests/unit/test_framework_schema.py`.
