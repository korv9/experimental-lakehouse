# Messy Records

Table-first product demonstrating heterogeneous Bronze-to-Silver cleaning.

```text
pipelines/bronze_to_silver.yaml
tables/
├── bronze/records_raw/contract.py
└── silver/records/
    ├── contract.py
    ├── spark_schema.py
    ├── transform.py
    └── quality.yaml
```

The governed table contract, Spark UDF return schema, transformation and quality
rules are colocated with the Silver table they define.

## Flow

`bronze.messy_demo_records` -> cleaning UDF -> quality gate -> contract check ->
MERGE into `silver.records` (key `record_id`). Rows failing an error-level rule
are appended to `quarantine.messy_records` instead of being dropped silently.

## Landing

The seed feed ships with the repository, so the product is self-contained:
`pipelines/land_bronze.yaml` reads `datasets/messy_demo/raw_records.json` with
the `json_records` reader (one verbatim `raw_payload` per record) and appends it
to Bronze. Re-running appends another batch; Silver deduplicates on `record_id`,
so that stays safe.

That makes `messy_records_pipeline` the cheapest end-to-end smoke test of a
platform change: `land_bronze` then `bronze_to_silver`.

`demo_local.py` runs the same cleaning logic in pure Python, so the
transformation can also be exercised without Spark at all.
