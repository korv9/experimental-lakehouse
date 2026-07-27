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

## Known gap: no ingestion job

`config/sources/messy_demo.yaml` declares `bronze.messy_demo_records` as the
landing table, but nothing writes it yet — the sample feed lives in
`datasets/messy_demo/raw_records.json`. Land that file into Bronze before
scheduling `messy_records_pipeline` in `orchestration.yml`; until then the
notebook is run manually against a Bronze table you populate yourself.

`demo_local.py` runs the same cleaning logic end to end in pure Python, so the
transformation can be exercised without Spark or a Bronze table.
