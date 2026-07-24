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
