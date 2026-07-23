# Code templates

Copy-paste starting points that follow this project's conventions. These are
illustrative skeletons, not a framework — adapt names to the source/entity at
hand. All examples assume Unity Catalog three-level names and Delta storage.

## Table of contents

1. Control-table DDL (`platform.*`)
2. Pipeline-run bookkeeping helper
3. Bronze ingestion (append-only)
4. Bronze → Silver (dedup + idempotent MERGE)
5. Silver → Gold (product)
6. Data-quality check (persisted)
7. Delta Live Tables variant

---

## 1. Control-table DDL

Minimal DDL for the operational backbone. Create once per environment.

```sql
CREATE SCHEMA IF NOT EXISTS ${catalog}.platform;

CREATE TABLE IF NOT EXISTS ${catalog}.platform.source_registry (
    source_name       STRING,
    source_type       STRING,
    config            STRING,        -- serialized YAML/JSON of the source def
    registered_at     TIMESTAMP,
    enabled           BOOLEAN
);

CREATE TABLE IF NOT EXISTS ${catalog}.platform.pipeline_runs (
    run_id            STRING,
    pipeline_name     STRING,
    source_name       STRING,
    started_at        TIMESTAMP,
    completed_at      TIMESTAMP,
    status            STRING,        -- running | success | failed
    records_read      BIGINT,
    records_written   BIGINT,
    records_rejected  BIGINT,
    error_message     STRING
);

CREATE TABLE IF NOT EXISTS ${catalog}.platform.ingestion_state (
    source_name       STRING,
    watermark_column  STRING,
    watermark_value   STRING,        -- store as string; cast on read
    updated_at        TIMESTAMP
);

CREATE TABLE IF NOT EXISTS ${catalog}.platform.schema_history (
    source_name       STRING,
    schema_version    STRING,
    schema_json       STRING,
    detected_at       TIMESTAMP
);

CREATE TABLE IF NOT EXISTS ${catalog}.platform.data_quality_results (
    run_id            STRING,
    table_name        STRING,
    check_name        STRING,
    status            STRING,        -- pass | warn | fail
    metric            DOUBLE,
    threshold         DOUBLE,
    checked_at        TIMESTAMP
);

CREATE TABLE IF NOT EXISTS ${catalog}.platform.failed_records (
    run_id            STRING,
    source_name       STRING,
    source_record_id  STRING,
    raw_payload       STRING,
    error_message     STRING,
    failed_at         TIMESTAMP
);
```

---

## 2. Pipeline-run bookkeeping helper

Lives in `src/metadata/`. Every pipeline opens a run, then closes it.

```python
import uuid
from datetime import datetime, timezone
from pyspark.sql import Row, SparkSession


def _now():
    return datetime.now(timezone.utc)


def start_run(spark: SparkSession, catalog: str, pipeline_name: str, source_name: str) -> str:
    run_id = str(uuid.uuid4())
    spark.createDataFrame([Row(
        run_id=run_id, pipeline_name=pipeline_name, source_name=source_name,
        started_at=_now(), completed_at=None, status="running",
        records_read=None, records_written=None, records_rejected=None,
        error_message=None,
    )]).write.mode("append").saveAsTable(f"{catalog}.platform.pipeline_runs")
    return run_id


def finish_run(spark, catalog, run_id, *, status, read=0, written=0, rejected=0, error=None):
    spark.sql(f"""
        MERGE INTO {catalog}.platform.pipeline_runs t
        USING (SELECT '{run_id}' AS run_id) s ON t.run_id = s.run_id
        WHEN MATCHED THEN UPDATE SET
            completed_at = current_timestamp(), status = '{status}',
            records_read = {read}, records_written = {written},
            records_rejected = {rejected},
            error_message = {'NULL' if error is None else repr(error)}
    """)
```

---

## 3. Bronze ingestion (append-only)

Generic shape: fetch via the source-agnostic client, attach standard metadata,
append. No cleaning here — that is silver's job.

```python
import uuid
from datetime import datetime, timezone
from pyspark.sql import functions as F


def land_to_bronze(spark, catalog, source_cfg, records, run_id):
    """records: list of dicts already retrieved by the generic client."""
    batch_id = str(uuid.uuid4())
    ingested_at = datetime.now(timezone.utc)

    df = (
        spark.createDataFrame([{"raw_payload": r["_raw"],
                                "source_record_id": str(r.get("id"))} for r in records])
        .withColumn("source_name", F.lit(source_cfg["source_name"]))
        .withColumn("source_endpoint", F.lit(source_cfg["endpoint"]))
        .withColumn("ingested_at", F.lit(ingested_at))
        .withColumn("batch_id", F.lit(batch_id))
        .withColumn("request_parameters", F.lit(source_cfg.get("request_parameters", "")))
        .withColumn("http_status", F.lit(200))
        .withColumn("schema_version", F.lit(source_cfg.get("schema_version", "v1")))
    )

    target = f"{catalog}.bronze.{source_cfg['source_name']}_records"
    df.write.mode("append").saveAsTable(target)
    return df.count()
```

---

## 4. Bronze → Silver (dedup + idempotent MERGE)

Read incrementally, keep the latest version of each business key, MERGE. Re-runs
must not duplicate rows.

```python
from delta.tables import DeltaTable
from pyspark.sql import Window
from pyspark.sql import functions as F


def bronze_to_silver_records(spark, catalog, business_key="source_record_id"):
    bronze = f"{catalog}.bronze.example_api_records"
    silver = f"{catalog}.silver.records"

    # incremental read using the stored watermark
    wm = (spark.table(f"{catalog}.platform.ingestion_state")
          .where(F.col("source_name") == "example_api")
          .select("watermark_value").collect())
    since = wm[0]["watermark_value"] if wm else "1970-01-01"

    src = (
        spark.table(bronze)
        .where(F.col("ingested_at") > F.lit(since))
        # ---- source-specific parsing / typing / flattening goes here ----
        .withColumn("row_num", F.row_number().over(
            Window.partitionBy(business_key).orderBy(F.col("ingested_at").desc())))
        .where(F.col("row_num") == 1).drop("row_num")
    )

    if spark.catalog.tableExists(silver):
        (DeltaTable.forName(spark, silver).alias("t")
         .merge(src.alias("s"), f"t.{business_key} = s.{business_key}")
         .whenMatchedUpdateAll().whenNotMatchedInsertAll().execute())
    else:
        src.write.saveAsTable(silver)
```

---

## 5. Silver → Gold (product)

Gold is built from silver and has a named consumer. Overwrite when it's a full
rebuild; MERGE when history accumulates.

```python
from pyspark.sql import functions as F


def gold_records_by_year(spark, catalog):
    """Consumer: portfolio dashboard 'records over time' chart."""
    silver = f"{catalog}.silver.records"
    gold = f"{catalog}.gold.analytics_records_by_year"

    (spark.table(silver)
        .withColumn("year", F.year("created_at"))
        .groupBy("year").agg(F.count("*").alias("record_count"))
        .write.mode("overwrite").option("overwriteSchema", "true")
        .saveAsTable(gold))
```

---

## 6. Data-quality check (persisted)

Checks compute a metric, compare against a threshold, and write a row to
`platform.data_quality_results`. Fail-level checks raise; warn-level record and
continue.

```python
from datetime import datetime, timezone
from pyspark.sql import Row, functions as F


def check_not_null(spark, catalog, run_id, table, column, *, fail=True):
    total = spark.table(table).count()
    nulls = spark.table(table).where(F.col(column).isNull()).count()
    null_rate = (nulls / total) if total else 0.0
    status = "pass" if null_rate == 0 else ("fail" if fail else "warn")

    spark.createDataFrame([Row(
        run_id=run_id, table_name=table, check_name=f"not_null:{column}",
        status=status, metric=null_rate, threshold=0.0,
        checked_at=datetime.now(timezone.utc),
    )]).write.mode("append").saveAsTable(f"{catalog}.platform.data_quality_results")

    if status == "fail":
        raise ValueError(f"DQ fail: {column} null_rate={null_rate:.3f} in {table}")
    return status
```

---

## 7. Delta Live Tables variant

When a pipeline is declarative, DLT expresses bronze/silver/gold plus quality as
expectations. Persist a DQ summary separately for run-over-run comparison.

```python
import dlt
from pyspark.sql import functions as F


@dlt.table(name="bronze_example_api_records", comment="Raw append-only landing")
def bronze_example():
    return spark.readStream.format("cloudFiles") \
        .option("cloudFiles.format", "json") \
        .load("/Volumes/landing/example_api/")


@dlt.table(name="silver_records", comment="Cleaned, deduplicated records")
@dlt.expect_or_drop("valid_id", "source_record_id IS NOT NULL")
@dlt.expect("recent", "ingested_at > '2000-01-01'")
def silver_records():
    return dlt.read_stream("bronze_example_api_records") \
        .dropDuplicates(["source_record_id"])
```
