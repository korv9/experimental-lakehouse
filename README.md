# Experimental Lakehouse Platform

A configuration-driven data platform for building independently owned data
products on Spark, Delta Lake and Databricks.

The project deliberately treats datasets as clients of a reusable platform.
Readers, writers, quality execution, orchestration and operational conventions
live in `lakehouse_platform`; domain transformations, contracts and pipeline
configuration live in `products`.

## Vision

Most data projects grow from one notebook into a collection of tightly coupled
jobs. This repository starts with the platform boundary instead:

```text
                    ACON
                      |
Sources -> Readers -> Engine -> Transform -> Quality -> Writers -> Maintenance
                         |           |
                         |           +-- product-owned Python
                         +-- run metadata and control
```

The goal is to scale across four dimensions:

- **Data volume:** distributed Spark transformations and Delta tables.
- **Sources:** replaceable readers, authentication and pagination strategies.
- **Data products:** configuration-first onboarding with isolated domain code.
- **Teams:** contracts, tests, versioned packages, CI and documented ownership.

This is intentionally an extensible portfolio platform. Scalability claims must
be backed by runnable products, tests and operational behavior rather than by
unused abstractions.

## Repository structure

```text
.
├── src/
│   └── lakehouse_platform/       # Installable, domain-agnostic platform package
│       ├── engine.py             # Public run_pipeline facade
│       ├── core/                 # ACON model, validation and imports
│       ├── ingestion/            # REST, downloads, rate limits and pagination
│       ├── io/                   # Readers and Delta writers
│       ├── metadata/             # Unity Catalog layout, state and watermarks
│       ├── observability/        # Progress events and logging
│       ├── quality/              # The single rule-driven quality gate
│       ├── schemas/              # Base table-contract machinery
│       ├── transforms/           # Reusable technical transformations
│       ├── tools/                # API explorer and developer utilities
│       └── post_actions/         # OPTIMIZE and VACUUM
├── products/
│   ├── example_works/            # Reference dataset (fixture), not a real product
│   │   ├── pipelines/            # Product ACONs
│   │   ├── tables/               # One folder per physical table
│   │   ├── experiments/          # Tested analytical experiments
│   │   ├── local/                # Dependency-free reference execution
│   │   └── serving/              # Product exports
│   ├── messy_records/
│   │   ├── pipelines/
│   │   └── tables/
│   └── philosophy_litterature/    # Contracts + runnable product notebooks
├── notebooks/
│   ├── setup/                    # One-time platform setup
│   ├── ingestion/                # API ingestion entrypoint
│   └── products/                 # Existing ACON/demo entrypoints
├── config/
│   ├── api/                      # Reusable endpoint test definitions
│   ├── environments/             # Environment values
│   └── sources/                  # Source registry
├── datasets/                     # Small, versioned demo fixtures
├── docs/                         # ADRs, architecture, models and runbooks
├── tests/                        # Unit, integration and quality tests
├── databricks_test.py            # End-to-end platform test on Databricks
└── pyproject.toml                # Package, dependencies and tool configuration
```

`lakehouse_platform` is intentionally located at `src/lakehouse_platform`, which
is the standard Python src-layout. After installing the project, it is imported
as `lakehouse_platform`; `src` is not part of the import name.

The architectural rule is:

```text
lakehouse_platform = how a pipeline runs
products           = contracts plus complete runnable product notebooks
notebooks          = shared setup and existing ACON/demo entrypoints
```

The research-product backlog and candidate future products are maintained in
[`IDEAS.md`](IDEAS.md). The first planned product is `philosophy_litterature`.

## ACON

ACON means **Algorithm Configuration**. It is the contract between a data
product and the execution engine. ACON describes the graph while ordinary
Python implements transformations that are too expressive for configuration.

```text
inputs
  └── transformations
        └── quality
              └── outputs
                    └── post_actions
```

Every intermediate result has an `id`. Downstream stages reference it through
`input_id`, making dependencies explicit and validating them before Spark work
starts.

Example:

```yaml
pipeline:
  id: messy_records_bronze_to_silver
  owner: data-platform
  version: 1

inputs:
  - id: bronze_records
    reader: unity_catalog_table
    options:
      table: ${catalog}.bronze.messy_demo_records

transformations:
  - id: cleaned_records
    input_id: bronze_records
    callable: products.messy_records.tables.silver.records.transform:transform

quality:
  - id: validated_records
    input_id: cleaned_records
    rules: ../tables/silver/records/quality.yaml
    on_failure: quarantine
    quarantine_table: ${catalog}.quarantine.messy_records

outputs:
  - id: silver_records
    input_id: validated_records
    writer: delta_merge
    contract: products.messy_records.tables.silver.records.contract:TableDefinition
    options:
      table: ${catalog}.silver.records
      keys: [record_id]

post_actions: []
```

The checked-in implementation is
[`products/messy_records/pipelines/bronze_to_silver.yaml`](products/messy_records/pipelines/bronze_to_silver.yaml).
The typed loader rejects missing IDs, duplicate IDs, broken references and
transformations without a callable.

`contract:` is optional per output. When present the engine validates the frame
against that `TableDefinition` — exact columns and types, no nulls in required
columns, unique primary key — and refuses to write when the result has drifted.
It is the declarative equivalent of the contract check `process_job` performs in
its imperative form, so both paths publish under the same guarantee.

## Transformation model

Product transformations do not read or write tables. They receive a DataFrame
and return a DataFrame:

```python
from pyspark.sql import DataFrame


def transform(bronze: DataFrame, options: dict | None = None) -> DataFrame:
    cleaned = ...
    return cleaned
```

This boundary makes the same transformation usable from:

- the ACON engine,
- a Databricks notebook,
- a local Spark integration test,
- a packaged Python/Databricks job.

Source names, mappings and domain rules belong to the product. Generic hashing,
I/O, orchestration and quality execution belong to the platform.

## Product notebooks: two calls, one engine

Every notebook uses the same two entry points, both from
`lakehouse_platform.jobs`. Reads go through `read_table`, writes through
`process_job`. Notebooks never call the engine, a reader or a writer directly.

Declarative products pass an ACON to `process_job`, which owns the whole graph:

```python
from lakehouse_platform.jobs import process_job

run_id = process_job(
    spark,
    acon="products/messy_records/pipelines/bronze_to_silver.yaml",
    catalog="dev_lakehouse",
)
```

Self-contained products build a DataFrame first, then hand it to the same call:

```python
from lakehouse_platform.jobs import process_job, read_table

df_bronze = read_table(spark, "bronze.gutenberg_catalog_raw", catalog=catalog)
df_silver = build_silver_gutenberg(df_bronze)

run_id = process_job(spark, job_config, catalog=catalog, dataframe=df_silver)
```

`read_table` resolves ACON variables (`${catalog}`) and dispatches into the ACON
reader registry, so an imperative read and an ACON `inputs` entry take the same
code path. Both `process_job` forms record a `platform.pipeline_runs` row.

See [`products/philosophy_litterature/notebooks/`](products/philosophy_litterature/notebooks/)
for the self-contained pattern and
[`notebooks/products/messy_records/bronze_to_silver.py`](notebooks/products/messy_records/bronze_to_silver.py)
for the ACON pattern.

## Execution engine

`process_job` opens a pipeline run, delegates to `run_pipeline`, and closes the
run with its status. `run_pipeline` performs these steps:

1. Parse and validate ACON.
2. Materialize configured inputs.
3. Resolve and call product transformation functions.
4. Apply product quality gates.
5. Write configured outputs.
6. Run explicit maintenance actions.
7. Return pipeline status, duration and written targets.

The notebook-facing API is deliberately small:

```python
from lakehouse_platform.jobs import process_job, read_table
```

`run_pipeline` remains importable for tooling, but products should go through
`process_job` so every execution is logged. Internal modules can evolve without
forcing every product to change.

## Data layers

- **Bronze:** append-oriented raw source payload plus ingestion metadata.
- **Silver:** typed, validated, deduplicated and reusable domain entities.
- **Gold:** deterministic analytical products built from Silver.

Bronze preserves enough information for replay. Silver owns business keys,
normalization and quality. Gold may be rebuilt without calling the source again.

## Table-first Kimball example

`example_works` demonstrates how table ownership scales without a monolithic
`gold.py`:

```text
products/example_works/tables/
├── bronze/works_raw/
│   └── contract.py
├── silver/works/
│   ├── contract.py
│   ├── transform.py
│   └── quality.yaml
└── gold/
    ├── dim_work/       # contract.py + transform.py
    ├── dim_author/     # contract.py + transform.py
    ├── dim_category/   # contract.py + transform.py
    ├── dim_date/       # contract.py + transform.py
    └── fact_work/      # contract.py + transform.py
```

`fact_work` has one row per current Work. Its contracts document the grain and
foreign keys, while every physical table owns its transformation independently.

## Add a data product

Create:

```text
products/my_product/
├── __init__.py
├── notebooks/
│   └── build_entity.py       # read_table + PySpark + checks + job_config
└── tables/
    ├── silver/
    │   └── entity_name/
    │       ├── contract.py
    │       ├── quality.yaml
    │       └── tests/
    └── gold/
        ├── dim_entity/
        │   └── contract.py
        └── fact_event/
            └── contract.py
```

Then:

1. Read Unity Catalog inputs with `read_table` near the start of the product notebook.
2. Implement the visible PySpark transformation in that notebook.
3. Add contract and product checks to its `job_config`.
4. Configure merge, overwrite or append in `job_config.target`.
5. Add unit, contract and integration tests.
6. Add the notebook as a Databricks Workflow task.

Only add a new platform adapter when multiple products need the capability.

## First run in Databricks

Run [`databricks_test.py`](databricks_test.py) first. Open the repository as a
Databricks Git folder, attach the file to Unity Catalog-enabled compute and run
the whole file. It is both a Databricks notebook source and a Python entrypoint,
and it adds the repository's `src` directory to the import path for this first
run.

It tests the platform end to end using the `messy_records` product, whose seed
file ships with the repository — so no external API has to be reachable:

1. creates or validates the catalog, layer schemas and control tables;
2. lands the seed verbatim into append-only Bronze through ACON;
3. runs Bronze -> Silver: cleaning, the quality gate and the contract gate;
4. verifies the outcome against numbers the seed implies — 11 records, 10 after
   deduplication, 2 quarantined, 8 in Silver — plus rows in
   `platform.pipeline_runs` and `platform.data_quality_results`.

It raises on failure, so a Workflow task turns red instead of quietly passing.

Use the notebook widgets at the top of the run:

| Widget | Default | Purpose |
| --- | --- | --- |
| `catalog` | `dev_lakehouse` | Unity Catalog catalog used by the test |
| `create_catalog` | `true` | Set to `false` when an administrator created it |
| `setup_platform` | `true` | Set to `false` to test against existing objects |

The setup identity needs permission to create the requested objects. In a
managed environment, ask an administrator to create and grant access to the
catalog, then run with `create_catalog=false`.

The file is safe to rerun: DDL uses `IF NOT EXISTS`, the temporary file probe is
removed, and each audit probe receives a new run ID. It deliberately does not
start any product ingestion.

After the summary is green, configure one Databricks Workflow with three
dependent notebook tasks:

1. [`bronze_gutenberg.py`](products/philosophy_litterature/notebooks/bronze_gutenberg.py)
   downloads the official `pg_catalog.csv.gz` feed atomically, validates gzip
   and CSV structure, records SHA-256 lineage and merges source-faithful rows
   into `bronze.gutenberg_catalog_raw`.
2. [`silver_gutenberg.py`](products/philosophy_litterature/notebooks/silver_gutenberg.py)
   produces one current normalized row per Gutenberg ID in
   `silver.gutenberg_work`.
3. [`silver_philosophy_corpus.py`](products/philosophy_litterature/notebooks/silver_philosophy_corpus.py)
   joins the reviewed corpus intent to official metadata and writes
   `silver.philosophy_litterature_work`.

All tasks accept `catalog` (default `dev_lakehouse`). Task 1 also accepts an
optional ISO `snapshot_date`; an empty value uses the current UTC date. The
compressed source is only about 5–6 MB, so preserving the complete catalog
makes future Gutenberg products reusable without additional source calls.

Gutendex remains available through `api_tester.py` for local discovery and
human review. It is deliberately not a scheduled production dependency because
its public Cloudflare-protected endpoint challenges Databricks compute.

## Unity Catalog storage model

Tabular data and operational state use three-part Unity Catalog names such as
`dev_lakehouse.bronze.gutenberg_catalog_raw`. Non-tabular source files and
checkpoints use governed managed volumes:

```text
/Volumes/dev_lakehouse/landing/source_files/<product>/<source>/...
/Volumes/dev_lakehouse/platform/checkpoints/<pipeline>/...
```

The setup notebook creates the layer schemas, `landing.source_files` volume,
`platform.checkpoints` volume and Delta control tables. It does not use DBFS
root. Downloaded files are written to a `.part` path, optionally resumed,
verified with SHA-256 and published with a same-directory replace only after
validation.

The complete object mapping, checkpoint rules and least-privilege SQL are in
[`docs/unity-catalog.md`](docs/unity-catalog.md).

Ingestion checkpoints are committed after their Bronze page is committed.
Bronze records use deterministic content IDs and `MERGE ... WHEN NOT MATCHED`,
so replaying the last page after a failure does not create duplicates.

## Explore API endpoints

Use the API explorer to inspect a source before implementing its production
reader. It runs without Spark and supports common HTTP methods, query
parameters, headers, JSON or raw request bodies, response previews and saving
the exact response body.

Ad-hoc request:

```powershell
lakehouse-api https://openlibrary.org/search.json `
  --param "q=data engineering" `
  --param "limit=3" `
  --header "Accept=application/json"
```

For endpoints you test repeatedly, copy
[`config/api/endpoints.example.yaml`](config/api/endpoints.example.yaml), add a
named endpoint and run:

```powershell
lakehouse-api --config config/api/endpoints.example.yaml `
  --endpoint open_library_search `
  --save datasets/api_samples/open_library_search.json
```

The repository also contains
[`config/api/humanities.yaml`](config/api/humanities.yaml) with live-tested
profiles for Gutenberg, Wikisource, Internet Archive, Open Library, Wikidata,
Libris, Library of Congress, Sveriges riksdag, PubMed, OpenAlex and arXiv. The
test results and source-selection guidance are documented in
[`docs/api-source-evaluation.md`](docs/api-source-evaluation.md).

Configuration values can reference environment variables, for example
`Authorization: Bearer ${EXAMPLE_API_TOKEN}`. Sensitive request headers are
masked in terminal output. Keep real tokens in environment variables and keep
local secret-bearing configuration out of Git.

The same module is usable from a notebook or Python:

```python
from lakehouse_platform.tools.api_explorer import ApiRequest, execute_request

response = execute_request(
    ApiRequest(
        name="works",
        url="https://openlibrary.org/search.json",
        params={"q": "data engineering", "limit": 3},
    )
)
print(response.status_code, response.body)
```

## Development

Install the project:

```bash
python -m pip install -e ".[dev]"
```

Run validation — one command, the same one CI runs:

```bash
python ci_check.py
```

It runs `ruff check .` and the full test suite, prints one summary and exits
non-zero if anything failed, so "green locally" and "green in CI" cannot drift
apart. The tests themselves stay split across `tests/` — what is unified is the
entry point, not the test files.

Tests that require a Spark or Delta runtime skip themselves when the runtime is
unavailable, which keeps the fast path fast. For the CI job that does install
pyspark, add `--require-spark` to turn those skips into a failure:

```bash
python ci_check.py --require-spark
```

The remaining checks are run separately when needed:

```bash
python -m products.example_works.local.reference_pipeline   # Spark-free star schema
python -m build                                             # packaging
```

## Current scope and roadmap

Implemented:

- typed and validated ACON loading,
- Unity Catalog, JSON and text readers,
- governed Unity Catalog landing and checkpoint volumes,
- resumable atomic file downloads with SHA-256 validation,
- rate limiting, transient retry handling and cursor pagination,
- durable cursor/watermark checkpoints and idempotent Bronze replay,
- deterministic HTML extraction and conservative OCR cleanup,
- callable product transformations,
- portable quality gates,
- Delta table output,
- explicit OPTIMIZE and VACUUM actions,
- runtime ACON variables for environment-specific catalogs,
- a complete messy-records example product,
- Example Works Bronze/Silver and Kimball Gold pipelines,
- tested fact/dimension integrity and experiment aggregation,
- a documented Philosophy Books MVP and product backlog,
- unit and Spark integration tests.

Next platform increments should be driven by the Philosophy Books MVP:

- source-specific Gutenberg boilerplate removal and text chunking,
- download-manifest integration in the Philosophy ingestion job,
- embedding/model version registry and experiment tracking,
- more quality check types and richer quality metrics,
- Databricks Asset Bundles and CI/CD,
- lineage and product ownership metadata,
- separate dev/test/prod deployment configuration.

## Design principles

- Prefer one stable public facade over notebook-specific orchestration.
- Keep configuration structural and transformation logic in Python.
- Keep product vocabulary out of platform modules.
- Make retries idempotent before adding more sources.
- Test configuration, contracts and transformations independently.
- Add abstraction only after a second real use case proves it reusable.
