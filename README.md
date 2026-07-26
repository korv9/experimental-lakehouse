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
│       ├── quality/              # Portable quality gate and DQX adapter
│       ├── schemas/              # Base table-contract machinery
│       ├── transforms/           # Reusable technical transformations
│       ├── tools/                # API explorer and developer utilities
│       └── post_actions/         # OPTIMIZE and VACUUM
├── products/
│   ├── example_works/
│   │   ├── pipelines/            # Product ACONs
│   │   ├── tables/               # One folder per physical table
│   │   ├── experiments/          # Tested analytical experiments
│   │   ├── local/                # Dependency-free reference execution
│   │   └── serving/              # Product exports
│   ├── messy_records/
│   │   ├── pipelines/
│   │   └── tables/
│   └── philosophy_litterature/    # First Atlas of Human Thought product
├── notebooks/
│   ├── setup/                    # One-time platform setup
│   ├── ingestion/                # API ingestion entrypoint
│   └── products/                 # Thin notebooks grouped by product
├── config/
│   ├── api/                      # Reusable endpoint test definitions
│   ├── environments/             # Environment values
│   └── sources/                  # Source registry
├── datasets/                     # Small, versioned demo fixtures
├── docs/                         # ADRs, architecture, models and runbooks
├── tests/                        # Unit, integration and quality tests
├── demo_databricks.py            # Guided first run on Databricks
└── pyproject.toml                # Package, dependencies and tool configuration
```

`lakehouse_platform` is intentionally located at `src/lakehouse_platform`, which
is the standard Python src-layout. After installing the project, it is imported
as `lakehouse_platform`; `src` is not part of the import name.

The architectural rule is:

```text
lakehouse_platform = how a pipeline runs
products           = what a pipeline means
notebooks          = where a pipeline is started interactively
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
    options:
      table: ${catalog}.silver.records
      keys: [record_id]

post_actions: []
```

The checked-in implementation is
[`products/messy_records/pipelines/bronze_to_silver.yaml`](products/messy_records/pipelines/bronze_to_silver.yaml).
The typed loader rejects missing IDs, duplicate IDs, broken references and
transformations without a callable.

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

## Thin notebooks

Production logic must not depend on notebook state. A notebook is an entrypoint:

```python
from lakehouse_platform.engine import run_pipeline

result = run_pipeline(
    spark=spark,
    acon="products/messy_records/pipelines/bronze_to_silver.yaml",
    variables={"catalog": "dev_lakehouse"},
)

print(result)
```

See [`notebooks/products/messy_records/bronze_to_silver.py`](notebooks/products/messy_records/bronze_to_silver.py).
During exploration, engineers can still import and call the product transform
directly and use `display()` on intermediate DataFrames.

## Execution engine

`run_pipeline` performs these steps:

1. Parse and validate ACON.
2. Materialize configured inputs.
3. Resolve and call product transformation functions.
4. Apply product quality gates.
5. Write configured outputs.
6. Run explicit maintenance actions.
7. Return pipeline status, duration and written targets.

The public API is deliberately small:

```python
from lakehouse_platform import run_pipeline
```

Internal modules can evolve without forcing every product to change.

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
├── pipelines/
│   └── bronze_to_silver.yaml
└── tables/
    ├── silver/
    │   └── entity_name/
    │       ├── contract.py
    │       ├── transform.py
    │       ├── quality.yaml
    │       └── tests/
    └── gold/
        ├── dim_entity/
        │   ├── contract.py
        │   └── transform.py
        └── fact_event/
            ├── contract.py
            └── transform.py
```

Then:

1. Declare readers and dependencies in the ACON.
2. Implement pure `DataFrame -> DataFrame` transformations.
3. Declare error- and warning-level quality rules.
4. Configure an idempotent target strategy.
5. Add unit, contract and integration tests.
6. Add a thin job entrypoint or deploy the wheel directly.

Only add a new platform adapter when multiple products need the capability.

## First run in Databricks

Run [`demo_databricks.py`](demo_databricks.py) first. Open the repository as a
Databricks Git folder, attach the file to Unity Catalog-enabled compute and run
the whole file. It is both a Databricks notebook source and a Python entrypoint,
and it adds the repository's `src` directory to the import path for this first
run.

The demo prints every stage while it:

1. shows the Spark runtime, current identity and selected catalog;
2. creates or validates the catalog, layer schemas and managed volumes;
3. creates the five Delta control tables;
4. inventories every expected Unity Catalog object;
5. verifies a temporary Volume file with SHA-256 and removes it;
6. records and completes a real row in `platform.pipeline_runs`; and
7. optionally tests outbound API access against Gutendex.

Use the notebook widgets at the top of the run:

| Widget | Default | Purpose |
| --- | --- | --- |
| `catalog` | `dev_lakehouse` | Unity Catalog catalog used by the demo |
| `create_catalog` | `true` | Set to `false` when an administrator created it |
| `run_volume_probe` | `true` | Test governed file write/read/delete access |
| `run_api_smoke` | `true` | Test internet egress without blocking UC setup |

The setup identity needs permission to create the requested objects. In a
managed environment, ask an administrator to create and grant access to the
catalog, then run with `create_catalog=false`.

The file is safe to rerun: DDL uses `IF NOT EXISTS`, the temporary file probe is
removed, and each audit probe receives a new run ID. It deliberately does not
start any product ingestion.

After the summary is green, the Philosophy metadata job can be run from
[`notebooks/products/philosophy_litterature/ingest_metadata_to_bronze.py`](notebooks/products/philosophy_litterature/ingest_metadata_to_bronze.py).
It reads the reviewed corpus report, requests 53 unique Gutenberg IDs in
bounded batches, validates the product-owned Bronze contract and writes to
`dev_lakehouse.bronze.philosophy_litterature_work_raw`. Configure it as a
Databricks Workflow notebook task with a `catalog` parameter when scheduling.

## Unity Catalog storage model

Tabular data and operational state use three-part Unity Catalog names such as
`dev_lakehouse.bronze.philosophy_litterature_work_raw`. Non-tabular source files and
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

Run validation:

```powershell
$env:PYTHONPATH='src;.'
python -m products.example_works.local.reference_pipeline
pytest
ruff check .
python -m build
```

Tests that require a Spark or Delta runtime skip themselves when the required
runtime is unavailable.

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
- richer DQX adapter and quality metrics,
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
