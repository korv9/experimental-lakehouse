---
name: lakehouse-engineer
description: >-
  Conventions, design guidance, and code patterns for THIS repository — the
  experimental-lakehouse platform (Databricks + Delta Lake + PySpark, medallion
  bronze/silver/gold, config-driven ingestion). Use this skill whenever working
  in this repo on: adding or reviewing a data source, writing ingestion /
  transformation / data-quality code, designing or naming Unity Catalog tables
  and schemas, building Delta Live Tables / Workflows pipelines, deciding what
  belongs in bronze vs silver vs gold, control tables, or where a new file goes
  in the src/ layout. Trigger it even when the user doesn't say "lakehouse" —
  e.g. "add an API source", "clean this table", "write a quality check",
  "where should this transform live", "is this the right layer". It encodes the
  project's own README so generated code and structure stay consistent instead
  of drifting toward a generic big-data stack.
---

# Lakehouse Engineer (project skill)

This skill keeps work on `experimental-lakehouse` consistent with the platform
the README describes. The README is the source of truth for *intent*; this skill
turns that intent into concrete conventions, code patterns, and review checks so
you don't re-derive them each time.

**Scope reminder:** this is a small, single-person, experimental platform. Favor
the simplest thing that honors the design principles below. Don't reach for
Kafka, multi-orchestrator abstractions, or a feature store because a "senior data
engineer" would — reach for them only when a concrete need in this repo appears.

## The design principles (non-negotiable)

These come straight from the README and every code/architecture decision should
be checkable against them:

1. **Platform first.** Don't couple code to the first dataset. New sources reuse
   generic components; source-specific parsing stays separate from generic
   ingestion.
2. **Raw is preserved.** Bronze is append-only. It must always be possible to
   rebuild silver and gold from bronze without re-calling the source API.
3. **Metadata is data.** Pipeline runs, source config, quality results, and
   schema history live in `platform.*` control tables, not in logs or notebook
   state.
4. **Configuration over duplication.** A new source should be a YAML file in
   `config/sources/`, not a new bespoke pipeline.
5. **Experiments are isolated.** Exploratory work reads silver/gold and writes to
   `sandbox` only. It never mutates bronze or silver.
6. **Gold is a product.** Every gold table has a named consumer (a dashboard, an
   export, an analysis). No gold table without a "who uses this".
7. **Reproducibility over manual work.** Once an experiment matters, it moves
   from a notebook into a version-controlled pipeline.

When a request would violate one of these, say so and offer the compliant
version rather than silently doing the convenient thing.

## Platform assumptions

Databricks-native: **Unity Catalog** (three-level `catalog.schema.table`),
**Delta Lake** storage, **PySpark** transforms, **Auto Loader** for file/landing
ingestion, and **Delta Live Tables (Lakeflow Declarative Pipelines)** or
**Databricks Workflows/Jobs** for orchestration. These are defaults, not
dogma — if the user is running plain open-source Spark, the medallion patterns
still apply; only the Databricks-specific glue changes.

## Naming & catalog conventions (project defaults)

The README lists naming conventions as a Phase-1 TODO. These are the proposed
defaults — follow them for new work and flag existing code that diverges.

**Catalog per environment**, medallion as **schemas** inside it:

```
<env>_lakehouse          # dev_lakehouse, prod_lakehouse (or experimental_lakehouse for a single env)
├── platform             # control tables (operational metadata)
├── bronze               # raw, append-only, one table per source+entity
├── silver               # cleaned, conformed domain entities + relationships
├── gold                 # analytics / ml / portfolio data products
└── sandbox              # isolated experiment outputs
```

**Table names** (all `snake_case`):

| Layer    | Pattern                          | Example                          |
| -------- | -------------------------------- | -------------------------------- |
| bronze   | `bronze.<source>_<entity>`       | `bronze.example_api_records`     |
| silver   | `silver.<entity>` (plural noun)  | `silver.works`, `silver.persons` |
| silver   | `silver.rel_<a>_<b>` (relationship) | `silver.rel_person_work`      |
| gold     | `gold.<consumer>_<product>`      | `gold.analytics_records_by_year`, `gold.portfolio_top_works` |
| platform | `platform.<name>`                | `platform.pipeline_runs`         |
| sandbox  | `sandbox.<experiment>_<thing>`   | `sandbox.clustering_embeddings`  |

**Columns:** `snake_case`; timestamps end in `_at` and are UTC; identifiers end
in `_id`; booleans start `is_`/`has_`.

**Standard bronze technical-metadata columns** (add to every bronze table):
`source_name`, `source_endpoint`, `ingested_at`, `batch_id`, `request_parameters`,
`http_status`, `source_record_id`, `raw_payload`, `schema_version`.

**Identifiers:** `run_id` and `batch_id` are UUID strings generated once per
pipeline run; `source_record_id` is the source's own id, preserved verbatim.

## Where files go (src/ layout)

Match the documented tree — put new code in the right home and don't invent
parallel structures:

| You're writing…                        | It goes in…                              |
| -------------------------------------- | ---------------------------------------- |
| Generic REST/API call, retries, paging | `src/ingestion/{clients,pagination,authentication}/` |
| The thing that runs a source end-to-end | `src/ingestion/ingestion_runner.py`     |
| Bronze→Silver cleaning/conforming      | `src/transformations/bronze_to_silver/`  |
| Silver→Gold products                   | `src/transformations/silver_to_gold/`    |
| Schema definitions per layer           | `src/schemas/{bronze,silver,gold}/`      |
| Data-quality checks                    | `src/quality/`                           |
| Control-table read/write helpers       | `src/metadata/`                          |
| Logging conventions/helpers            | `src/logging/`                           |
| Export/serving code                    | `src/exports/`                           |
| A new source definition                | `config/sources/<source>.yaml`           |
| A runnable pipeline / DLT / Job spec   | `pipelines/{ingestion,transformations,orchestration}/` |
| Exploratory analysis                   | `notebooks/experiments/` (writes to `sandbox` only) |

**Keep docs and tree in sync.** If you add a top-level directory or a new module
area, update the "Repository structure" diagram in `README.md` in the same
change — doc/tree drift is exactly what this repo was restructured to eliminate.

## Code patterns

Concise versions below; fuller copy-paste templates (ingestion runner, MERGE
upsert, DQ check, control-table DDL, DLT pipeline) are in
`references/code-templates.md` — read it when generating real code.

### Bronze (ingest, append-only)

- Never dedupe, filter, or reshape in bronze — land the raw payload plus the
  standard metadata columns and append.
- Prefer Auto Loader for files/landing zones; use the generic REST client for
  APIs. Source-specific parsing belongs to the source, not the client.
- One `run_id`/`batch_id` per execution; write a `platform.pipeline_runs` row at
  start and update it at the end (status, counts, error).

### Silver (clean, conform, idempotent)

- Read bronze **incrementally** using the watermark in
  `platform.ingestion_state` (e.g. `ingested_at` or the source's `modified_at`).
- Deduplicate with a window: `row_number()` over the business key ordered by
  `ingested_at desc`, keep row 1.
- Write with **`MERGE`** on the business key so re-runs are idempotent — never
  blind-append to silver.
- Silver tables are reusable domain entities (works, persons, …), not reports.

### Gold (products)

- Build from silver only; must be rebuildable without touching the source.
- Overwrite-by-partition or `MERGE`; pick based on whether history matters.
- Document the consumer in a table comment or the pipeline file.

### Data quality (persisted, not manual)

- Checks return structured results written to `platform.data_quality_results`
  (`run_id`, `check_name`, `status`, `metric`, `threshold`, `checked_at`).
- Distinguish **warn** vs **fail** thresholds; fail-level checks stop the
  pipeline, warn-level checks record and continue.
- On Databricks DLT, prefer `@dlt.expect_*` expectations for inline enforcement,
  and still persist a summary for run-over-run comparison.

### Control tables (`platform.*`)

The operational backbone. Standard set: `source_registry`, `pipeline_runs`,
`ingestion_state`, `schema_history`, `data_quality_results`, `failed_records`.
Read/write them through `src/metadata/` helpers, not ad-hoc SQL scattered across
transforms. DDL lives in `references/code-templates.md`.

## Architecture-review checklist

When asked to review a design or a change, walk this list and report concretely:

- [ ] **Right layer?** Raw→bronze, conformed→silver, product→gold, experiment→sandbox.
- [ ] **Bronze append-only?** No transformation or dedup happening in bronze.
- [ ] **Idempotent?** Silver/gold use MERGE or partition-overwrite; a re-run
      doesn't duplicate or corrupt.
- [ ] **Metadata written?** A `pipeline_runs` row; watermark updated in
      `ingestion_state`; failures captured in `failed_records`.
- [ ] **Config-driven?** A new source is a YAML file reusing generic components,
      not a copy-pasted pipeline.
- [ ] **Experiments isolated?** Exploratory code reads silver/gold, writes only
      to `sandbox`.
- [ ] **Gold has a consumer?** There's a named downstream use.
- [ ] **Naming?** Matches the conventions above.
- [ ] **Docs synced?** README structure/diagram updated if the layout changed.
- [ ] **Not over-built?** Complexity is justified by a real need in this repo,
      not by generic best-practice reflexes.

## When unsure

If a request is ambiguous about layer, consumer, or scope, ask one sharp
question rather than guessing — this platform's whole point is that structure is
deliberate. And when the README and this skill disagree, the README wins; update
this skill to match.
