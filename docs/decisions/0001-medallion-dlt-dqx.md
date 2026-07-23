# ADR 0001: Medallion with DLT for transforms and DQX for quality

Status: accepted (example / illustrative)

## Context

We need a repeatable path from raw API data to analysis-ready products, with
data quality built into the pipeline rather than checked manually at the end.

## Decision

- **Medallion** (bronze/silver/gold) as the core structure.
- **Imperative ingestion** for APIs (paginated REST doesn't fit DLT), landing
  raw payloads in append-only bronze.
- **DLT (Lakeflow Declarative Pipelines)** for the bronze→silver→gold transform,
  where its managed incremental processing and inline expectations are the most
  efficient option.
- **DQX (Databricks Labs)** for reusable, config-driven quality rules with
  quarantine and persisted results; DLT's native expectations are used for
  lightweight inline gates.

## Consequences

- Two quality mechanisms coexist; that's intentional (lightweight inline vs
  rich reusable). Keep rules in `config/quality/` to avoid duplication.
- Ingestion and transform are decoupled, so a new source reuses the ingestion
  engine and only adds a config file + a source-specific parse step.
