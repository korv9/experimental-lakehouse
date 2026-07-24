# ADR 0001: Medallion with ACON orchestration and governed quality

Status: accepted

## Context

The platform needs one repeatable path from raw API data to analysis-ready
products without duplicating orchestration inside notebooks or product code.

## Decision

- Use Bronze, Silver and Gold as storage responsibilities.
- Use imperative ingestion for paginated APIs and preserve source payloads in
  append-oriented Bronze tables.
- Use ACON as the single pipeline graph for inputs, transformations, quality,
  outputs and post-actions.
- Keep product transformations as testable `DataFrame -> DataFrame` functions.
- Use error-level quality rules to route invalid rows to product quarantine
  tables. Keep DQX behind the platform quality boundary where appropriate.
- Use idempotent Delta MERGE for mutable Silver entities.
- Model analytical Gold products as explicit Kimball facts and dimensions.

## Consequences

- Thin notebooks start ACON pipelines but contain no transformation logic.
- Product folders own domain schemas, rules and transformations.
- `src/lakehouse_platform` owns reusable execution mechanics.
- There is one canonical orchestration path rather than parallel ACON and DLT
  implementations.
