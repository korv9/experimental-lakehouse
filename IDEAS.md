# Data Product Ideas: Atlas of Human Thought

## Purpose

The long-term objective is to study how the language preserved in intellectual
and cultural records changes over time. The platform must not present this as a
measurement of humanity's collective mood. It measures the surviving,
digitized and legally accessible record, with explicit coverage and uncertainty
metrics.

Working research question:

> How did the language of preserved human thought evolve from 1700 to the
> present?

The portfolio value comes from combining governed data engineering, NLP,
computational humanities and reproducible historical analysis. Every analytical
claim must remain traceable to source files, editions, transformations, model
versions and sampling decisions.

## First product: Philosophy Books MVP

Start with an English-language, public-domain corpus. Keep philosophy,
political theory and literature as separate strata rather than treating them as
one homogeneous corpus.

Initial source roles:

- Gutendex: local, human-reviewed discovery only; its public endpoint is not a
  production dependency.
- Project Gutenberg catalog feed: official cloud-ingested metadata snapshots.
- Project Gutenberg: plain-text book files.
- Open Library and Libris: bibliographic and edition metadata.
- Wikidata: authors, dates, places and philosophical-school enrichment.
- Wikisource: supplementary versioned texts where Gutenberg coverage is weak.

The first analytical period is 1700 onward, but periods with inadequate
coverage must be marked insufficient rather than filled with misleading
statistics.

### MVP questions

- Which semantic themes emerge, disappear or split over time?
- Which decades exhibit the largest semantic movement?
- Do familiar intellectual periods emerge from unsupervised clustering?
- Does semantic diversity increase within philosophy?
- Which authors and concepts bridge otherwise separate clusters?
- How sensitive are the conclusions to source, genre, language and sampling?

### Unity Catalog layout

Use one catalog per environment and the existing layer schemas:

```text
dev_lakehouse
├── landing.source_files                    # managed volume
├── platform.checkpoints                    # managed volume
├── platform.pipeline_runs                  # Delta table
├── platform.ingestion_checkpoints          # Delta table
├── platform.download_manifest              # Delta table
├── bronze.gutenberg_catalog_raw
├── bronze.philosophy_litterature_text_manifest
├── silver.gutenberg_work
├── silver.philosophy_litterature_work
├── silver.philosophy_litterature_text_version
├── silver.philosophy_litterature_text_chunk
├── gold.philosophy_litterature_dim_work
├── gold.philosophy_litterature_dim_author
├── gold.philosophy_litterature_dim_time
├── gold.philosophy_litterature_fact_chunk_embedding
├── gold.philosophy_litterature_fact_period_metrics
└── gold.philosophy_litterature_fact_semantic_shift
```

Raw files and checkpoints use governed volume paths:

```text
/Volumes/dev_lakehouse/landing/source_files/philosophy_litterature/gutenberg/...
/Volumes/dev_lakehouse/platform/checkpoints/philosophy_litterature/...
```

Do not store raw books, streaming checkpoints or model artifacts in DBFS root.
Tables are addressed by their three-part Unity Catalog name.

## Philosophy Books MVP backlog

### Phase 0 — platform prerequisites

- [x] Test source endpoints without Spark.
- [x] Add resumable HTTP downloads with atomic commits.
- [x] Calculate and validate SHA-256 checksums.
- [x] Add shared client-side rate limiting and `Retry-After` handling.
- [x] Add cursor pagination with repeated-cursor protection.
- [x] Add durable cursor and watermark checkpoints.
- [x] Add HTML extraction and conservative OCR cleanup.
- [x] Define Unity Catalog schemas, managed volumes and control tables.
- [ ] Deploy the setup notebook in a Unity Catalog-enabled Databricks workspace.
- [ ] Grant the job service principal only the required catalog/schema/volume
      permissions.

### Phase 1 — source discovery and landing

- [x] Define and version the curated philosophy author/work seed list.
- [ ] Record source terms, licenses and jurisdiction notes.
- [x] Create Bronze and Silver contracts for the official catalog metadata.
- [x] Download the official compressed catalog to a governed Volume.
- [x] Validate gzip/CSV structure and persist SHA-256 artifact manifests.
- [x] Preserve source-faithful catalog rows in Bronze.
- [x] Normalize the current catalog and select the reviewed corpus in Silver.
- [ ] Download plain-text works to the landing volume.
- [x] Record URL, ETag, byte size, SHA-256, retrieval time and run ID in
      `platform.download_manifest`.
- [x] Prove that interrupted downloads resume and snapshot replay creates no
      duplicate Bronze rows.
- [x] Add a descriptive source-specific `User-Agent`.

### Phase 2 — Silver corpus

- [ ] Resolve works, editions, translations and authors into stable IDs.
- [ ] Keep original publication, edition and translation dates separate.
- [ ] Strip Gutenberg boilerplate in a product-owned transformation.
- [ ] Extract visible text from Wikisource HTML.
- [ ] Apply conservative OCR cleanup and retain the unmodified Bronze file.
- [ ] Split text into deterministic, overlapping chunks.
- [ ] Store content hashes for text versions and chunks.
- [ ] Calculate language, token count and text-quality metrics.
- [ ] Quarantine empty, duplicate, corrupt or implausibly short texts.

### Phase 3 — embeddings and Gold

- [ ] Freeze and register one embedding model/version for the baseline.
- [ ] Create chunk embeddings and aggregate chunk → work → author → period.
- [ ] Prevent prolific authors from dominating period centroids.
- [ ] Produce decade coverage, semantic diversity, novelty and drift metrics.
- [ ] Bootstrap period metrics to produce uncertainty intervals.
- [ ] Create balanced samples by genre, source and author.
- [ ] Validate all fact-to-dimension relationships.

### Phase 4 — experiments and presentation

- [ ] Cluster decades using text-derived features only.
- [ ] Compare detected change points with historical events after clustering.
- [ ] Compare embedding drift with TF-IDF and vocabulary baselines.
- [ ] Run leave-one-author-out and leave-one-source-out sensitivity tests.
- [ ] Build a coverage-first dashboard that shows uncertainty beside results.
- [ ] Document negative and unstable findings, not only attractive charts.

### MVP completion criteria

- At least three centuries have usable, explicitly reported coverage.
- Every downloaded byte is represented by a checksum and manifest record.
- Re-running ingestion creates no duplicate files or Bronze records.
- A stopped run resumes from its committed page/cursor.
- Every Gold metric is reproducible from governed source files and versioned
  Silver tables.
- The dashboard never displays a trend without document, author, source and
  language coverage.

## Platform portability: a Microsoft Fabric backend

The README claims the architecture is transferable to other lakehouse platforms.
That claim is currently untested. Adding a Fabric backend would prove it, and
the proof is the point — the valuable statement is "the same transformations ran
on both", not "there is a fabric/ folder".

**Only add this once a product runs end to end on Databricks.** An unexercised
second backend is worth less than nothing: it undermines the parts that do work.

### What already ports unchanged

Everything above the storage boundary is plain PySpark and Python:

- the ACON graph model, loader and engine
- contracts (`schemas/base.py`, `schemas/types.py`) and the contract gate
- the quality engine and every product `quality.yaml`
- every `transform.py` — they are `DataFrame -> DataFrame` by design
- the Kimball models and the whole test suite

### What needs a Fabric implementation

| Databricks | Fabric | Notes |
| --- | --- | --- |
| `metadata/unity_catalog.py` | Workspace → Lakehouse | the real work; a sibling `metadata/fabric.py` |
| Catalog → Schema → Table | Lakehouse → Schema → Table | `uc_read` already accepts two-part names |
| Managed Volumes | OneLake `Files/` | landing and checkpoint paths |
| `databricks.yml` + Asset Bundles | Fabric CLI / `fabric-cicd` / Deployment Pipelines | orchestration only |
| Workflows | Data Pipelines or scheduled notebooks | `orchestration.yml` equivalent |

`io/writers.py` is expected to need little or no change: Fabric Lakehouse is
Delta natively, so `saveAsTable` and `DeltaTable.merge` should work. Expected is
not verified — treat it as the first thing to test, not an assumption.

### Suggested scope

1. `config/environments/fabric.yaml` beside `dev.yaml`.
2. `metadata/fabric.py` implementing the same layout contract as
   `unity_catalog.py`, so `00_create_platform` can target either.
3. Run **messy_records** end to end in Fabric — it is self-contained, needs no
   external API, and asserts fixed row counts, so success is unambiguous.
4. Record the result in the README next to the Databricks run.

### Getting capacity

- **Azure free account** — $200 credit; provision the smallest capacity (F2) and
  **pause it** when not running. Most predictable option, since consumption is
  under your control.
- **Fabric trial** — 60 days, usually requires a work or school account.
  Availability has changed repeatedly; verify current terms before planning
  around it.
- A Fabric-enabled work tenant, if policy permits personal portfolio work.

The capacity can be paused the day after the run. The evidence in the README
stays.

## Candidate future data products

### Political Speeches

Riksdagen, UK parliamentary debates and other legally reusable official
records. Analyze rhetorical change by country, party and debate type without
mixing those populations silently.

### Scientific Thought

OpenAlex, arXiv and PubMed metadata or abstracts. Study the emergence,
convergence and fragmentation of research topics.

### Literature and Humor

Public-domain literature, plays and satire. Compare humor and narrative style
across time and language, with a dedicated genre model and rights review.

### Religion and Mythology

Multiple translations and textual traditions. Translation date and edition
must be first-class dimensions because they materially affect the language.

### Historical Context

Wars, elections, inflation, population, pandemics and climate indicators.
These are interpretation and correlation products. They must not be supplied to
an unsupervised text model when claiming that the model independently
rediscovered a historical period.

### Entity and Knowledge Graph

Authors, works, schools, influences, places and institutions from Wikidata and
bibliographic sources. Use it to explain clusters after the text-only analysis,
not to leak predefined schools into discovery experiments.

## Methodological guardrails

- Analyze languages separately before using multilingual comparisons.
- Never treat translation language as original historical language.
- Compare balanced samples, not raw document counts.
- Freeze embedding models for longitudinal comparisons.
- Report missingness, OCR quality and source composition for every period.
- Treat external-event relationships as correlations unless the research
  design supports causal inference.
- Preserve source payloads and text files so every result can be rebuilt.
