# Humanities API source evaluation

Live evaluation date: 2026-07-25.

The reusable request definitions are in
[`config/api/humanities.yaml`](../config/api/humanities.yaml). The checks below
were sent through `lakehouse_platform.tools.api_explorer`, not through a browser
or a separate HTTP client.

## Verified sources

| Source | Profile | Result | Best platform role |
|---|---|---:|---|
| Gutendex | `gutendex_plato` | 200 JSON locally; Cloudflare challenge from Databricks | Discovery and human review only |
| Gutenberg catalog | official `pg_catalog.csv.gz` feed | 200 compressed CSV from Databricks | Production metadata ingestion |
| Project Gutenberg | `gutenberg_republic_full_text` | 200 text, 1,244,164 bytes | Public-domain full-text corpus |
| Wikisource | `wikisource_republic_rendered` | 200 JSON, 84,008 HTML characters | Versioned classical works |
| Internet Archive | `internet_archive_plato` | 200 JSON | Discover item IDs and downloadable files |
| Open Library | `open_library_plato` | 200 JSON | Book and edition metadata |
| Wikidata | `wikidata_plato` | 200 JSON | Entity resolution and enrichment |
| Libris | `libris_nietzsche` | 200 JSON-LD | Swedish bibliographic enrichment |
| Library of Congress | `library_of_congress_philosophy` | 200 JSON | Digitized collections and metadata |
| Sveriges riksdag | `riksdagen_anforanden` | 200 JSON | Discover speeches |
| Sveriges riksdag | `riksdagen_speech_full_text` | 200 JSON, 872 text characters | Political speech full text |
| PubMed | `pubmed_digital_humanities` | 200 JSON | Biomedical publication discovery |
| OpenAlex | `openalex_digital_humanities` | 200 JSON | Scholarly metadata and topic evolution |
| arXiv | `arxiv_digital_humanities` | 200 Atom XML | Preprint metadata and abstracts |

The Wikisource `prop=extracts` request returned a valid response but no full
book text for the transcluded work page. The checked-in profile therefore uses
`action=parse`; its `parse.text` field contains rendered HTML that should be
cleaned in Silver.

## Sources requiring a different access strategy

- **Europeana:** the Search API requires a free API key. A ready profile is
  included and reads `${EUROPEANA_API_KEY}` from the environment.
- **OpenAlex:** anonymous low-volume evaluation worked, but current production
  guidance is to register a key before scheduled or high-volume ingestion.
- **Open Library:** use its API for low-volume discovery and its monthly dumps
  for bulk lakehouse ingestion.
- **Sveriges riksdag:** use the API during development and compressed CSVT
  datasets for historical bulk loads containing complete speech text.
- **PubMed:** use E-utilities for incremental discovery; use official bulk data
  for large-scale text mining and respect NCBI rate guidance.
- **Reddit:** requires OAuth and is a weaker first corpus because access and
  redistribution constraints are materially more complicated.
- **The Onion and Stanford Encyclopedia of Philosophy:** neither provides a
  suitable public bulk corpus API. Do not build the first ingestion around
  scraping them without explicit permission and a rights review.
- **HathiTrust:** bibliographic and data APIs exist, but full-text access
  depends on rights status and research-access arrangements.

## Recommended first data product

Start with `philosophy_litterature`:

1. Gutendex discovers candidate works locally and provides review evidence.
2. The official Gutenberg catalog feed is the production metadata source.
3. Gutenberg plain-text files become append-only Bronze records.
4. Open Library and Libris enrich editions and identifiers in Silver.
5. Wikidata resolves authors, eras, countries and philosophical schools.
6. Gold contains `dim_author`, `dim_work`, `dim_school`, `dim_time` and
   `fact_text_chunk`.

Add `political_speeches` as the second product using Riksdagen API discovery
plus CSVT bulk backfills. That proves the platform can handle both one-file-per-
work corpora and continuously updated event-like text.
