# Example data model

Tables produced by the walkthrough pipeline, by layer.

## bronze.example_data_records (append-only)

Raw payload + technical metadata. See `src/schemas/bronze/example_data.py`.

`source_name, source_endpoint, ingested_at, batch_id, request_parameters,
http_status, source_record_id, raw_payload, schema_version`

## silver.works (entity)

Cleaned, schema-enforced, deduplicated on `work_id`.

| Column | Type | From |
|--------|------|------|
| `work_id` (key) | string | raw `id` |
| `title` | string | raw `title` |
| `category` | string | raw `category` |
| `year` | int | raw `year` |
| `language` | string | raw `language` |
| `author_id` | string | raw `author.id` |
| `author_name` | string | raw `author.name` |
| `updated_at` | timestamp | raw `updated_at` |

## silver.persons (entity)

Authors extracted from works, deduplicated on `person_id`.

| Column | Type | From |
|--------|------|------|
| `person_id` (key) | string | `author.id` |
| `name` | string | `author.name` |

## gold.analytics_works_by_category (product)

| Column | Type |
|--------|------|
| `category` | string |
| `year` | int |
| `work_count` | bigint |

Consumer: dashboard "works over time" tile → exported to
`exports/portfolio/featured_works.json`.
