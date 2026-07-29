# Example Works data model

**Reference dataset, not a real data product.** The values are invented; the
model exists to show how a fact and its dimensions should be split.

## Bronze

`bronze.example_data_records` preserves each raw API record plus source,
batch, request and ingestion metadata.

## Silver

`silver.works` is typed and deduplicated on `work_id`. It contains Work,
Author, Category, tags and the source update timestamp. Invalid error-level
records are written to `quarantine.example_works`.

## Gold Kimball model

### fact_work

Grain: one row per current Work.

| Column | Role |
|---|---|
| `work_key` | FK to `dim_work` |
| `author_key` | FK to `dim_author` |
| `category_key` | FK to `dim_category` |
| `date_key` | FK to `dim_date` |
| `work_count` | Additive count measure, always 1 |
| `tag_count` | Additive number of tags |

### Dimensions

- `dim_work`: business ID, title, language and publication year.
- `dim_author`: Author business ID and name.
- `dim_category`: normalized category name.
- `dim_date`: full date, year, quarter, month and day.

The fact deliberately stores surrogate keys and measures only. Descriptive
attributes stay in dimensions, so experiments can slice the same measures by
different business perspectives.
