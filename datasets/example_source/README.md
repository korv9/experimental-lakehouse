# Example source dataset

A tiny fictional dataset used to demonstrate the complete platform flow. It
stands in for a paginated public REST API.

## API shape

`GET https://example.com/data?page=<n>&limit=<size>` returns:

```json
{"page": 1, "total_pages": 1, "results": [{"id": "rec-001"}]}
```

Each result contains a Work ID, title, nested Author, category, year, language,
tags and source update timestamp.

## Data flow

```text
raw response
  -> bronze.example_data_records
  -> silver.works
  -> gold.fact_work
       + gold.dim_work
       + gold.dim_author
       + gold.dim_category
       + gold.dim_date
  -> experimental category aggregation
```

The local reference pipeline in `products/example_works/local/` runs directly
over `sample_response.json`, while
the Spark implementation reads equivalent Bronze rows through ACON.
