# Example source dataset

A tiny, fictional dataset used to demonstrate the full pipeline end to end.
It stands in for any paginated public REST API.

## API shape

`GET https://example.com/data?page=<n>&limit=<size>` returns:

```json
{ "page": 1, "total_pages": 3, "results": [ { ...record... } ] }
```

## Record shape (one item in `results`)

| Field        | Type        | Notes                                  |
| ------------ | ----------- | -------------------------------------- |
| `id`         | string      | source identifier (→ `work_id`)        |
| `title`      | string      | work title                             |
| `author`     | object      | nested `{ id, name }` (→ `silver.persons`) |
| `category`   | string      | e.g. fiction / nonfiction              |
| `year`       | int         | publication year                       |
| `language`   | string      | ISO code                               |
| `tags`       | array       | free-form labels                       |
| `updated_at` | string (ts) | source last-modified time              |

`sample_response.json` in this folder is one page of this API, handy for reading
and for local tests.

## Where it goes

`raw record → bronze.example_data_records → silver.works (+ silver.persons)
→ gold.analytics_works_by_category → exports/portfolio/featured_works.json`
