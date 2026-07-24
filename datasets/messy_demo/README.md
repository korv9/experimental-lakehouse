# Messy demo dataset

A deliberately **unstructured** test feed. Every record in `raw_records.json` is
the kind of raw JSON you actually get from real sources — wrong types, wrong
casing, null-ish strings, mixed formats, nested-or-flat fields, duplicates. Its
job is to prove the bronze→silver cleaning turns chaos into a predictable table.

## What each field exercises

| Raw field   | Mess it demonstrates | Cleaned to |
| ----------- | -------------------- | ---------- |
| `id`        | string / int / `" rec-004 "` / **null** / **duplicate** | `record_id` (str, deduped) |
| `title`     | whitespace, `&amp;`, unicode/emoji, `null`, a **number** | `title` (str) |
| `creator`   | `"Last, First"`, `{name}`, `"A; B"`, `["A","B"]`, null, ALLCAPS, accents | `creators` (array) |
| `summary`   | HTML tags, newlines, tabs, quotes, empty string, accents | `summary` (str) |
| `category`  | `Fiction` / `fiction` / `NON-FICTION ` / `unknown` | `category` (canonical str) |
| `labels`    | array / `"a, b, c"` CSV / `""` / null / duplicates | `labels` (unique array) |
| `year`      | int / `"1999"` / `"c. 1200"` / `"2,010"` / `MCMXCIX` / `N/A` | `year` (int) |
| `rating`    | `"4.5"` / `3` / `"4,9"` / `"N/A"` | `rating` (double) |
| `is_public` | `true` / `"yes"` / `"Y"` / `0` / `"false"` | `is_public` (bool) |
| `price`     | `"$12.99"` / `"9,99 €"` / `"1 234,56 kr"` / null | `price` (double) |
| `email`     | mixed case + spaces / invalid / blank | `email` (valid or null) |
| `url`       | scheme-less / blank / `ftp://` | `url` (https or null) |
| `location`  | `{lat,lon}` string coords / `"Stockholm"` / null / `{}` | `lat`, `lon` (double) |
| `language`  | `en` / `EN` / `english` / null / `e` | `language` (ISO-ish) |
| `updated`   | `2024-01-15` / `15/01/2024` / ISO-Z / **epoch int** / `Jan 2024` / `unknown` | `updated_at` (ISO date) |
| extra field | unexpected keys not in the schema | dropped |

## Notable rows

- **REC-001 appears twice** (rows 1 and 5) → dedup keeps the later version.
- **`id: null`** (row 9) → fails an error-level quality rule → quarantined.
- **`title: 67890`** and **`creator: "  "`** → wrong-type / empty handling.

## Try it

```bash
python demo_local.py          # pure-Python, no Spark needed — prints before/after
pytest tests/unit/test_cleaning.py            # cleaning logic
pytest tests/integration/test_messy_pipeline.py   # full Spark run (needs pyspark)
```

On Databricks, `notebooks/products/messy_records/bronze_to_silver.py` starts the
ACON in `products/messy_records/pipelines/bronze_to_silver.yaml`. The engine reads Bronze, calls the
cleaning transformation, applies quality rules, persists rejected rows and
MERGEs valid records into Silver.
