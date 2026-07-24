# Libris source (KB — Kungliga biblioteket)

The first *real* source: bibliographic data from Sweden's national library
catalogue, via the Libris XL `/find` API as JSON-LD (KBV/BIBFRAME).

- **Config:** `config/sources/libris_find.yaml`
- **Parser:** `src/transformations/libris_parse.py` (pure Python)
- **Transform:** `src/transformations/bronze_to_silver/libris_works.py` (Spark UDF)
- **Silver:** `silver.libris_works`
- **Sample:** `sample_find_response.json` (representative format — see the note in
  the file; not a live capture, because the sandbox blocks `libris.kb.se`)

## Field mapping (JSON-LD -> silver.libris_works)

| Libris (JSON-LD) | silver column |
| ---------------- | ------------- |
| `meta.@id` | `record_id` |
| `hasTitle[].mainTitle` | `title` |
| `instanceOf.contribution[].agent` (givenName + familyName) | `creators` (array) |
| `instanceOf.subject[].prefLabel` | `subjects` (array) |
| `publication[].year` | `year` |
| `instanceOf.language[].@id` (last segment) | `language` (e.g. `swe`) |
| `identifiedBy[] @type=ISBN` | `isbn` |
| `publication[].agent.label` | `publisher` |
| `meta.modified` | `updated_at` (+ incremental watermark) |

## Notes

- Read access is open (no API key). **Nationalbibliografin + Swedish authorities
  are CC0.** Verify licensing if you pull beyond `nb`.
- Names are already split into `givenName`/`familyName`, so the parser *joins*
  them (opposite of the messy demo, which splits `"Last, First"`).
- Data spans two levels: the `Instance` (edition) and `instanceOf` (the Work).
- To run for real, allow `libris.kb.se` in the environment network policy, then:
  `ingest(spark, "config/sources/libris_find.yaml")`.
