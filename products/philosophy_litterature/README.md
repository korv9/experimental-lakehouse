# Philosophy Books

The first planned data product for the Atlas of Human Thought.

Scope for the MVP:

- English-language public-domain philosophy and political-theory works.
- Original publication year 1700 or later where reliable metadata exists.
- Gutenberg full text with Gutendex discovery.
- Open Library, Libris and Wikidata enrichment.
- Decade-level coverage, semantic diversity, novelty and drift.

The implementation remains deliberately empty until the source seed list,
licensing notes and Bronze contracts are reviewed. Product logic will follow
the repository's table-first convention:

```text
tables/<layer>/<physical_table>/{contract.py, transform.py, quality.yaml}
```

The actionable backlog and Unity Catalog object model are maintained in the
repository root [`IDEAS.md`](../../IDEAS.md).
