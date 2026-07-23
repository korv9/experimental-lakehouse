"""Silver schema for ``silver.persons``.

Persons (authors) are a second domain entity derived from the same parsed
records. Splitting them out is what makes silver *reusable*: many works share an
author, and other sources can contribute to the same person table.

Columns: ``person_id`` (from author.id), ``name`` (from author.name).
A ``silver.rel_person_work`` relationship table could join the two; omitted here
to keep the example small.
"""
TABLE = "silver.persons"
BUSINESS_KEY = "person_id"
