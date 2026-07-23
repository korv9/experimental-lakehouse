"""Integration smoke test (placeholder).

Integration tests exercise several components together against a real Spark +
Delta session — e.g. ingest the sample response, run bronze->silver->gold, and
assert row counts and the silver.works schema. They need a Spark environment, so
this is left as a documented stub rather than a fake-passing test.
"""
import pytest


@pytest.mark.skip(reason="requires a Spark + Delta environment; see docstring")
def test_end_to_end_sample():
    # 1. land datasets/example_source/sample_response.json into bronze
    # 2. run transformations.bronze_to_silver.example_works.run(...)
    # 3. run transformations.silver_to_gold.works_by_category.run(...)
    # 4. assert silver.works has 2 rows and gold has expected category counts
    ...
