# Databricks notebook source
"""Land the example JSON as a governed Bronze Delta table."""

from os import getenv
from pathlib import Path

from lakehouse_engine.engine import load_data

CATALOG = getenv("EXAMPLE_WORKS_CATALOG", "dev_lakehouse")
DQ_ROOT = getenv("EXAMPLE_WORKS_DQ_ROOT", "/tmp/example_works/dq")
SOURCE = getenv(
    "EXAMPLE_WORKS_SOURCE",
    (Path(__file__).resolve().parents[3] / "datasets/example_works/works.json").as_uri(),
)
BRONZE_TABLE = f"{CATALOG}.bronze_example_works.works"

ACON = {
    "input_specs": [
        {
            "spec_id": "source_response",
            "read_type": "batch",
            "data_format": "json",
            "location": SOURCE,
            "options": {"multiLine": True},
        }
    ],
    "transform_specs": [
        {
            "spec_id": "bronze_works",
            "input_id": "source_response",
            "transformers": [
                {
                    "function": "explode_columns",
                    "args": {"array_cols_to_explode": ["results"]},
                },
                {
                    "function": "column_selector",
                    "args": {
                        "cols": {
                            "results.id": "work_id",
                            "results.title": "title",
                            "results.author.id": "author_id",
                            "results.author.name": "author_name",
                            "results.category": "category",
                            "results.year": "publication_year",
                            "results.language": "language",
                            "results.tags": "tags",
                            "results.updated_at": "updated_at",
                        }
                    },
                },
                {"function": "add_current_date", "args": {"output_col": "loaded_at"}},
            ],
        }
    ],
    "dq_specs": [
        {
            "spec_id": "bronze_quality",
            "input_id": "bronze_works",
            "dq_type": "validator",
            "store_backend": "file_system",
            "local_fs_root_dir": f"{DQ_ROOT}/bronze",
            "unexpected_rows_pk": ["work_id"],
            "fail_on_error": True,
            "dq_functions": [
                {
                    "function": "expect_column_values_to_not_be_null",
                    "args": {"column": "work_id"},
                },
                {
                    "function": "expect_table_row_count_to_be_between",
                    "args": {"min_value": 1},
                },
            ],
        }
    ],
    "output_specs": [
        {
            "spec_id": "bronze_output",
            "input_id": "bronze_quality",
            "write_type": "overwrite",
            "data_format": "delta",
            "db_table": BRONZE_TABLE,
            "options": {"overwriteSchema": "true"},
        }
    ],
}

if __name__ == "__main__":
    load_data(acon=ACON)
