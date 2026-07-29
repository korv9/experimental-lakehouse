# Databricks notebook source
"""Clean and type Bronze works before publishing Silver."""

from os import getenv

from lakehouse_engine.engine import load_data

CATALOG = getenv("EXAMPLE_WORKS_CATALOG", "dev_lakehouse")
DQ_ROOT = getenv("EXAMPLE_WORKS_DQ_ROOT", "/tmp/example_works/dq")
BRONZE_TABLE = f"{CATALOG}.bronze_example_works.works"
SILVER_TABLE = f"{CATALOG}.silver_example_works.works"

ACON = {
    "input_specs": [
        {
            "spec_id": "bronze_works",
            "read_type": "batch",
            "data_format": "delta",
            "db_table": BRONZE_TABLE,
        }
    ],
    "transform_specs": [
        {
            "spec_id": "silver_works",
            "input_id": "bronze_works",
            "transformers": [
                {
                    "function": "with_expressions",
                    "args": {
                        "cols_and_exprs": {
                            "work_id": "trim(work_id)",
                            "title": "trim(title)",
                            "author_id": "trim(author_id)",
                            "author_name": "trim(author_name)",
                            "category": "lower(trim(category))",
                            "publication_year": "cast(publication_year as int)",
                            "language": "lower(trim(language))",
                            "updated_at": "cast(updated_at as timestamp)",
                        }
                    },
                },
                {"function": "drop_duplicate_rows", "args": {"cols": ["work_id"]}},
                {
                    "function": "column_selector",
                    "args": {
                        "cols": {
                            "work_id": "work_id",
                            "title": "title",
                            "author_id": "author_id",
                            "author_name": "author_name",
                            "category": "category",
                            "publication_year": "publication_year",
                            "language": "language",
                            "tags": "tags",
                            "updated_at": "updated_at",
                            "loaded_at": "loaded_at",
                        }
                    },
                },
            ],
        }
    ],
    "dq_specs": [
        {
            "spec_id": "silver_quality",
            "input_id": "silver_works",
            "dq_type": "validator",
            "store_backend": "file_system",
            "local_fs_root_dir": f"{DQ_ROOT}/silver",
            "unexpected_rows_pk": ["work_id"],
            "fail_on_error": True,
            "dq_functions": [
                {
                    "function": "expect_column_values_to_not_be_null",
                    "args": {"column": "work_id"},
                },
                {
                    "function": "expect_column_values_to_be_unique",
                    "args": {"column": "work_id"},
                },
                {
                    "function": "expect_column_values_to_be_between",
                    "args": {"column": "publication_year", "min_value": 0, "max_value": 2100},
                },
            ],
        }
    ],
    "output_specs": [
        {
            "spec_id": "silver_output",
            "input_id": "silver_quality",
            "write_type": "merge",
            "data_format": "delta",
            "db_table": SILVER_TABLE,
            "merge_opts": {"merge_predicate": "new.work_id = current.work_id"},
        }
    ],
}

if __name__ == "__main__":
    load_data(acon=ACON)
