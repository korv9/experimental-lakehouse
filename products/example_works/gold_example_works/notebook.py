# Databricks notebook source
"""Publish a compact Gold summary by work category."""

from os import getenv

from lakehouse_engine.engine import load_data

CATALOG = getenv("EXAMPLE_WORKS_CATALOG", "dev_lakehouse")
DQ_ROOT = getenv("EXAMPLE_WORKS_DQ_ROOT", "/tmp/example_works/dq")
SILVER_TABLE = f"{CATALOG}.silver_example_works.works"
GOLD_TABLE = f"{CATALOG}.gold_example_works.category_summary"

ACON = {
    "input_specs": [
        {
            "spec_id": "silver_works",
            "read_type": "batch",
            "data_format": "delta",
            "db_table": SILVER_TABLE,
            "temp_view": "works_silver",
        }
    ],
    "transform_specs": [
        {
            "spec_id": "category_summary",
            "input_id": "silver_works",
            "transformers": [
                {
                    "function": "sql_transformation",
                    "args": {
                        "sql": """
                            SELECT
                                category,
                                COUNT(*) AS work_count,
                                COUNT(DISTINCT author_id) AS author_count,
                                SUM(size(tags)) AS tag_count,
                                MIN(publication_year) AS first_publication_year,
                                MAX(publication_year) AS latest_publication_year,
                                current_timestamp() AS refreshed_at
                            FROM works_silver
                            GROUP BY category
                        """
                    },
                }
            ],
        }
    ],
    "dq_specs": [
        {
            "spec_id": "gold_quality",
            "input_id": "category_summary",
            "dq_type": "validator",
            "store_backend": "file_system",
            "local_fs_root_dir": f"{DQ_ROOT}/gold",
            "unexpected_rows_pk": ["category"],
            "fail_on_error": True,
            "dq_functions": [
                {
                    "function": "expect_column_values_to_not_be_null",
                    "args": {"column": "category"},
                },
                {
                    "function": "expect_column_values_to_be_between",
                    "args": {"column": "work_count", "min_value": 1},
                },
            ],
        }
    ],
    "output_specs": [
        {
            "spec_id": "gold_output",
            "input_id": "gold_quality",
            "write_type": "overwrite",
            "data_format": "delta",
            "db_table": GOLD_TABLE,
            "options": {"overwriteSchema": "true"},
        }
    ],
}

if __name__ == "__main__":
    load_data(acon=ACON)
