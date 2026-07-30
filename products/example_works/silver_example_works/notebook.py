# Databricks notebook source
"""Normalize, validate and deduplicate Bronze works with explicit PySpark."""

from os import getenv

from lakehouse_engine.engine import load_data
from pyspark.sql import Window
from pyspark.sql import functions as F

CATALOG = getenv("EXAMPLE_WORKS_CATALOG", "dev_lakehouse")
DQ_ROOT = getenv("EXAMPLE_WORKS_DQ_ROOT", "/tmp/example_works/dq")
PREVIEW = getenv("EXAMPLE_WORKS_PREVIEW", "true").lower() == "true"
BRONZE_TABLE = f"{CATALOG}.bronze_example_works.works"
SILVER_TABLE = f"{CATALOG}.silver_example_works.works"
REJECTED_TABLE = f"{CATALOG}.silver_example_works.rejected_works"

READ_ACON = {
    "input_specs": [
        {
            "spec_id": "bronze_table",
            "read_type": "batch",
            "data_format": "delta",
            "db_table": BRONZE_TABLE,
        }
    ],
    "output_specs": [
        {
            "spec_id": "bronze_works",
            "input_id": "bronze_table",
            "data_format": "dataframe",
        }
    ],
}

if __name__ == "__main__":
    df_bronze = load_data(acon=READ_ACON)["bronze_works"]

    work_id = F.upper(F.regexp_replace(F.trim("raw_work_id"), r"\s+", ""))
    author_id = F.upper(F.regexp_replace(F.trim("raw_author_id"), r"\s+", ""))
    title = F.regexp_replace(F.trim("raw_title"), r"\s+", " ")
    author_name = F.initcap(F.lower(F.regexp_replace(F.trim("raw_author_name"), r"\s+", " ")))

    category_key = F.regexp_replace(F.lower(F.trim("raw_category")), r"[^a-z]", "")
    category = (
        F.when(category_key == "fiction", "fiction")
        .when(category_key == "nonfiction", "nonfiction")
        .when(category_key.isin("scifi", "sciencefiction"), "science_fiction")
        .when(category_key == "poetry", "poetry")
        .when(category_key.isin("essay", "essays"), "essays")
    )

    language_key = F.regexp_replace(F.lower(F.trim("raw_language")), r"[^a-z]", "")
    language = (
        F.when(language_key.isin("en", "eng", "english", "enus"), "en")
        .when(language_key.isin("sv", "swe", "swedish"), "sv")
        .when(language_key.isin("no", "nor", "norwegian"), "no")
        .otherwise("unknown")
    )

    publication_year = F.regexp_extract("raw_publication_year", r"(\d{4})", 1).try_cast("int")
    price_text = F.regexp_replace(F.lower(F.trim("raw_price")), r"[^0-9,.-]", "")
    price = (
        F.when(F.lower(F.trim("raw_price")) == "free", F.lit(0.0))
        .otherwise(F.regexp_replace(price_text, ",", ".").try_cast("decimal(10,2)"))
        .alias("price")
    )
    rating = F.regexp_replace(F.trim("raw_rating"), ",", ".").try_cast("double")
    updated_at = F.coalesce(
        F.expr("try_to_timestamp(raw_updated_at)"),
        F.expr("try_to_timestamp(raw_updated_at, 'dd/MM/yyyy HH:mm:ss')"),
    )
    empty_tags = F.array().cast("array<string>")
    tags = F.filter(
        F.array_distinct(
            F.transform(F.coalesce(F.col("raw_tags"), empty_tags), lambda tag: F.lower(F.trim(tag)))
        ),
        lambda tag: tag.isNotNull() & (F.length(tag) > 0),
    )

    df_clean = df_bronze.select(
        "source_row_id",
        "extract_id",
        F.when(F.length(work_id) > 0, work_id).alias("work_id"),
        F.when(F.length(title) > 0, title).alias("title"),
        F.when(F.length(author_id) > 0, author_id).alias("author_id"),
        F.when(F.length(author_name) > 0, author_name).alias("author_name"),
        category.alias("category"),
        publication_year.alias("publication_year"),
        language.alias("language"),
        tags.alias("tags"),
        price,
        rating.alias("rating"),
        "raw_price",
        "raw_rating",
        F.lower(F.trim("raw_status")).alias("status"),
        updated_at.alias("updated_at"),
        "raw_payload",
        "ingested_at",
    )

    df_classified = df_clean.withColumn(
        "quality_errors",
        F.array_compact(
            F.array(
                F.when(F.col("work_id").isNull(), "missing_work_id"),
                F.when(F.col("title").isNull(), "missing_title"),
                F.when(F.col("author_id").isNull(), "missing_author_id"),
                F.when(F.col("category").isNull(), "unknown_category"),
                F.when(
                    F.col("publication_year").isNull()
                    | ~F.col("publication_year").between(1450, 2100),
                    "invalid_publication_year",
                ),
                F.when(F.col("updated_at").isNull(), "invalid_updated_at"),
                F.when(F.col("status") != "published", "not_published"),
                F.when(
                    (F.length(F.trim("raw_price")) > 0)
                    & (F.col("price").isNull() | (F.col("price") < 0)),
                    "invalid_price",
                ),
                F.when(
                    (F.length(F.trim("raw_rating")) > 0)
                    & (F.col("rating").isNull() | ~F.col("rating").between(0.0, 5.0)),
                    "invalid_rating",
                ),
            )
        ),
    )

    latest = Window.partitionBy(F.coalesce("work_id", "source_row_id")).orderBy(
        F.col("updated_at").desc_nulls_last(),
        F.col("ingested_at").desc(),
        F.col("source_row_id").desc(),
    )
    df_ranked = df_classified.withColumn("_version_rank", F.row_number().over(latest))

    df_silver = df_ranked.where(
        (F.size("quality_errors") == 0) & (F.col("_version_rank") == 1)
    ).select(
        "work_id",
        "title",
        "author_id",
        "author_name",
        "category",
        "publication_year",
        "language",
        "tags",
        "price",
        "rating",
        "updated_at",
        "source_row_id",
        "extract_id",
        "ingested_at",
    )

    df_rejected = df_ranked.where(
        (F.size("quality_errors") > 0) | (F.col("_version_rank") > 1)
    ).select(
        "source_row_id",
        "extract_id",
        "work_id",
        "raw_payload",
        F.when(
            F.col("_version_rank") > 1,
            F.array_union("quality_errors", F.array(F.lit("superseded_duplicate"))),
        )
        .otherwise(F.col("quality_errors"))
        .alias("rejection_reasons"),
        "ingested_at",
    )

    if PREVIEW:
        df_silver.show(20, truncate=False)
        df_rejected.select("work_id", "rejection_reasons").show(20, truncate=False)

    load_data(
        acon={
            "input_specs": [
                {
                    "spec_id": "silver_works",
                    "read_type": "batch",
                    "data_format": "dataframe",
                    "df_name": df_silver,
                },
                {
                    "spec_id": "rejected_works",
                    "read_type": "batch",
                    "data_format": "dataframe",
                    "df_name": df_rejected,
                },
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
                            "function": "expect_table_row_count_to_be_between",
                            "args": {"min_value": 1},
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
                },
                {
                    "spec_id": "rejected_output",
                    "input_id": "rejected_works",
                    "write_type": "overwrite",
                    "data_format": "delta",
                    "db_table": REJECTED_TABLE,
                    "options": {"overwriteSchema": "true"},
                },
            ],
        }
    )
