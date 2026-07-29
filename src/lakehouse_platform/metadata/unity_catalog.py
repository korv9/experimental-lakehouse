"""Unity Catalog names and governed file locations."""
from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import PurePosixPath
from typing import Any

from lakehouse_platform.observability.progress import progress

IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def validate_identifier(value: str, kind: str = "identifier") -> str:
    if not IDENTIFIER.fullmatch(value):
        raise ValueError(f"Invalid Unity Catalog {kind}: {value!r}")
    return value


@dataclass(frozen=True)
class UnityCatalogLayout:
    catalog: str
    platform_schema: str = "platform"
    landing_schema: str = "landing"
    bronze_schema: str = "bronze"
    silver_schema: str = "silver"
    gold_schema: str = "gold"
    # The ML layer sits beside Gold, not above it: `feature` holds model inputs
    # derived from Gold, `ml` holds model outputs scored back into the lakehouse.
    feature_schema: str = "feature"
    ml_schema: str = "ml"
    quarantine_schema: str = "quarantine"
    sandbox_schema: str = "sandbox"
    source_files_volume: str = "source_files"
    checkpoints_volume: str = "checkpoints"

    def __post_init__(self) -> None:
        for name, value in vars(self).items():
            validate_identifier(value, name)

    @property
    def schemas(self) -> tuple[str, ...]:
        return (
            self.platform_schema,
            self.landing_schema,
            self.bronze_schema,
            self.silver_schema,
            self.gold_schema,
            self.feature_schema,
            self.ml_schema,
            self.quarantine_schema,
            self.sandbox_schema,
        )

    def table(self, schema: str, table: str) -> str:
        return ".".join(
            (
                validate_identifier(self.catalog, "catalog"),
                validate_identifier(schema, "schema"),
                validate_identifier(table, "table"),
            )
        )

    def volume_path(self, schema: str, volume: str, *parts: str) -> str:
        validate_identifier(schema, "schema")
        validate_identifier(volume, "volume")
        safe_parts: list[str] = []
        for part in parts:
            parsed = PurePosixPath(str(part).replace("\\", "/"))
            if parsed.is_absolute() or ".." in parsed.parts:
                raise ValueError(f"Unsafe Unity Catalog volume path segment: {part!r}")
            safe_parts.extend(segment for segment in parsed.parts if segment not in ("", "."))
        root = PurePosixPath("/Volumes", self.catalog, schema, volume)
        return str(root.joinpath(*safe_parts))

    def source_path(self, source: str, *parts: str) -> str:
        validate_identifier(source, "source")
        return self.volume_path(
            self.landing_schema,
            self.source_files_volume,
            source,
            *parts,
        )

    def checkpoint_path(self, pipeline: str, *parts: str) -> str:
        validate_identifier(pipeline, "pipeline")
        return self.volume_path(
            self.platform_schema,
            self.checkpoints_volume,
            pipeline,
            *parts,
        )


def create_unity_catalog_objects(
    spark: Any,
    layout: UnityCatalogLayout,
    *,
    create_catalog: bool = False,
) -> None:
    """Create governed schemas and managed volumes required by the platform."""
    if create_catalog:
        progress("SETUP", "Ensuring Unity Catalog catalog", catalog=layout.catalog)
        spark.sql(f"CREATE CATALOG IF NOT EXISTS {layout.catalog}")
    for schema in layout.schemas:
        progress("SETUP", "Ensuring Unity Catalog schema", schema=f"{layout.catalog}.{schema}")
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {layout.catalog}.{schema}")
    spark.sql(
        "CREATE VOLUME IF NOT EXISTS "
        f"{layout.catalog}.{layout.landing_schema}.{layout.source_files_volume}"
    )
    spark.sql(
        "CREATE VOLUME IF NOT EXISTS "
        f"{layout.catalog}.{layout.platform_schema}.{layout.checkpoints_volume}"
    )
    progress(
        "SETUP",
        "Unity Catalog volumes ready",
        landing=layout.source_path("shared"),
        checkpoints=layout.checkpoint_path("shared"),
    )
