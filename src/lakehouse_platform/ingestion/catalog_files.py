"""Validation and immutable manifests for compressed catalog snapshots."""
from __future__ import annotations

import csv
import gzip
import json
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any

import yaml

from lakehouse_platform.ingestion.files import DownloadResult, download_file
from lakehouse_platform.metadata.control_tables import record_download
from lakehouse_platform.metadata.unity_catalog import UnityCatalogLayout
from lakehouse_platform.observability.progress import progress


@dataclass(frozen=True)
class GzipCsvSource:
    name: str
    url: str
    schema_version: str
    file_name: str
    landing_source: str
    landing_subpath: str
    required_columns: tuple[str, ...]
    headers: dict[str, str]
    timeout_seconds: float
    max_retries: int

    @classmethod
    def from_yaml(cls, path: str | Path) -> GzipCsvSource:
        config = yaml.safe_load(Path(path).read_text(encoding="utf-8"))
        request = config.get("request", {})
        file_config = config["file"]
        return cls(
            name=str(config["source_name"]),
            url=str(config["url"]),
            schema_version=str(config.get("schema_version", "v1")),
            file_name=str(file_config["name"]),
            landing_source=str(file_config["landing_source"]),
            landing_subpath=str(file_config["landing_subpath"]),
            required_columns=tuple(file_config["required_columns"]),
            headers={str(key): str(value) for key, value in request.get("headers", {}).items()},
            timeout_seconds=float(request.get("timeout_seconds", 120)),
            max_retries=int(request.get("max_retries", 4)),
        )


@dataclass(frozen=True)
class GzipCsvSnapshot:
    path: Path
    header: tuple[str, ...]
    source: GzipCsvSource
    snapshot_date: date
    checksum: str
    source_modified_at: str | None


def land_gzip_csv_snapshot(
    spark: Any,
    *,
    catalog: str,
    source: GzipCsvSource,
    snapshot_date: date,
    run_id: str,
) -> GzipCsvSnapshot:
    """Download, validate and register one immutable gzip CSV snapshot."""
    layout = UnityCatalogLayout(catalog)
    target = Path(
        layout.source_path(
            source.landing_source,
            source.landing_subpath,
            snapshot_date.isoformat(),
            source.file_name,
        )
    )
    result = download_file(
        source.url,
        target,
        headers=source.headers,
        timeout=source.timeout_seconds,
        max_retries=source.max_retries,
    )
    header = validate_gzip_csv(target, list(source.required_columns))
    write_artifact_manifest(
        result,
        source_name=source.name,
        source_url=source.url,
        snapshot_date=snapshot_date,
    )
    record_download(
        spark,
        catalog,
        source_name=source.name,
        source_record_id=snapshot_date.isoformat(),
        source_url=source.url,
        volume_path=str(target),
        sha256=result.sha256,
        size_bytes=result.size_bytes,
        source_etag=result.source_etag,
        status="downloaded" if result.downloaded else "reused",
        run_id=run_id,
    )
    return GzipCsvSnapshot(
        path=target,
        header=tuple(header),
        source=source,
        snapshot_date=snapshot_date,
        checksum=result.sha256,
        source_modified_at=result.source_last_modified,
    )




def validate_gzip_csv(path: str | Path, required_columns: list[str]) -> list[str]:
    """Read through gzip integrity and return the source header."""
    with gzip.open(path, "rt", encoding="utf-8-sig", newline="") as handle:
        reader = csv.reader(handle)
        header = next(reader, None)
        if not header:
            raise ValueError(f"Catalog {path} has no CSV header")
        missing = sorted(set(required_columns) - set(header))
        if missing:
            raise ValueError(f"Catalog {path} is missing required columns: {missing}")
        for _ in reader:
            pass
    progress("CATALOG", "Compressed catalog and CSV header validated", columns=len(header))
    return header


def write_artifact_manifest(
    result: DownloadResult,
    *,
    source_name: str,
    source_url: str,
    snapshot_date: date,
) -> Path:
    """Persist and verify file lineage beside an immutable snapshot."""
    manifest_path = result.path.with_name(f"{result.path.name}.manifest.json")
    document = {
        "source_name": source_name,
        "source_url": source_url,
        "source_snapshot_date": snapshot_date.isoformat(),
        "volume_path": str(result.path),
        "sha256": result.sha256,
        "size_bytes": result.size_bytes,
        "source_etag": result.source_etag,
        "source_last_modified": result.source_last_modified,
    }
    if manifest_path.exists():
        existing = json.loads(manifest_path.read_text(encoding="utf-8"))
        if existing.get("sha256") != result.sha256:
            raise ValueError(f"Manifest checksum disagrees with {result.path}")
    else:
        manifest_path.write_text(
            json.dumps(document, indent=2, ensure_ascii=False, sort_keys=True) + "\n",
            encoding="utf-8",
        )
    progress("CATALOG", "Artifact manifest verified", path=manifest_path)
    return manifest_path
