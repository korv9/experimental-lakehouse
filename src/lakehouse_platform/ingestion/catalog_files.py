"""Validation and immutable manifests for compressed catalog snapshots."""
from __future__ import annotations

import csv
import gzip
import json
from datetime import date
from pathlib import Path

from lakehouse_platform.ingestion.files import DownloadResult
from lakehouse_platform.observability.progress import progress


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
