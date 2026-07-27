"""Typed representation and validation of an Algorithm Configuration (ACON)."""
from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml


class AconError(ValueError):
    """Raised when an ACON document is structurally invalid."""


@dataclass(frozen=True)
class PipelineMetadata:
    id: str
    owner: str = "data-platform"
    version: int = 1
    description: str = ""


@dataclass(frozen=True)
class Spec:
    id: str
    kind: str
    input_id: str | None = None
    callable: str | None = None
    contract: str | None = None
    options: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class QualitySpec:
    id: str
    input_id: str
    rules: str
    on_failure: str = "fail"
    quarantine_table: str | None = None


@dataclass(frozen=True)
class PostActionSpec:
    action: str
    target: str
    options: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class Acon:
    pipeline: PipelineMetadata
    inputs: tuple[Spec, ...]
    transformations: tuple[Spec, ...]
    quality: tuple[QualitySpec, ...]
    outputs: tuple[Spec, ...]
    post_actions: tuple[PostActionSpec, ...]
    source_path: Path | None = None

    @classmethod
    def from_yaml(cls, path: str | Path) -> Acon:
        source = Path(path)
        with source.open(encoding="utf-8") as handle:
            data = yaml.safe_load(handle) or {}
        return cls.from_dict(data, source_path=source)

    @classmethod
    def from_dict(cls, data: dict[str, Any], source_path: Path | None = None) -> Acon:
        pipeline_data = data.get("pipeline") or {}
        if not pipeline_data.get("id"):
            raise AconError("pipeline.id is required")

        def specs(section: str) -> tuple[Spec, ...]:
            result = []
            for raw in data.get(section, []):
                result.append(
                    Spec(
                        id=raw.get("id", ""),
                        kind=raw.get("reader") or raw.get("writer") or section.rstrip("s"),
                        input_id=raw.get("input_id"),
                        callable=raw.get("callable"),
                        contract=raw.get("contract"),
                        options=raw.get("options") or {},
                    )
                )
            return tuple(result)

        acon = cls(
            pipeline=PipelineMetadata(**pipeline_data),
            inputs=specs("inputs"),
            transformations=specs("transformations"),
            quality=tuple(QualitySpec(**item) for item in data.get("quality", [])),
            outputs=specs("outputs"),
            post_actions=tuple(PostActionSpec(**item) for item in data.get("post_actions", [])),
            source_path=source_path,
        )
        acon.validate()
        return acon

    def validate(self) -> None:
        if not self.inputs:
            raise AconError("at least one input is required")
        if not self.outputs:
            raise AconError("at least one output is required")

        ids: set[str] = set()
        for spec in self.inputs:
            if not spec.id:
                raise AconError("every input requires an id")
            if spec.id in ids:
                raise AconError(f"duplicate spec id: {spec.id}")
            if not spec.kind:
                raise AconError(f"input {spec.id} requires a reader")
            ids.add(spec.id)

        for spec in self.transformations:
            if not spec.id:
                raise AconError("every transformation requires an id")
            if spec.id in ids:
                raise AconError(f"duplicate spec id: {spec.id}")
            if not spec.input_id or spec.input_id not in ids:
                raise AconError(f"{spec.id} references unknown input_id: {spec.input_id}")
            if not spec.callable:
                raise AconError(f"transformation {spec.id} requires callable")
            ids.add(spec.id)

        for spec in self.quality:
            if spec.input_id not in ids:
                raise AconError(f"{spec.id} references unknown input_id: {spec.input_id}")
            if spec.id in ids:
                raise AconError(f"duplicate spec id: {spec.id}")
            if spec.on_failure == "quarantine" and not spec.quarantine_table:
                raise AconError(f"quality spec {spec.id} requires quarantine_table")
            ids.add(spec.id)

        for spec in self.outputs:
            if not spec.id or spec.id in ids:
                raise AconError(f"duplicate or missing output id: {spec.id}")
            if not spec.input_id or spec.input_id not in ids:
                raise AconError(f"output {spec.id} references unknown input_id: {spec.input_id}")
            if not spec.kind:
                raise AconError(f"output {spec.id} requires a writer")
            ids.add(spec.id)
