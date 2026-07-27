"""Public facade for executing ACON pipelines."""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from time import perf_counter

from lakehouse_platform.core.acon import Acon
from lakehouse_platform.core.imports import import_callable
from lakehouse_platform.io.readers import read_input
from lakehouse_platform.io.writers import write_output
from lakehouse_platform.observability.progress import progress
from lakehouse_platform.post_actions.runner import run_post_action
from lakehouse_platform.quality.engine import apply_quality


@dataclass(frozen=True)
class PipelineResult:
    pipeline_id: str
    status: str
    duration_seconds: float
    outputs: tuple[str, ...]


def run_pipeline(
    spark,
    acon: str | Path | dict | Acon,
    variables: dict[str, str] | None = None,
) -> PipelineResult:
    """Run one validated ACON and return execution metadata."""
    config = _load_acon(acon)
    variables = variables or {}
    frames = {}
    started = perf_counter()
    progress("ENGINE", "Pipeline started", pipeline=config.pipeline.id)

    for spec in config.inputs:
        progress("INPUT", "Reading source", id=spec.id, reader=spec.kind)
        frames[spec.id] = read_input(spark, spec.kind, resolve_values(spec.options, variables))

    for spec in config.transformations:
        progress(
            "TRANSFORM",
            "Running transformation",
            id=spec.id,
            input=spec.input_id,
            callable=spec.callable,
        )
        transform = import_callable(spec.callable)
        frames[spec.id] = transform(
            frames[spec.input_id],
            resolve_values(spec.options, variables),
        )

    for spec in config.quality:
        progress("QUALITY", "Applying quality rules", id=spec.id, rules=spec.rules)
        rules = _relative_to_acon(config, spec.rules)
        good, quarantine = apply_quality(
            frames[spec.input_id],
            rules,
            spec.on_failure,
            spark=spark,
            catalog=variables.get("catalog"),
            run_id=variables.get("run_id"),
            table=f"{config.pipeline.id}.{spec.id}",
        )
        frames[spec.id] = good
        if spec.on_failure == "quarantine" and spec.quarantine_table:
            quarantine_table = resolve_values(spec.quarantine_table, variables)
            progress("QUALITY", "Persisting quarantine", table=quarantine_table)
            write_output(
                spark,
                quarantine,
                "delta_table",
                {"table": quarantine_table, "mode": "append"},
            )

    written = []
    for spec in config.outputs:
        options = resolve_values(spec.options, variables)
        frame = frames[spec.input_id]
        if spec.contract:
            # Governance: refuse to publish a table that drifted from its contract.
            contract = import_callable(spec.contract)
            progress(
                "SCHEMA",
                "Validating output contract",
                id=spec.id,
                contract=contract.object_location(),
            )
            try:
                contract.validate(frame)
            except Exception as error:
                progress("SCHEMA", "Contract validation failed", id=spec.id, error=str(error))
                raise
            progress("SCHEMA", "Output contract passed", id=spec.id)
        progress("OUTPUT", "Writing target", id=spec.id, table=options["table"])
        write_output(spark, frame, spec.kind, options)
        written.append(options["table"])

    for spec in config.post_actions:
        progress("MAINTENANCE", "Running post action", action=spec.action, target=spec.target)
        run_post_action(
            spark,
            spec.action,
            resolve_values(spec.target, variables),
            resolve_values(spec.options, variables),
        )

    result = PipelineResult(
        pipeline_id=config.pipeline.id,
        status="succeeded",
        duration_seconds=perf_counter() - started,
        outputs=tuple(written),
    )
    progress(
        "ENGINE",
        "Pipeline completed",
        pipeline=config.pipeline.id,
        seconds=f"{result.duration_seconds:.2f}",
        outputs=len(result.outputs),
    )
    return result


def _load_acon(value) -> Acon:
    if isinstance(value, Acon):
        return value
    if isinstance(value, dict):
        return Acon.from_dict(value)
    return Acon.from_yaml(value)


def _relative_to_acon(acon: Acon, value: str) -> Path:
    path = Path(value)
    if path.is_absolute() or acon.source_path is None:
        return path
    return acon.source_path.parent / path


def resolve_values(value, variables: dict[str, str]):
    """Resolve ${name} placeholders recursively in ACON runtime values."""
    if isinstance(value, str):
        for name, replacement in variables.items():
            value = value.replace(f"${{{name}}}", str(replacement))
        if "${" in value:
            raise ValueError(f"unresolved ACON variable: {value}")
        return value
    if isinstance(value, dict):
        return {key: resolve_values(item, variables) for key, item in value.items()}
    if isinstance(value, list):
        return [resolve_values(item, variables) for item in value]
    return value
