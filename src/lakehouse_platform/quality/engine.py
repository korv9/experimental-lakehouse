"""The platform's single quality gate, driven by ACON.

Rules are data: a product declares them in ``quality.yaml`` next to the table
they protect, and the ACON ``quality:`` section points at that file. This module
turns those rules into one pass/fail split and records the outcome so quality is
comparable between runs.

Criticality decides what failing means:
    error  the row is rejected — quarantined, or the run fails outright
    warn   the row is kept, but the failure is counted and persisted

Adding a check means adding an entry to ``CHECKS``; the ACON loader and the
tests both read that registry, so a rule the engine cannot evaluate is caught
before a pipeline runs.
"""
from __future__ import annotations

from pathlib import Path

import yaml

from lakehouse_platform.observability.progress import progress


def _is_not_null(column, args):
    return column.isNotNull()


def _is_in_range(column, args):
    # nulls are out of scope for a range check; is_not_null is the rule for that
    return column.isNull() | column.between(args["min_limit"], args["max_limit"])


CHECKS = {
    "is_not_null": _is_not_null,
    "is_in_range": _is_in_range,
}

CRITICALITIES = {"error", "warn"}


def load_rules(rules_path: str | Path) -> list[dict]:
    with Path(rules_path).open(encoding="utf-8") as handle:
        return yaml.safe_load(handle) or []


def _expression(rule):
    from pyspark.sql import functions as F

    check = rule["check"]
    function = check["function"]
    if function not in CHECKS:
        raise ValueError(f"unsupported quality function: {function}")
    args = check.get("arguments", {})
    expression = CHECKS[function](F.col(args["column"]), args)
    # a null outcome must not make the row vanish from both sides of the split
    return F.coalesce(expression, F.lit(False))


def apply_quality(
    df,
    rules_path: str | Path,
    on_failure: str = "fail",
    *,
    spark=None,
    catalog: str | None = None,
    run_id: str | None = None,
    table: str | None = None,
):
    """Split ``df`` into rows that pass the error rules and rows that do not.

    Warn rules never reject a row; they are evaluated so the failure count can be
    reported and persisted. When ``spark`` and ``catalog`` are given, one row per
    rule is appended to ``platform.data_quality_results``.
    """
    from pyspark.sql import functions as F

    rules = load_rules(rules_path)
    progress("QUALITY", "Rules loaded", count=len(rules), failure_mode=on_failure)

    valid = F.lit(True)
    expressions = {}
    for rule in rules:
        criticality = rule.get("criticality", "error")
        if criticality not in CRITICALITIES:
            raise ValueError(f"unknown criticality '{criticality}' in rule {rule.get('name')}")
        expression = _expression(rule)
        expressions[rule.get("name", rule["check"]["function"])] = (criticality, expression)
        if criticality == "error":
            valid = valid & expression

    good = df.filter(valid)
    bad = df.filter(~valid)

    # Counting failures costs an extra pass over the data, so only do it when the
    # numbers have somewhere to go — a run persisting to the control table.
    if spark is not None and catalog:
        counts = _failure_counts(df, expressions)
        for name, failures in counts.items():
            if failures:
                progress("QUALITY", "Rule failures", rule=name,
                         criticality=expressions[name][0], rows=failures)
        _persist_results(spark, catalog, run_id, table, expressions, counts)

    if on_failure == "fail" and bad.limit(1).count():
        raise ValueError(f"data-quality gate failed for {table or 'frame'}")

    progress("QUALITY", "Quality gate built", error_rules=sum(
        1 for criticality, _ in expressions.values() if criticality == "error"
    ))
    return good, bad


def _failure_counts(df, expressions) -> dict[str, int]:
    """Rows failing each rule, counted in a single pass."""
    from pyspark.sql import functions as F

    if not expressions:
        return {}
    row = df.agg(
        *[
            F.sum(F.when(~expression, 1).otherwise(0)).alias(name)
            for name, (_, expression) in expressions.items()
        ]
    ).collect()[0]
    return {name: int(row[name] or 0) for name in expressions}


def _persist_results(spark, catalog, run_id, table, expressions, counts) -> None:
    """Append one row per rule to platform.data_quality_results."""
    from datetime import datetime, timezone

    from pyspark.sql import Row

    checked_at = datetime.now(timezone.utc)
    rows = [
        Row(
            run_id=run_id,
            table_name=table,
            check_name=name,
            status=("pass" if not counts[name] else criticality_status(criticality)),
            metric=float(counts[name]),
            threshold=0.0,
            checked_at=checked_at,
        )
        for name, (criticality, _) in expressions.items()
    ]
    if not rows:
        return
    target = f"{catalog}.platform.data_quality_results"
    progress("QUALITY", "Persisting results", table=target, rules=len(rows))
    spark.createDataFrame(rows).write.mode("append").saveAsTable(target)


def criticality_status(criticality: str) -> str:
    return "fail" if criticality == "error" else "warn"
