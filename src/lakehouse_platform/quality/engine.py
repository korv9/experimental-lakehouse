"""Small portable quality gate; DQX can be plugged in behind the same boundary."""
from __future__ import annotations

from pathlib import Path

import yaml

from lakehouse_platform.observability.progress import progress


def apply_quality(df, rules_path: str | Path, on_failure: str = "fail"):
    from pyspark.sql import functions as F

    with Path(rules_path).open(encoding="utf-8") as handle:
        rules = yaml.safe_load(handle) or []
    progress("QUALITY", "Rules loaded", count=len(rules), failure_mode=on_failure)

    valid = F.lit(True)
    for rule in rules:
        check = rule["check"]
        args = check.get("arguments", {})
        function = check["function"]
        column = F.col(args["column"])
        if function == "is_not_null":
            expression = column.isNotNull()
        elif function == "is_in_range":
            expression = column.isNull() | column.between(args["min_limit"], args["max_limit"])
        else:
            raise ValueError(f"unsupported quality function: {function}")
        if rule.get("criticality", "error") == "error":
            valid = valid & expression

    good = df.filter(valid)
    bad = df.filter(~valid)
    if on_failure == "fail" and bad.limit(1).count():
        raise ValueError("data-quality gate failed")
    progress("QUALITY", "Quality gate built", error_rules=sum(
        rule.get("criticality", "error") == "error" for rule in rules
    ))
    return good, bad
