"""Every product quality.yaml must be executable by the quality engine.

These run without Spark. They catch the failure mode that is otherwise only
visible at 05:00 in a scheduled run: a rule that references a check the engine
does not implement, or a criticality it does not understand.
"""
from pathlib import Path

import pytest
import yaml

from lakehouse_platform.quality.engine import CHECKS, CRITICALITIES, load_rules

ROOT = Path(__file__).resolve().parents[2]
RULE_FILES = sorted(ROOT.glob("products/*/tables/*/*/quality.yaml"))


def test_products_actually_declare_quality_rules():
    assert RULE_FILES, "no product quality.yaml files found"


@pytest.mark.parametrize("path", RULE_FILES, ids=lambda p: str(p.relative_to(ROOT)))
def test_rules_are_well_formed_and_supported(path):
    for rule in load_rules(path):
        assert rule.get("name"), f"rule without a name in {path}"
        assert rule.get("criticality", "error") in CRITICALITIES
        check = rule["check"]
        assert check["function"] in CHECKS, (
            f"{path}: '{check['function']}' is not implemented by the quality engine"
        )
        assert "column" in check.get("arguments", {})


@pytest.mark.parametrize("path", RULE_FILES, ids=lambda p: str(p.relative_to(ROOT)))
def test_range_rules_declare_both_limits(path):
    for rule in load_rules(path):
        if rule["check"]["function"] == "is_in_range":
            arguments = rule["check"]["arguments"]
            assert "min_limit" in arguments and "max_limit" in arguments


def test_every_acon_quality_step_points_at_a_real_rule_file():
    for acon_path in sorted(ROOT.glob("products/*/pipelines/*.yaml")):
        acon = yaml.safe_load(acon_path.read_text(encoding="utf-8"))
        for step in acon.get("quality") or []:
            rules = (acon_path.parent / step["rules"]).resolve()
            assert rules.is_file(), f"{acon_path}: missing rules file {step['rules']}"
            assert step.get("on_failure", "fail") in {"fail", "quarantine"}
            if step.get("on_failure") == "quarantine":
                assert step.get("quarantine_table"), (
                    f"{acon_path}: quarantine mode needs a quarantine_table"
                )
