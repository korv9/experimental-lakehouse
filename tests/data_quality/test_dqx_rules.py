"""Data-quality test: the DQX rule set is well-formed.

Cheap guard that runs without Spark — catches typos in the rule definitions
before they hit a pipeline.
"""
from lakehouse_platform.quality.dqx import WORKS_CHECKS


def test_rules_have_required_fields():
    for rule in WORKS_CHECKS:
        assert rule["criticality"] in {"error", "warn"}
        assert "function" in rule["check"]
        assert "column" in rule["check"]["arguments"]
