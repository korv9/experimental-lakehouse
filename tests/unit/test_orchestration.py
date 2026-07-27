"""Orchestration bundle checks (no Spark, no Databricks CLI required).

A job file that drifts from the repository is worse than no job file, so these
tests keep orchestration.yml honest: every notebook it schedules must exist,
task graphs must be sound, and the bundle root must actually include it.
"""
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[2]
BUNDLE = ROOT / "databricks.yml"
ORCHESTRATION = ROOT / "orchestration.yml"


def _load(path):
    return yaml.safe_load(path.read_text(encoding="utf-8"))


def _jobs():
    return _load(ORCHESTRATION)["resources"]["jobs"]


def test_bundle_root_includes_the_orchestration_file():
    bundle = _load(BUNDLE)
    assert bundle["bundle"]["name"]
    assert "orchestration.yml" in bundle["include"]


def test_every_scheduled_notebook_exists():
    missing = []
    for job_name, job in _jobs().items():
        for task in job["tasks"]:
            path = task["notebook_task"]["notebook_path"]
            if not (ROOT / path.lstrip("./")).is_file():
                missing.append(f"{job_name}.{task['task_key']} -> {path}")
    assert missing == [], f"orchestration references missing notebooks: {missing}"


def test_task_dependencies_resolve_within_their_job():
    for job_name, job in _jobs().items():
        keys = {task["task_key"] for task in job["tasks"]}
        assert len(keys) == len(job["tasks"]), f"{job_name} has duplicate task_keys"
        for task in job["tasks"]:
            for dependency in task.get("depends_on", []):
                assert dependency["task_key"] in keys, (
                    f"{job_name}.{task['task_key']} depends on unknown "
                    f"{dependency['task_key']}"
                )


def test_every_task_declares_the_cluster_its_job_defines():
    for job_name, job in _jobs().items():
        declared = {cluster["job_cluster_key"] for cluster in job.get("job_clusters", [])}
        for task in job["tasks"]:
            key = task.get("job_cluster_key")
            assert key in declared, f"{job_name}.{task['task_key']} uses undeclared cluster {key}"


def test_variables_used_by_jobs_are_declared_in_the_bundle():
    declared = set(_load(BUNDLE)["variables"])
    used = set()
    text = ORCHESTRATION.read_text(encoding="utf-8")
    for token in text.split("${var.")[1:]:
        used.add(token.split("}")[0])
    assert used <= declared, f"undeclared bundle variables: {sorted(used - declared)}"


def test_every_product_pipeline_is_scheduled_or_explicitly_manual():
    """Each product should be represented, so nothing is silently unrunnable."""
    jobs = _jobs()
    for product in ("example_works", "messy_records", "philosophy_litterature"):
        assert f"{product}_pipeline" in jobs, f"{product} has no job in orchestration.yml"
