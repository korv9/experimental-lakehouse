from __future__ import annotations

import importlib
import sys
from types import ModuleType, SimpleNamespace


def _load_runner_without_pyspark(monkeypatch):
    pyspark = ModuleType("pyspark")
    pyspark_sql = ModuleType("pyspark.sql")
    pyspark_sql.SparkSession = object
    control = ModuleType("lakehouse_platform.metadata.control_tables")

    class Checkpoint:
        def __init__(
            self,
            pipeline_name,
            source_name,
            partition_key,
            cursor,
            watermark_value,
            page_number,
            status,
            run_id,
        ):
            self.pipeline_name = pipeline_name
            self.source_name = source_name
            self.partition_key = partition_key
            self.cursor = cursor
            self.watermark_value = watermark_value
            self.page_number = page_number
            self.status = status
            self.run_id = run_id

    control.IngestionCheckpoint = Checkpoint
    control.finish_run = lambda *args, **kwargs: None
    control.get_checkpoint = lambda *args, **kwargs: None
    control.set_checkpoint = lambda *args, **kwargs: None
    control.set_watermark = lambda *args, **kwargs: None
    control.start_run = lambda *args, **kwargs: "run-test"
    monkeypatch.setitem(sys.modules, "pyspark", pyspark)
    monkeypatch.setitem(sys.modules, "pyspark.sql", pyspark_sql)
    monkeypatch.setitem(sys.modules, control.__name__, control)
    sys.modules.pop("lakehouse_platform.ingestion.runner", None)
    return importlib.import_module("lakehouse_platform.ingestion.runner")


def test_corpus_runner_batches_validates_commits_and_checkpoints(monkeypatch):
    runner = _load_runner_without_pyspark(monkeypatch)
    requests = []
    committed = []
    checkpoints = []
    finished = []

    class FakeClient:
        def __init__(self, *args, **kwargs):
            pass

        def get(self, endpoint, params=None):
            ids = params["ids"].split(",")
            requests.append(ids)
            return {"results": [{"id": int(source_id)} for source_id in ids]}

    monkeypatch.setattr(runner, "RestClient", FakeClient)
    monkeypatch.setattr(runner, "start_run", lambda *args, **kwargs: "run-test")
    monkeypatch.setattr(runner, "get_checkpoint", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        runner,
        "set_checkpoint",
        lambda spark, catalog, checkpoint: checkpoints.append(checkpoint),
    )
    monkeypatch.setattr(runner, "set_watermark", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        runner,
        "finish_run",
        lambda *args, **kwargs: finished.append(kwargs),
    )
    monkeypatch.setattr(
        runner,
        "_commit_bronze_page",
        lambda spark, target, rows, contract=None: committed.append((target, rows, contract)),
    )

    run_id = runner.ingest_corpus(
        SimpleNamespace(),
        "config/sources/philosophy_gutendex.yaml",
        catalog="dev_lakehouse",
    )

    assert run_id == "run-test"
    assert [len(batch) for batch in requests] == [25, 25, 3]
    assert sum(len(rows) for _, rows, _ in committed) == 53
    assert all(target == "dev_lakehouse.bronze.philosophy_litterature_work_raw" for target, _, _ in committed)
    assert checkpoints[-1].status == "completed"
    assert checkpoints[-1].page_number == 3
    assert finished[-1]["status"] == "success"
    assert finished[-1]["read"] == 53
    assert finished[-1]["written"] == 53
    sys.modules.pop("lakehouse_platform.ingestion.runner", None)
