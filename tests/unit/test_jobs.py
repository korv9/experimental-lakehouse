import pytest

from lakehouse_platform import jobs


class Frame:
    def count(self):
        return 3


class Contract:
    validated = False

    @classmethod
    def object_location(cls):
        return "silver.work"

    @classmethod
    def column_names(cls):
        return ["work_id"]

    @classmethod
    def validate(cls, frame):
        cls.validated = True


def _patch_runtime(monkeypatch):
    calls = []
    monkeypatch.setattr(jobs, "start_run", lambda *args, **kwargs: "run-1")
    monkeypatch.setattr(jobs, "finish_run", lambda *args, **kwargs: calls.append(kwargs))
    monkeypatch.setattr(jobs, "write_output", lambda *args: calls.append(args))
    monkeypatch.setattr(jobs, "progress", lambda *args, **kwargs: None)
    return calls


def test_process_job_validates_and_merges_a_ready_dataframe(monkeypatch):
    calls = _patch_runtime(monkeypatch)
    frame = Frame()
    job_config = {
        "pipeline_name": "bronze_to_silver",
        "source_name": "source",
        "contract": Contract,
        "target": {
            "path": "silver.work",
            "format": "delta",
            "mode": "merge",
            "keys": ["work_id"],
            "when_matched": "ignore",
        },
    }

    run_id = jobs.process_job(object(), job_config, catalog="dev", dataframe=frame)

    assert run_id == "run-1"
    assert Contract.validated
    writer_call = next(call for call in calls if isinstance(call, tuple) and len(call) == 4)
    assert writer_call[2] == "delta_merge"
    assert writer_call[3]["table"] == "dev.silver.work"
    assert writer_call[3]["when_matched"] == "ignore"


@pytest.mark.parametrize("mode", ["overwrite", "append"])
def test_process_job_supports_table_write_modes(monkeypatch, mode):
    calls = _patch_runtime(monkeypatch)
    job_config = {
        "pipeline_name": "rebuild",
        "source_name": "source",
        "contract": Contract,
        "target": {"path": "dev.gold.fact", "format": "delta", "mode": mode},
    }

    jobs.process_job(object(), job_config, catalog="ignored", dataframe=Frame())

    writer_call = next(call for call in calls if isinstance(call, tuple) and len(call) == 4)
    assert writer_call[2] == "delta_table"
    assert writer_call[3]["mode"] == mode
    assert writer_call[3]["table"] == "dev.gold.fact"


def test_process_job_can_build_with_context_for_ingestion(monkeypatch):
    _patch_runtime(monkeypatch)
    contexts = []
    job_config = {
        "pipeline_name": "ingest",
        "source_name": "source",
        "contract": Contract,
        "target": {"path": "bronze.raw", "mode": "append"},
        "transformation": lambda context: contexts.append(context) or Frame(),
    }

    jobs.process_job(object(), job_config, catalog="dev")

    assert contexts[0].catalog == "dev"
    assert contexts[0].run_id == "run-1"


def test_process_job_records_failure(monkeypatch):
    finishes = []
    monkeypatch.setattr(jobs, "start_run", lambda *args, **kwargs: "run-2")
    monkeypatch.setattr(jobs, "finish_run", lambda *args, **kwargs: finishes.append(kwargs))
    monkeypatch.setattr(jobs, "progress", lambda *args, **kwargs: None)
    job_config = {
        "pipeline_name": "broken",
        "source_name": "source",
        "contract": Contract,
        "target": {"path": "silver.work", "mode": "merge", "keys": ["work_id"]},
        "transformation": lambda context: (_ for _ in ()).throw(ValueError("bad row")),
    }

    with pytest.raises(ValueError, match="bad row"):
        jobs.process_job(object(), job_config, catalog="dev")

    assert finishes == [{"status": "failed", "error": "bad row"}]


def test_process_job_prints_expected_and_actual_schema_on_contract_error(
    monkeypatch, capsys
):
    _patch_runtime(monkeypatch)

    class ExpectedSchema:
        def simpleString(self):
            return "struct<work_id:string>"

    class BrokenContract(Contract):
        @classmethod
        def spark_schema(cls):
            return ExpectedSchema()

        @classmethod
        def validate(cls, frame):
            raise ValueError("wrong column types")

    class PrintableFrame(Frame):
        def printSchema(self):
            print("root\n |-- work_id: long")

    job_config = {
        "pipeline_name": "schema_error",
        "source_name": "source",
        "contract": BrokenContract,
        "target": {"path": "silver.work", "mode": "append"},
    }

    with pytest.raises(ValueError, match="wrong column types"):
        jobs.process_job(
            object(),
            job_config,
            catalog="dev",
            dataframe=PrintableFrame(),
        )

    output = capsys.readouterr().out
    assert "Expected: struct<work_id:string>" in output
    assert "work_id: long" in output


def test_process_job_rejects_merge_without_keys(monkeypatch):
    _patch_runtime(monkeypatch)
    job_config = {
        "pipeline_name": "invalid",
        "source_name": "source",
        "contract": Contract,
        "target": {"path": "silver.work", "mode": "merge"},
    }

    with pytest.raises(ValueError, match="target.keys"):
        jobs.process_job(object(), job_config, catalog="dev", dataframe=Frame())
