"""The ACON ``contract:`` gate (no Spark required).

An output may declare a contract; the engine then validates the frame before it
is written. These tests use fakes to prove the wiring: a passing contract still
writes, a failing one raises and writes nothing.
"""
import pytest

from lakehouse_platform.core.acon import Acon


class _Contract:
    """Stand-in for a TableDefinition."""

    passes = True

    @classmethod
    def object_location(cls):
        return "silver.fake"

    @classmethod
    def validate(cls, df):
        if not cls.passes:
            raise ValueError("silver.fake: unexpected columns ['drifted']")
        return True


class _PassingContract(_Contract):
    passes = True


class _FailingContract(_Contract):
    passes = False


def _acon_dict(contract_ref):
    return {
        "pipeline": {"id": "contract_gate_test"},
        "inputs": [{"id": "src", "reader": "fake_reader", "options": {"table": "bronze.fake"}}],
        "outputs": [
            {
                "id": "out",
                "input_id": "src",
                "writer": "fake_writer",
                "contract": contract_ref,
                "options": {"table": "${catalog}.silver.fake"},
            }
        ],
    }


def test_acon_parses_the_contract_reference():
    acon = Acon.from_dict(_acon_dict("tests.unit.test_acon_contract_gate:_PassingContract"))
    assert acon.outputs[0].contract == "tests.unit.test_acon_contract_gate:_PassingContract"


def test_output_without_a_contract_is_still_valid():
    acon = Acon.from_dict(_acon_dict(None))
    assert acon.outputs[0].contract is None


def _run(monkeypatch, contract_ref):
    """Run the engine with fake reader/writer so no Spark is involved."""
    from lakehouse_platform import engine

    written = []
    monkeypatch.setattr(engine, "read_input", lambda spark, kind, options: "frame")
    monkeypatch.setattr(
        engine,
        "write_output",
        lambda spark, df, kind, options: written.append(options["table"]),
    )
    engine.run_pipeline(None, _acon_dict(contract_ref), {"catalog": "dev"})
    return written


def test_passing_contract_writes_the_table(monkeypatch):
    written = _run(monkeypatch, "tests.unit.test_acon_contract_gate:_PassingContract")
    assert written == ["dev.silver.fake"]


def test_failing_contract_blocks_the_write(monkeypatch):
    with pytest.raises(ValueError, match="unexpected columns"):
        _run(monkeypatch, "tests.unit.test_acon_contract_gate:_FailingContract")
