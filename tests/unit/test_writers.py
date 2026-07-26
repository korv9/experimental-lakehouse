from __future__ import annotations

from types import ModuleType

import pytest

from lakehouse_platform.io import writers


class MergeRecorder:
    def __init__(self):
        self.updated = False
        self.inserted = False
        self.executed = False

    def alias(self, name):
        return self

    def merge(self, source, predicate):
        return self

    def whenMatchedUpdateAll(self):
        self.updated = True
        return self

    def whenNotMatchedInsertAll(self):
        self.inserted = True
        return self

    def execute(self):
        self.executed = True


class Frame:
    def alias(self, name):
        return self


class Catalog:
    def tableExists(self, table):
        return True


class Spark:
    catalog = Catalog()


def test_insert_only_merge_does_not_update_append_only_bronze(monkeypatch):
    recorder = MergeRecorder()
    delta_module = ModuleType("delta.tables")

    class DeltaTable:
        @staticmethod
        def forName(spark, table):
            return recorder

    delta_module.DeltaTable = DeltaTable
    monkeypatch.setitem(__import__("sys").modules, "delta.tables", delta_module)

    writers.write_output(
        Spark(),
        Frame(),
        "delta_merge",
        {
            "table": "dev.bronze.raw",
            "keys": ["ingestion_id"],
            "when_matched": "ignore",
        },
    )

    assert recorder.inserted
    assert recorder.executed
    assert not recorder.updated


def test_merge_rejects_unknown_matched_strategy(monkeypatch):
    recorder = MergeRecorder()
    delta_module = ModuleType("delta.tables")

    class DeltaTable:
        @staticmethod
        def forName(spark, table):
            return recorder

    delta_module.DeltaTable = DeltaTable
    monkeypatch.setitem(__import__("sys").modules, "delta.tables", delta_module)

    with pytest.raises(ValueError, match="when_matched"):
        writers.write_output(
            Spark(),
            Frame(),
            "delta_merge",
            {"table": "dev.silver.work", "keys": ["id"], "when_matched": "delete"},
        )
