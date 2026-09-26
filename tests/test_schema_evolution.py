"""目标端结构演进：新增列缺省语义确定，删列/改名被拒绝，历史结论可复现。"""
from __future__ import annotations

import logging

import pytest

from app.connectors.jsonl import JsonlConnector
from app.pipeline import IngestionPipeline
from app.schema import Column, ColumnType, SchemaError, TableSchema
from app.store import SCHEMA_V1, TargetStore

from .conftest import good_row, log_state, write_jsonl

logger = logging.getLogger("ingestion.test")

V2 = TableSchema(2, SCHEMA_V1.columns + (Column("score", ColumnType.REAL, default=0.0),))


def test_add_column_default_is_deterministic(data_dir, store):
    src = data_dir / "events.jsonl"
    write_jsonl(src, [good_row("old1"), good_row("old2")])
    IngestionPipeline(JsonlConnector(src), store).run_until_drained()

    store.evolve(V2)
    # 演进后新写入的记录未提供新列 -> 同样落缺省值
    write_jsonl(src, [good_row("old1"), good_row("old2"), good_row("new1")])
    IngestionPipeline(JsonlConnector(src), store).run_until_drained()
    log_state(store, "jsonl", "add-column")

    rows = {r["record_id"]: r for r in store.fetch_all()}
    assert store.count_rows() == 3
    # 历史行与新行的缺省语义一致且稳定
    assert all(rows[rid]["score"] == 0.0 for rid in ("old1", "old2", "new1"))
    # 变更流水记录了演进
    kinds = [e["kind"] for e in store.changelog()]
    assert "schema_evolve" in kinds


def test_drop_rename_retype_are_rejected(store):
    with pytest.raises(SchemaError, match="dropping"):
        store.evolve(TableSchema(2, SCHEMA_V1.columns[:2]))           # 删列
    renamed = (Column("rid", ColumnType.TEXT, required=True),) + SCHEMA_V1.columns[1:]
    with pytest.raises(SchemaError, match="renamed"):
        store.evolve(TableSchema(2, renamed))                          # 改名
    retyped = SCHEMA_V1.columns[:3] + (Column("value", ColumnType.TEXT, required=True),) + SCHEMA_V1.columns[4:]
    with pytest.raises(SchemaError, match="type changed"):
        store.evolve(TableSchema(2, retyped))                          # 改类型
    with pytest.raises(SchemaError, match="increment"):
        store.evolve(TableSchema(5, SCHEMA_V1.columns + (Column("s", ColumnType.REAL, default=0.0),)))
    log_state(store, "jsonl", "evolve-rejected")
    assert store.schema.version == 1      # 非法演进不影响当前结构


def test_malformed_records_during_schema_change(data_dir, store):
    """结构变更前后，畸形记录仍被正确归类，合法记录不受影响。"""
    src = data_dir / "events.jsonl"
    write_jsonl(src, [good_row("g1"), good_row("bad", ts="oops")])
    IngestionPipeline(JsonlConnector(src), store).run_until_drained()

    store.evolve(V2)
    write_jsonl(src, [
        good_row("g1"), good_row("bad", ts="oops"),      # 旧内容（重复消费）
        good_row("g2"), "{broken json",                  # 新内容含畸形记录
    ])
    r = IngestionPipeline(JsonlConnector(src), store).run_until_drained()
    log_state(store, "jsonl", "evolve-with-malformed")

    assert r.inserted == 1                               # 只有 g2 是新增
    assert store.count_rows() == 2
    reasons = {x["reason"] for x in store.list_rejects()}
    assert reasons == {"invalid_format"}                 # 两处畸形都正确归类
    assert store.get_checkpoint("jsonl") == 4


def test_changelog_reproducible_after_rule_update(data_dir):
    """同样的操作序列重放，位点与变更流水结论一致（可复现）。"""
    def replay(db):
        s = TargetStore(db)
        src = data_dir / "events.jsonl"   # 同一来源文件
        write_jsonl(src, [good_row("a"), good_row("b", ts="bad")])
        IngestionPipeline(JsonlConnector(src), s).run_until_drained()
        s.evolve(V2)
        write_jsonl(src, [good_row("a"), good_row("b", ts="bad"), good_row("c")])
        IngestionPipeline(JsonlConnector(src), s).run_until_drained()
        out = ([(e["kind"], e["detail"]) for e in s.changelog()],
               s.get_checkpoint("jsonl"), s.count_rows(), s.list_rejects())
        s.close()
        return out

    first = replay(data_dir / "run1.db")
    second = replay(data_dir / "run2.db")
    logger.info("replay#1 %s", first)
    logger.info("replay#2 %s", second)
    assert first == second
