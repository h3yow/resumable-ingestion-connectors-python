"""重复消费、服务重启、并发消费与位点回退。"""
from __future__ import annotations

import logging
import threading

import pytest

from app.connectors.jsonl import JsonlConnector
from app.pipeline import IngestionPipeline
from app.store import PositionRollbackError, TargetStore

from .conftest import good_row, log_state, write_jsonl

logger = logging.getLogger("ingestion.test")


def _rows(n: int, prefix: str = "r") -> list[dict]:
    return [good_row(f"{prefix}{i}", value=float(i)) for i in range(n)]


def test_duplicate_consumption_is_idempotent(data_dir, store):
    src = data_dir / "events.jsonl"
    write_jsonl(src, _rows(5))
    pipe = IngestionPipeline(JsonlConnector(src), store)

    r1 = pipe.run_until_drained()
    r2 = pipe.run_until_drained()   # 重复消费同一来源
    log_state(store, "jsonl", "dup-consume")

    assert r1.inserted == 5 and r2.inserted == 0
    assert store.count_rows() == 5
    assert store.get_checkpoint("jsonl") == 5


def test_restart_resumes_from_checkpoint(data_dir):
    src = data_dir / "events.jsonl"
    write_jsonl(src, _rows(3))
    db = data_dir / "target.db"

    s1 = TargetStore(db)
    IngestionPipeline(JsonlConnector(src), s1).run_until_drained()
    pos_before = s1.get_checkpoint("jsonl")
    s1.close()

    # 模拟服务重启：新连接、新流水线，来源再追加 2 条
    write_jsonl(src, _rows(5))
    s2 = TargetStore(db)
    r = IngestionPipeline(JsonlConnector(src), s2).run_until_drained()
    log_state(s2, "jsonl", "after-restart")

    assert pos_before == 3
    assert r.inserted == 2                 # 只消费新增部分
    assert s2.count_rows() == 5            # 不丢不重
    assert s2.get_checkpoint("jsonl") == 5
    s2.close()


def test_concurrent_consumption_no_dup_no_loss(data_dir):
    src = data_dir / "events.jsonl"
    write_jsonl(src, _rows(200))
    db = data_dir / "target.db"
    store = TargetStore(db)
    errors: list[Exception] = []

    def worker():
        try:
            # 每个线程独立流水线，共享同一 store（模拟并发消费者）
            IngestionPipeline(JsonlConnector(src), store).run_until_drained(limit=7)
        except Exception as exc:  # pragma: no cover
            errors.append(exc)

    threads = [threading.Thread(target=worker) for _ in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    log_state(store, "jsonl", "concurrent")

    assert not errors
    assert store.count_rows() == 200
    assert store.get_checkpoint("jsonl") == 200
    ids = [row["record_id"] for row in store.fetch_all()]
    assert len(ids) == len(set(ids))       # 无重复
    store.close()


def test_position_rollback_is_rejected(data_dir, store):
    src = data_dir / "events.jsonl"
    write_jsonl(src, _rows(4))
    IngestionPipeline(JsonlConnector(src), store).run_until_drained()
    assert store.get_checkpoint("jsonl") == 4

    # 位点回退必须被明确拒绝，而不是从中间继续
    with pytest.raises(PositionRollbackError):
        store.commit_batch("jsonl", [], [], 2)
    log_state(store, "jsonl", "rollback-rejected")
    assert store.get_checkpoint("jsonl") == 4   # 位点未被破坏
    assert store.count_rows() == 4
