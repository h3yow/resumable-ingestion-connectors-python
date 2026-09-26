"""重复消费幂等、重启续传、位点回退显式拒绝。"""
import json

import pytest

from app.checkpoint import CheckpointStore, OffsetRegressionError
from app.connectors.jsonl import JsonlConnector
from app.pipeline import Pipeline, connect_db

SCHEMA = {"id": str, "name": str, "age": int}


def _source(tmp_path, n=5):
    path = tmp_path / "src.jsonl"
    path.write_text(
        "\n".join(json.dumps({"id": f"r{i}", "name": f"n{i}", "age": i}) for i in range(n)) + "\n",
        encoding="utf-8",
    )
    return str(path)


def test_duplicate_consumption_is_idempotent(pipeline, tmp_path, caplog):
    src = _source(tmp_path)
    connector = JsonlConnector(src, name="dup")

    first = pipeline.run_once(connector)
    assert first.accepted == 5 and first.checkpoint.last_offset == 4

    # 重复消费同一来源：位点之后已无数据，目标端不重复
    second = pipeline.run_once(connector)
    assert second.accepted == 0
    rows = pipeline.sink.fetch_all()
    assert len(rows) == 5
    assert len({r["record_id"] for r in rows}) == 5

    # 直接重复 upsert 同一批记录：仍不产生重复行
    from app.normalize import normalize_batch
    raws = list(connector.read(-1))
    accepted, _ = normalize_batch(raws, SCHEMA)
    pipeline.sink.upsert(accepted)
    pipeline.sink.upsert(accepted)
    assert len(pipeline.sink.fetch_all()) == 5
    print("位点:", pipeline.checkpoints.get("dup").last_offset, "行数:", len(rows))


def test_restart_resumes_from_checkpoint(tmp_path):
    src = _source(tmp_path, n=5)
    db_path = tmp_path / "ingest.db"

    p1 = Pipeline(connect_db(str(db_path)), dict(SCHEMA))
    p1.init()
    r1 = p1.run_once(JsonlConnector(src, name="s"), batch_size=2)
    assert r1.checkpoint.last_offset == 1  # 只消费了前两条

    # 模拟服务重启：新连接、新 Pipeline，从持久化位点续传
    p2 = Pipeline(connect_db(str(db_path)), dict(SCHEMA))
    p2.init()
    assert p2.checkpoints.get("s").last_offset == 1
    r2 = p2.run_once(JsonlConnector(src, name="s"))
    assert r2.accepted == 3 and r2.checkpoint.last_offset == 4

    rows = p2.sink.fetch_all()
    assert len(rows) == 5  # 不重复、不丢失
    assert [r["record_id"] for r in rows] == [f"s#r{i}" for i in range(5)]
    print("重启后位点:", r2.checkpoint.last_offset, "总行数:", len(rows))


def test_offset_regression_is_rejected(pipeline, tmp_path):
    src = _source(tmp_path)
    pipeline.run_once(JsonlConnector(src, name="reg"))
    assert pipeline.checkpoints.get("reg").last_offset == 4

    store: CheckpointStore = pipeline.checkpoints
    # 位点回退（含相等）必须被显式拒绝，而不是从中间继续
    with pytest.raises(OffsetRegressionError):
        store.commit("reg", 2)
    with pytest.raises(OffsetRegressionError):
        store.commit("reg", 4)
    # 位点未被破坏
    assert store.get("reg").last_offset == 4
    print("回退被拒绝，位点保持:", store.get("reg").last_offset)
