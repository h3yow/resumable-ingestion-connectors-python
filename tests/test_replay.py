"""历史重放：导出操作流水，在干净实例上按真实顺序重放，逐项状态一致。"""
import json

import pytest

from app.connectors.jsonl import JsonlConnector
from app.pipeline import Pipeline, connect_db
from app.replay import diff_states, replay
from app.sink import SchemaEvolutionError

SCHEMA = {"id": str, "name": str, "age": int}


def _write(path, rows):
    path.write_text("\n".join(json.dumps(r) for r in rows) + "\n", encoding="utf-8")
    return str(path)


def test_replay_on_clean_instance(tmp_path, caplog):
    src_a = _write(tmp_path / "a.jsonl", [
        {"id": "a1", "name": "alice", "age": 30},
        {"id": "a2", "name": "bob"},                       # 缺 age -> missing_field
        {"id": "a3", "name": "carol", "age": 33, "email": "c@x.com", "tag": "x"},
    ])
    src_b = tmp_path / "b.jsonl"
    src_b.write_text(
        json.dumps({"id": "b1", "name": "dave", "age": 40}) + "\n"
        + "not json\n"                                     # invalid_format
        + json.dumps({"id": "b2", "name": "erin", "age": 41, "email": "e@x.com"}) + "\n",
        encoding="utf-8",
    )

    # ---- 原实例：摄入与结构变更交错发生 ----
    p1 = Pipeline(connect_db(str(tmp_path / "orig.db")), dict(SCHEMA))
    p1.init()
    p1.run_once(JsonlConnector(src_a, name="a"), batch_size=2)   # 部分批次
    p1.apply_schema({**SCHEMA, "email": str})                    # v2 兼容变更
    with pytest.raises(SchemaEvolutionError):
        p1.apply_schema({"id": str, "name": str})                # 不兼容，拒绝留痕
    p1.run_once(JsonlConnector(src_a, name="a"))                 # 续传完 a
    p1.run_once(JsonlConnector(str(src_b), name="b"))            # 消费 b

    ops = p1.history.export()
    assert [op.kind.value for op in ops] == [
        "ingest", "schema", "schema", "ingest", "ingest",
    ]
    print("导出历史:", [(op.seq, op.kind.value, op.payload.get("to_offset",
          op.payload.get("status"))) for op in ops])

    # ---- 干净实例：按真实顺序重放 ----
    p2 = Pipeline(connect_db(str(tmp_path / "replica.db")), dict(SCHEMA))
    p2.init()
    replay(ops, p2, {"a": JsonlConnector(src_a, name="a"),
                     "b": JsonlConnector(str(src_b), name="b")})

    # ---- 逐项比对：数据、位点、拒绝记录、结构历史、操作流水 ----
    assert diff_states(p1._db, p2._db) == {}

    rows = p2.sink.fetch_all()
    assert len(rows) == 3  # a1, a3, b2；不重复不丢失
    assert {r["record_id"] for r in rows} == {"a#a1", "a#a3", "b#b2"}
    assert p2.checkpoints.get("a").last_offset == 2
    assert p2.checkpoints.get("b").last_offset == 2

    rejections = p2.rejections()
    assert [(s, o, r) for s, o, r, _ in rejections] == [
        ("a", 1, "missing_field"),   # a2 缺 age（v1 时期）
        ("b", 0, "missing_field"),   # b1 缺 email（v2 生效后）
        ("b", 1, "invalid_format"),
    ]

    history = p2.sink.registry.history()
    assert [(e.version, e.status.value) for e in history] == [
        (1, "applied"), (2, "applied"), (None, "rejected"),
    ]
    assert p2.sink.registry.current_version() == 2
    # 重放实例的操作流水与原实例逐条一致
    assert [(op.kind, op.payload) for op in p2.history.export()] == [
        (op.kind, op.payload) for op in ops
    ]
    print("重放后逐项一致: 行数=%d 位点=%s 拒绝=%d 结构版本=v%d" % (
        len(rows),
        {s: p2.checkpoints.get(s).last_offset for s in ("a", "b")},
        len(rejections), p2.sink.registry.current_version(),
    ))
