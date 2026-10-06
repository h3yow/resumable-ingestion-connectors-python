"""历史重放：导出摄入+结构变更的混合历史，在干净实例上原样重放并逐项比对。"""
import json

import pytest

from app.connectors.jsonl import JsonlConnector
from app.history import export_history, replay_history, verify_replay
from app.pipeline import Pipeline, connect_db
from app.sink import SchemaEvolutionError

SCHEMA = {"id": str, "name": str, "age": int}


def _write(tmp_path, name, rows):
    path = tmp_path / name
    path.write_text("\n".join(json.dumps(r) for r in rows) + "\n", encoding="utf-8")
    return str(path)


def _build_original(tmp_path):
    """构造一个含摄入、拒绝、兼容/不兼容结构变更的混合历史实例。"""
    db = connect_db(str(tmp_path / "orig.db"))
    p = Pipeline(db, dict(SCHEMA))
    p.init()

    # 1) 第一批摄入：含畸形记录（缺字段、类型不符、非 JSON）
    src1 = _write(tmp_path, "s1.jsonl", [
        {"id": "a", "name": "alice", "age": 30},
        {"id": "b", "name": "bob"},                    # 缺 age -> rejected
        {"id": "c", "name": "carol", "age": "abc"},    # 类型不符 -> rejected
    ])
    p.run_once(JsonlConnector(src1, name="s1"))

    # 2) 兼容结构变更：加 email -> 版本 2
    p.apply_schema({**SCHEMA, "email": str})

    # 3) 第二批摄入：带新字段，且来自另一来源
    src2 = _write(tmp_path, "s2.jsonl", [
        {"id": "x", "name": "xav", "age": 20, "email": "x@x"},
        {"id": "y", "name": "yves", "age": 21, "email": "y@y", "tag": "t"},  # 演进新列
    ])
    p.run_once(JsonlConnector(src2, name="s2"))

    # 4) 不兼容结构变更：删列被拒（留痕）
    with pytest.raises(SchemaEvolutionError):
        p.apply_schema({"id": str, "name": str})

    # 5) 再摄入一批 s1（位点续传）
    src1b = _write(tmp_path, "s1.jsonl", [
        {"id": "a", "name": "alice", "age": 30},
        {"id": "b", "name": "bob"},
        {"id": "c", "name": "carol", "age": "abc"},
        {"id": "d", "name": "dave", "age": 40, "email": "d@d"},
    ])
    p.run_once(JsonlConnector(src1b, name="s1"))
    return p


def test_replay_on_clean_instance_matches_item_by_item(tmp_path, caplog):
    orig = _build_original(tmp_path)
    exported = export_history(orig._db)
    # 导出物可 JSON 落盘（模拟搬运到干净环境）
    exported = json.loads(json.dumps(exported, ensure_ascii=False))
    assert [op["kind"] for op in exported["ops"]] == [
        "ingest", "schema", "ingest", "schema", "ingest",
    ]

    # 全新干净实例：新数据库文件，仅带相同初始结构配置
    fresh = Pipeline(connect_db(str(tmp_path / "replay.db")), dict(SCHEMA))
    fresh.init()
    replay_history(fresh, exported)

    report = verify_replay(orig._db, fresh._db)
    print("重放比对:", report["counts"], "不一致项:", report["mismatches"])
    assert report["ok"], report["mismatches"]

    # 逐项显式核对（不只依赖汇总）
    assert report["counts"]["records"] == 4           # a/d/x/y 四条合法记录
    assert report["counts"]["rejections"] == 2        # 首批两条畸形记录；续传批不重复拒绝
    assert report["counts"]["schema_history"] == 3    # v1、v2、一次 rejected
    cps = {s: o for s, o in fresh._db.execute(
        "SELECT source, last_offset FROM checkpoints").fetchall()}
    assert cps == {"s1": 3, "s2": 1}
    versions = [v.version for v in fresh.sink.registry.versions()]
    assert versions == [1, 2]
    rejected = [e for e in fresh.sink.registry.history() if e.status == "rejected"]
    assert len(rejected) == 1 and "drop" in rejected[0].detail

    # 日志里能看到位点区间与结构变更记录
    messages = [r.getMessage() for r in caplog.records]
    assert any("offsets=" in m and "checkpoint=" in m for m in messages)
    assert any("schema change applied: version=2" in m for m in messages)
    assert any("schema change rejected" in m for m in messages)
