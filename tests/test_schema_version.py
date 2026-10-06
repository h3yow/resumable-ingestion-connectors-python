"""结构版本：版本递增不跳号、历史可查、拒绝留痕、重启后继续按最新版本运行。"""
import json

import pytest

from app.connectors.jsonl import JsonlConnector
from app.models import SchemaStatus
from app.pipeline import Pipeline, connect_db
from app.schema_version import StaleSchemaVersionError
from app.sink import SchemaEvolutionError

SCHEMA = {"id": str, "name": str, "age": int}


def test_versions_increment_without_gaps(pipeline):
    assert pipeline.sink.registry.current_version() == 1  # init 建立 v1

    e2 = pipeline.apply_schema({**SCHEMA, "email": str})
    e3 = pipeline.apply_schema({**SCHEMA, "email": str, "score": float})
    assert (e2.version, e3.version) == (2, 3)  # 接在当前版本后面，不跳号

    history = pipeline.sink.registry.history()
    applied = [e for e in history if e.status is SchemaStatus.APPLIED]
    assert [e.version for e in applied] == [1, 2, 3]
    # 每个版本具体改了什么可查
    assert "email" in applied[1].detail
    assert applied[2].fields == {
        "id": "str", "name": "str", "age": "int", "email": "str", "score": "float",
    }
    # 当前生效版本可查
    assert pipeline.sink.registry.current_fields() == applied[2].fields
    print("结构历史:", [(e.version, e.status.value, e.detail) for e in history])


def test_rejected_attempts_are_recorded(pipeline):
    # 不兼容变更（删列）：明确拒绝且留痕
    with pytest.raises(SchemaEvolutionError, match="drop"):
        pipeline.apply_schema({"id": str, "name": str})
    # 拿过期基线覆盖新版本：明确拒绝且留痕
    pipeline.apply_schema({**SCHEMA, "email": str})  # v2
    with pytest.raises(StaleSchemaVersionError, match="stale"):
        pipeline.apply_schema({**SCHEMA, "nick": str}, base_version=1)

    history = pipeline.sink.registry.history()
    rejected = [e for e in history if e.status is SchemaStatus.REJECTED]
    assert len(rejected) == 2
    assert all(e.version is None for e in rejected)  # 被拒绝不分配版本号
    assert "drop" in rejected[0].detail
    assert "stale" in rejected[1].detail
    # 版本号没有被拒绝尝试消耗，当前版本仍是 v2
    assert pipeline.sink.registry.current_version() == 2
    applied_versions = [e.version for e in history if e.status is SchemaStatus.APPLIED]
    assert applied_versions == [1, 2]
    print("拒绝留痕:", [(e.seq, e.detail) for e in rejected])


def test_restart_keeps_latest_version(tmp_path):
    db_path = str(tmp_path / "ingest.db")
    src = tmp_path / "src.jsonl"
    src.write_text(
        json.dumps({"id": "a", "name": "alice", "age": 30, "email": "a@x.com"}) + "\n"
        + json.dumps({"id": "b", "name": "bob", "age": 40}) + "\n",  # 缺 email
        encoding="utf-8",
    )

    p1 = Pipeline(connect_db(db_path), dict(SCHEMA))
    p1.init()
    p1.apply_schema({**SCHEMA, "email": str})  # v2：email 成为必填声明字段

    # 重启：新连接、新 Pipeline，继续按最新已生效版本运行
    p2 = Pipeline(connect_db(db_path), dict(SCHEMA))
    p2.init()
    assert p2.sink.registry.current_version() == 2
    assert p2._schema == {**SCHEMA, "email": str}  # 不退回旧结构

    # 按 v2 摄入：email 缺失的记录被明确拒绝，合法记录正常落库
    result = p2.run_once(JsonlConnector(str(src), name="s"))
    assert result.accepted == 1
    assert [r.reason.value for r in result.rejected] == ["missing_field"]
    assert p2.checkpoints.get("s").last_offset == 1

    # 重启后拿旧版本结构覆盖：按删列规则明确拒绝，版本保持 v2
    with pytest.raises(SchemaEvolutionError, match="drop"):
        p2.apply_schema(dict(SCHEMA))
    assert p2.sink.registry.current_version() == 2
    # 重启后新的兼容变更接在 v2 后面
    e3 = p2.apply_schema({**SCHEMA, "email": str, "score": float})
    assert e3.version == 3
    print("重启后位点:", p2.checkpoints.get("s").last_offset,
          "版本:", p2.sink.registry.current_version(),
          "历史:", [(e.version, e.status.value) for e in p2.sink.registry.history()])


def test_failed_change_rolls_back_entirely(pipeline, tmp_path, monkeypatch):
    src = tmp_path / "src.jsonl"
    src.write_text(json.dumps({"id": "a", "name": "alice", "age": 30}) + "\n",
                   encoding="utf-8")
    pipeline.run_once(JsonlConnector(str(src), name="s"))
    before_cp = pipeline.checkpoints.get("s").last_offset
    before_rows = len(pipeline.sink.fetch_all())

    # 变更中途失败（模拟存储异常）：整体不生效，不留半提交状态
    def boom(*args, **kwargs):
        raise RuntimeError("simulated storage failure")

    monkeypatch.setattr(pipeline.sink.registry, "record_applied", boom)
    with pytest.raises(RuntimeError, match="simulated"):
        pipeline.apply_schema({**SCHEMA, "email": str})

    assert pipeline.sink.registry.current_version() == 1
    assert pipeline.sink.registry.current_fields() == {
        k: v.__name__ for k, v in SCHEMA.items()
    }
    # 位点和数据停在变更前
    assert pipeline.checkpoints.get("s").last_offset == before_cp
    assert len(pipeline.sink.fetch_all()) == before_rows
    # 声明字段未留下半提交的列
    declared = {r[0] for r in pipeline._db.execute(
        "SELECT name FROM declared_fields").fetchall()}
    assert declared == set(SCHEMA)
    # 失败未污染历史：没有 applied 的 v2，也没有半条 rejected 记录
    assert [e.version for e in pipeline.sink.registry.history()
            if e.status is SchemaStatus.APPLIED] == [1]
    print("变更失败整体回滚: 版本=v%d 位点=%d 行数=%d" % (
        pipeline.sink.registry.current_version(), before_cp, before_rows,
    ))
