"""结构版本：版本连续可查、重启后按最新版本运行、被拒尝试可审计。"""
import json

import pytest

from app.connectors.jsonl import JsonlConnector
from app.pipeline import Pipeline, connect_db
from app.sink import SchemaEvolutionError

SCHEMA = {"id": str, "name": str, "age": int}


def _write(tmp_path, rows, name="src.jsonl"):
    path = tmp_path / name
    path.write_text("\n".join(json.dumps(r) for r in rows) + "\n", encoding="utf-8")
    return str(path)


def test_versions_sequential_and_queryable(pipeline):
    registry = pipeline.sink.registry
    # init 建立版本 1
    assert registry.current().version == 1
    assert registry.current().fields == {"id": "str", "name": "str", "age": "int"}

    # 每次兼容变更产生一个新版本，严格接续、不跳号
    v2 = pipeline.apply_schema({**SCHEMA, "email": str})
    v3 = pipeline.apply_schema({**SCHEMA, "email": str, "score": float})
    assert (v2.version, v3.version) == (2, 3)
    assert [v.version for v in registry.versions()] == [1, 2, 3]
    assert registry.current().version == 3

    # 每个版本具体改了什么可查：v2 多 email，v3 多 score
    v1, v2f, v3f = [v.fields for v in registry.versions()]
    assert set(v2f) - set(v1) == {"email"}
    assert set(v3f) - set(v2f) == {"score"}

    # 重复声明当前版本：幂等 no-op，不产生新版本
    pipeline.apply_schema({**SCHEMA, "email": str, "score": float})
    assert [v.version for v in registry.versions()] == [1, 2, 3]
    print("版本历史:", [(e.seq, e.version, e.status, e.detail)
                       for e in registry.history()])


def test_restart_continues_at_latest_version(tmp_path):
    db_path = str(tmp_path / "ingest.db")
    p1 = Pipeline(connect_db(db_path), dict(SCHEMA))
    p1.init()
    p1.apply_schema({**SCHEMA, "email": str})  # 生效版本 2
    src = _write(tmp_path, [{"id": "a", "name": "alice", "age": 30, "email": "a@x"}])
    p1.run_once(JsonlConnector(src, name="s"))
    p1._db.close()

    # 重启：即便配置的是旧结构，也必须按最新已生效版本（v2）运行
    p2 = Pipeline(connect_db(db_path), dict(SCHEMA))
    p2.init()
    assert p2.sink.registry.current().version == 2
    assert p2._schema == {**SCHEMA, "email": str}  # 归一化按 v2 校验
    assert [v.version for v in p2.sink.registry.versions()] == [1, 2]  # 未产生新版本

    # 按 v2 校验：缺 email 的记录被明确拒绝，位点照常推进
    src2 = _write(tmp_path, [{"id": "b", "name": "bob", "age": 40}], name="src2.jsonl")
    result = p2.run_once(JsonlConnector(src2, name="s2"))
    assert result.accepted == 0
    assert [r.reason.value for r in result.rejected] == ["missing_field"]
    print("重启后版本:", p2.sink.registry.current().version,
          "位点:", result.checkpoint.last_offset)


def test_rejected_attempts_auditable_and_old_version_cannot_override(pipeline):
    registry = pipeline.sink.registry
    pipeline.apply_schema({**SCHEMA, "email": str})  # v2

    # 不兼容变更：删列被拒
    with pytest.raises(SchemaEvolutionError, match="drop"):
        pipeline.apply_schema({"id": str, "name": str})
    # 拿旧版本（v1）结构覆盖当前版本：等价删列，同样被拒，不能算成功
    with pytest.raises(SchemaEvolutionError):
        pipeline.apply_schema(dict(SCHEMA))

    # 当前版本未被破坏
    assert registry.current().version == 2
    assert registry.current().fields["email"] == "str"

    # 被拒尝试在历史里查得到：version 为 NULL、status=rejected、含原因
    rejected = [e for e in registry.history() if e.status == "rejected"]
    assert len(rejected) == 2
    assert all(e.version is None for e in rejected)
    assert "drop" in rejected[0].detail
    assert rejected[0].fields == {"id": "str", "name": "str"}
    print("结构历史:", [(e.seq, e.version, e.status) for e in registry.history()])
