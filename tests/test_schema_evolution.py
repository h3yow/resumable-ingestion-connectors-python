"""目标端结构演进：加列兼容且缺省确定，删列/改名显式拒绝，流水可复现。"""
import json

import pytest

from app.connectors.jsonl import JsonlConnector
from app.models import ChangeKind
from app.pipeline import Pipeline, connect_db
from app.sink import SchemaEvolutionError

SCHEMA = {"id": str, "name": str, "age": int}


def _write(tmp_path, rows):
    path = tmp_path / "src.jsonl"
    path.write_text("\n".join(json.dumps(r) for r in rows) + "\n", encoding="utf-8")
    return str(path)


def test_add_column_with_deterministic_default(pipeline, tmp_path):
    src = _write(tmp_path, [
        {"id": "a", "name": "alice", "age": 30},
        {"id": "b", "name": "bob", "age": 40, "email": "b@x.com", "score": 9.5},
    ])
    result = pipeline.run_once(JsonlConnector(src, name="evo"))

    added = [(c.column, c.kind) for c in result.changes]
    # 首批记录建立 name/age 列，随后 email/score 作为演进新增列
    assert added == [
        ("name", ChangeKind.COLUMN_ADDED),
        ("age", ChangeKind.COLUMN_ADDED),
        ("email", ChangeKind.COLUMN_ADDED),
        ("score", ChangeKind.COLUMN_ADDED),
    ]

    rows = {r["record_id"]: r for r in pipeline.sink.fetch_all()}
    # 历史行读回新增列时得到确定性缺省值：str->''、float->0.0
    assert rows["evo#a"]["email"] == ""
    assert rows["evo#a"]["score"] == 0.0
    assert rows["evo#b"]["email"] == "b@x.com"
    assert rows["evo#b"]["score"] == 9.5
    print("变更流水:", [(c.seq, c.kind.value, c.column, c.detail)
                       for c in pipeline.sink.change_log()])


def test_drop_and_rename_rejected(pipeline):
    # 删列：显式拒绝并记录流水
    with pytest.raises(SchemaEvolutionError, match="drop"):
        pipeline.sink.apply_declared_schema({"id": str, "name": str})
    # 改名（删一列同时加一列）：显式拒绝并记录流水
    with pytest.raises(SchemaEvolutionError, match="rename"):
        pipeline.sink.apply_declared_schema(
            {"id": str, "fullname": str, "age": int}
        )
    # 加列：允许
    pipeline.sink.apply_declared_schema(
        {"id": str, "name": str, "age": int, "email": str}
    )

    kinds = [(c.kind, c.column) for c in pipeline.sink.change_log()]
    assert (ChangeKind.COLUMN_DROP_REJECTED, "age") in kinds
    assert (ChangeKind.COLUMN_RENAME_REJECTED, "name") in kinds
    # 被拒绝的演进未生效：声明 schema 仍是原字段加 email
    rows = pipeline._db.execute("SELECT name FROM declared_fields ORDER BY name").fetchall()
    assert [r[0] for r in rows] == ["age", "email", "id", "name"]
    print("变更流水:", [(c.seq, c.kind.value, c.column) for c in pipeline.sink.change_log()])


def test_change_log_reproducible_after_restart(tmp_path):
    db_path = str(tmp_path / "ingest.db")
    src = _write(tmp_path, [
        {"id": "a", "name": "alice", "age": 30, "tag": "x"},
    ])
    p1 = Pipeline(connect_db(db_path), dict(SCHEMA))
    p1.init()
    p1.run_once(JsonlConnector(src, name="s"))
    log1 = [(c.seq, c.kind.value, c.column, c.detail) for c in p1.sink.change_log()]

    # 重启后重放流水：结论完全一致（seq、类型、列、缺省语义）
    p2 = Pipeline(connect_db(db_path), dict(SCHEMA))
    p2.init()
    log2 = [(c.seq, c.kind.value, c.column, c.detail) for c in p2.sink.change_log()]
    assert log1 == log2
    assert any(kind == "column_added" and col == "tag" for _, kind, col, _ in log2)

    # 历史行缺省值在重启后依然稳定
    row = p2.sink.fetch_all()[0]
    assert row["tag"] == "x"
    print("重放流水:", log2)
