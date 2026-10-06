"""历史操作导出与重放：摄入与结构变更按真实发生顺序落账、导出、重放、比对。

语义：
- op_log 表按 seq 记录两类操作：ingest（一个批次消费到的原始记录）与
  schema（一次声明式结构变更及其结果），与对应的状态变更同事务提交；
- export_history 导出为可 JSON 序列化的结构，可落盘搬运到干净环境；
- replay_history 在全新实例上按 seq 顺序原样重放：ingest 操作用记录的
  原始数据经完整流水线（归一化 -> 演进 -> 落库 -> 位点）重跑，schema
  操作重新走兼容校验（被拒的尝试在重放时必须同样被拒）；
- verify_replay 逐项比对两个实例：数据行、各来源位点、拒绝记录、
  结构演进流水、结构版本历史与操作流水本身。

注意：导出内容即重放的全部输入，不依赖原来源文件仍然存在。
记录 payload 须可 JSON 序列化（本地 JSONL/CSV 连接器均满足）。
"""
from __future__ import annotations

import json
import sqlite3
import time
from typing import Any, Iterator

from app.models import RawRecord
from app.schema_versions import names_to_spec
from app.sink import SchemaEvolutionError

_INIT_SQL = """
CREATE TABLE IF NOT EXISTS op_log (
    seq INTEGER PRIMARY KEY AUTOINCREMENT,
    kind TEXT NOT NULL,            -- 'ingest' | 'schema'
    payload TEXT NOT NULL,         -- JSON
    at REAL NOT NULL
)
"""

KIND_INGEST = "ingest"
KIND_SCHEMA = "schema"


def init_op_log(db: sqlite3.Connection) -> None:
    db.execute(_INIT_SQL)


def record_op(db: sqlite3.Connection, kind: str, payload: dict[str, Any]) -> int:
    """追加一条操作流水，返回 seq。须由调用方纳入其事务。"""
    cur = db.execute(
        "INSERT INTO op_log (kind, payload, at) VALUES (?, ?, ?)",
        (kind, json.dumps(payload, ensure_ascii=False, default=str), time.time()),
    )
    return cur.lastrowid


def export_history(db: sqlite3.Connection) -> dict[str, Any]:
    """导出全部操作流水（按 seq 升序）为可 JSON 序列化结构。"""
    rows = db.execute(
        "SELECT seq, kind, payload FROM op_log ORDER BY seq"
    ).fetchall()
    return {
        "ops": [
            {"seq": seq, "kind": kind, "payload": json.loads(payload)}
            for seq, kind, payload in rows
        ]
    }


class ReplayConnector:
    """重放连接器：按导出记录中的原始数据产出，offset 语义与来源一致。"""

    def __init__(self, name: str, raws: list[RawRecord]) -> None:
        self._name = name
        self._raws = raws

    @property
    def name(self) -> str:
        return self._name

    def read(self, from_offset: int) -> Iterator[RawRecord]:
        for raw in self._raws:
            if raw.offset > from_offset:
                yield raw


def replay_history(pipeline: Any, export: dict[str, Any]) -> None:
    """在 pipeline 指向的全新实例上按序重放导出的操作。

    重放结果与记录的结果不一致（如本应被拒的结构变更竟然生效）时
    抛 ReplayMismatchError。
    """
    for op in export["ops"]:
        kind, payload = op["kind"], op["payload"]
        if kind == KIND_INGEST:
            raws = [
                RawRecord(source=payload["source"], offset=r["offset"],
                          payload=r["payload"])
                for r in payload["raws"]
            ]
            connector = ReplayConnector(payload["source"], raws)
            pipeline.run_once(connector, batch_size=len(raws))
        elif kind == KIND_SCHEMA:
            spec = names_to_spec(payload["fields"])
            try:
                pipeline.apply_schema(spec)
                outcome = "applied"
            except SchemaEvolutionError:
                outcome = "rejected"
            if outcome != payload["result"]:
                raise ReplayMismatchError(
                    f"op #{op['seq']}: schema op outcome {outcome} != "
                    f"recorded {payload['result']}"
                )
        else:
            raise ReplayMismatchError(f"op #{op['seq']}: unknown kind {kind!r}")


class ReplayMismatchError(Exception):
    """重放结果与导出记录不一致。"""


def _rows(db: sqlite3.Connection, sql: str) -> list[tuple]:
    return db.execute(sql).fetchall()


def verify_replay(
    db_orig: sqlite3.Connection, db_replay: sqlite3.Connection
) -> dict[str, Any]:
    """逐项比对原实例与重放实例，返回比对报告（mismatches 为空即一致）。"""
    checks: dict[str, tuple[str, str]] = {
        "records": (
            f"SELECT * FROM records ORDER BY record_id",
            "数据行（主键升序全量比对）",
        ),
        "checkpoints": (
            "SELECT source, last_offset FROM checkpoints ORDER BY source",
            "各来源位点",
        ),
        "rejections": (
            "SELECT source, offset, reason, detail FROM rejections "
            "ORDER BY source, offset",
            "拒绝记录",
        ),
        "change_log": (
            "SELECT seq, kind, column_name, detail FROM change_log ORDER BY seq",
            "结构演进流水",
        ),
        "schema_history": (
            "SELECT seq, version, status, fields, detail FROM schema_history "
            "ORDER BY seq",
            "结构版本历史",
        ),
        "op_log": (
            "SELECT seq, kind, payload FROM op_log ORDER BY seq",
            "操作流水",
        ),
    }
    mismatches: list[str] = []
    counts: dict[str, int] = {}
    for name, (sql, label) in checks.items():
        a, b = _rows(db_orig, sql), _rows(db_replay, sql)
        counts[name] = len(a)
        if a != b:
            mismatches.append(f"{label}({name}): 原 {len(a)} 条 vs 重放 {len(b)} 条")
    return {"ok": not mismatches, "mismatches": mismatches, "counts": counts}
