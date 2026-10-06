"""历史操作流水：摄入与结构变更按真实发生顺序落库，可导出并在干净实例重放。

- 每次摄入批次（含被拒绝记录）与每次结构变更尝试（含被拒绝的）各记一条，
  seq 单调递增即真实发生顺序；
- 摄入操作与批次落库在同事务，结构变更操作与版本历史在同事务，
  流水与数据状态始终一致，崩溃不留半提交；
- 导出的 payload 不含时间戳等不确定字段，重放后可逐项比对。
"""
from __future__ import annotations

import json
import sqlite3
import time

from app.models import OpKind, OpRecord

_INIT_SQL = """
CREATE TABLE IF NOT EXISTS op_log (
    seq INTEGER PRIMARY KEY AUTOINCREMENT,
    kind TEXT NOT NULL,
    payload TEXT NOT NULL,
    at REAL NOT NULL
)
"""


class HistoryRecorder:
    def __init__(self, db: sqlite3.Connection) -> None:
        self._db = db

    def init_schema(self) -> None:
        self._db.execute(_INIT_SQL)

    def record(self, kind: OpKind, payload: dict) -> OpRecord:
        """追加一条操作流水；调用方负责把它包在对应业务事务内。"""
        at = time.time()
        cur = self._db.execute(
            "INSERT INTO op_log (kind, payload, at) VALUES (?, ?, ?)",
            (kind.value, json.dumps(payload, sort_keys=True), at),
        )
        return OpRecord(seq=cur.lastrowid, kind=kind, payload=payload, at=at)

    def export(self) -> list[OpRecord]:
        """按真实发生顺序导出全部历史操作。"""
        rows = self._db.execute(
            "SELECT seq, kind, payload, at FROM op_log ORDER BY seq"
        ).fetchall()
        return [
            OpRecord(seq=seq, kind=OpKind(kind), payload=json.loads(payload), at=at)
            for seq, kind, payload, at in rows
        ]
