"""目标端：SQLite 落库，幂等写入 + 结构演进。

兼容规则（确定且稳定）：
- 新增列：允许。物理列缺省值按类型确定：str->''、int->0、float->0.0、
  bool->0、其他->NULL；历史行读回时恒定得到该缺省值；
- 删列：拒绝。记录 COLUMN_DROP_REJECTED 流水并抛 SchemaEvolutionError；
- 改名（同一次声明更新中删一列且加一列）：拒绝。记录
  COLUMN_RENAME_REJECTED 流水并抛 SchemaEvolutionError；
- 所有演进事件写入 change_log 流水，规则更新后历史结论可复现。

幂等：record_id 为主键，重复/并发写入走 UPSERT，不产生重复行。
"""
from __future__ import annotations

import json
import sqlite3
import time
from typing import Any

from app.models import ChangeEvent, ChangeKind, NormalizedRecord
from app.schema_versions import SchemaRegistry

TABLE = "records"

_INIT_SQL = [
    f'CREATE TABLE IF NOT EXISTS {TABLE} (record_id TEXT PRIMARY KEY)',
    """
    CREATE TABLE IF NOT EXISTS declared_fields (
        name TEXT PRIMARY KEY,
        type TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS change_log (
        seq INTEGER PRIMARY KEY AUTOINCREMENT,
        kind TEXT NOT NULL,
        column_name TEXT NOT NULL,
        detail TEXT NOT NULL,
        at REAL NOT NULL
    )
    """,
]

_TYPE_MAP = {str: ("TEXT", "''"), int: ("INTEGER", "0"), float: ("REAL", "0.0"), bool: ("INTEGER", "0")}


def _column_type(sample: Any) -> tuple[str, str | None]:
    """按样本值确定列类型与缺省值（确定性规则）。"""
    for py_type, (sql_type, default) in _TYPE_MAP.items():
        if isinstance(sample, py_type):
            return sql_type, default
    return "TEXT", None  # 其他类型 JSON 序列化存储，缺省 NULL


def _quote(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


class SchemaEvolutionError(Exception):
    """删列或改名等不兼容演进被显式拒绝。"""


class Sink:
    def __init__(self, db: sqlite3.Connection) -> None:
        self._db = db
        self.registry = SchemaRegistry(db)

    def init_schema(self) -> None:
        for sql in _INIT_SQL:
            self._db.execute(sql)
        self.registry.init_schema()

    # ---- 结构演进 ----

    def _existing_columns(self) -> list[str]:
        rows = self._db.execute(f"PRAGMA table_info({_quote(TABLE)})").fetchall()
        return [r[1] for r in rows]

    def _log_change(self, kind: ChangeKind, column: str, detail: str) -> ChangeEvent:
        at = time.time()
        cur = self._db.execute(
            "INSERT INTO change_log (kind, column_name, detail, at) VALUES (?, ?, ?, ?)",
            (kind.value, column, detail, at),
        )
        return ChangeEvent(kind=kind, column=column, detail=detail, seq=cur.lastrowid, at=at)

    def _add_column(self, name: str, sample: Any) -> ChangeEvent:
        sql_type, default = _column_type(sample)
        ddl = f"ALTER TABLE {TABLE} ADD COLUMN {_quote(name)} {sql_type}"
        if default is not None:
            ddl += f" DEFAULT {default}"
        self._db.execute(ddl)
        return self._log_change(
            ChangeKind.COLUMN_ADDED, name,
            f"added column {name} {sql_type} default {default}",
        )

    def ensure_columns(self, fields: dict[str, Any]) -> list[ChangeEvent]:
        """按记录字段演进目标表：仅新增列；返回新增列流水。"""
        existing = set(self._existing_columns())
        events: list[ChangeEvent] = []
        for name, value in fields.items():
            if name not in existing:
                events.append(self._add_column(name, value))
                existing.add(name)
        return events

    def apply_declared_schema(self, spec: dict[str, type]) -> list[ChangeEvent]:
        """应用声明式逻辑 schema：加列兼容，删列/改名显式拒绝。

        版本语义：
        - 与当前生效版本完全一致：幂等 no-op，不产生新版本；
        - 仅新增字段：兼容，登记为新版本（版本号接在当前版本之后）；
        - 删字段/改名（含拿旧版本覆盖新版本）：显式拒绝，拒绝尝试记入
          结构历史，当前版本与数据不受影响。
        """
        rows = self._db.execute("SELECT name, type FROM declared_fields").fetchall()
        declared = {name: typ for name, typ in rows}
        incoming = {name: typ.__name__ for name, typ in spec.items()}

        if incoming == declared and declared:
            return []  # 重复声明当前版本：幂等 no-op

        removed = set(declared) - set(incoming)
        added = set(incoming) - set(declared)
        if removed:
            if added:
                for name in sorted(removed):
                    self._log_change(
                        ChangeKind.COLUMN_RENAME_REJECTED, name,
                        f"rename {name} -> {sorted(added)} rejected",
                    )
                detail = f"column rename rejected: {sorted(removed)} -> {sorted(added)}"
                self.registry.record_rejected(incoming, detail)
                raise SchemaEvolutionError(detail)
            for name in sorted(removed):
                self._log_change(
                    ChangeKind.COLUMN_DROP_REJECTED, name,
                    f"drop column {name} rejected",
                )
            detail = f"column drop rejected: {sorted(removed)}"
            self.registry.record_rejected(incoming, detail)
            raise SchemaEvolutionError(detail)

        events: list[ChangeEvent] = []
        for name, typ in incoming.items():
            if name not in declared:
                self._db.execute(
                    "INSERT INTO declared_fields (name, type) VALUES (?, ?)",
                    (name, typ),
                )
        self.registry.record_applied(
            incoming, f"applied schema version with added columns: {sorted(added)}"
        )
        return events

    # ---- 写入与读取 ----

    def upsert(self, records: list[NormalizedRecord]) -> int:
        """幂等写入：同一 record_id 重复写入不产生重复行。返回写入条数。"""
        count = 0
        for rec in records:
            self.ensure_columns(rec.fields)
            columns = ["record_id", *rec.fields.keys()]
            values: list[Any] = [rec.record_id]
            for v in rec.fields.values():
                values.append(v if isinstance(v, (str, int, float, bool)) or v is None
                              else json.dumps(v, ensure_ascii=False))
            placeholders = ", ".join("?" for _ in columns)
            updates = ", ".join(
                f"{_quote(c)} = excluded.{_quote(c)}" for c in columns if c != "record_id"
            )
            sql = (
                f"INSERT INTO {TABLE} ({', '.join(_quote(c) for c in columns)}) "
                f"VALUES ({placeholders})"
            )
            sql += f" ON CONFLICT(record_id) DO UPDATE SET {updates}" if updates else \
                   " ON CONFLICT(record_id) DO NOTHING"
            self._db.execute(sql, values)
            count += 1
        return count

    def change_log(self) -> list[ChangeEvent]:
        """结构演进流水（按 seq 升序，可复现）。"""
        rows = self._db.execute(
            "SELECT seq, kind, column_name, detail, at FROM change_log ORDER BY seq"
        ).fetchall()
        return [
            ChangeEvent(kind=ChangeKind(k), column=c, detail=d, seq=s, at=a)
            for s, k, c, d, a in rows
        ]

    def fetch_all(self) -> list[dict[str, Any]]:
        cur = self._db.execute(f"SELECT * FROM {TABLE} ORDER BY record_id")
        names = [d[0] for d in cur.description]
        return [dict(zip(names, row)) for row in cur.fetchall()]
