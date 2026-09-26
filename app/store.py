"""目标端存储：SQLite 落库 + 位点/检查点管理 + 变更流水。

幂等语义：
- 数据表以 (source, record_id) 为主键，写入用 INSERT OR IGNORE，
  重复消费 / 并发消费不会产生重复行。
- 位点推进与数据写入在同一事务提交，重启后不丢不重。
- 位点回退（end_position < 当前检查点）抛出 PositionRollbackError，
  绝不从中间继续。
"""
from __future__ import annotations

import json
import sqlite3
import threading
from datetime import datetime, timezone
from pathlib import Path

from app.models import NormalizedRecord, RejectedRecord
from app.schema import Column, ColumnType, SchemaError, TableSchema, evolve_schema

DATA_TABLE = "records"

# 归一化记录 -> 目标列的固定映射（schema v1）
BASE_COLUMNS: tuple[Column, ...] = (
    Column("record_id", ColumnType.TEXT, required=True),
    Column("occurred_at", ColumnType.TEXT, required=True),
    Column("name", ColumnType.TEXT, required=True),
    Column("value", ColumnType.REAL, required=True),
    Column("tags", ColumnType.TEXT, default=""),
)

SCHEMA_V1 = TableSchema(version=1, columns=BASE_COLUMNS)


class PositionRollbackError(Exception):
    """位点回退被明确拒绝。"""


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _default_literal(col: Column) -> str:
    """把新增列的缺省值渲染为 SQL 字面量（确定且稳定）。"""
    if col.default is None:
        return "NULL"
    if col.type == ColumnType.TEXT:
        return "'" + str(col.default).replace("'", "''") + "'"
    if col.type in (ColumnType.REAL, ColumnType.INTEGER):
        if isinstance(col.default, bool) or not isinstance(col.default, (int, float)):
            raise ValueError(f"bad default for {col.name}: {col.default!r}")
        return repr(col.default)
    raise ValueError(f"unsupported column type: {col.type}")


class TargetStore:
    """SQLite 目标端：数据表 + 检查点表 + 拒绝记录表 + 变更流水表。"""

    def __init__(self, db_path: str | Path, schema: TableSchema = SCHEMA_V1) -> None:
        self.db_path = str(db_path)
        self.schema = schema
        self._lock = threading.RLock()
        self._conn = sqlite3.connect(self.db_path, check_same_thread=False)
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA busy_timeout=5000")
        self._init_tables()

    # ---------- 建表 ----------

    def _init_tables(self) -> None:
        cols = ", ".join(
            f"{c.name} {c.type.value.upper()}"
            + (" NOT NULL" if c.required else "")
            + ("" if c.default is None else f" DEFAULT {_default_literal(c)}")
            for c in self.schema.columns
        )
        with self._conn:
            self._conn.execute(
                f"CREATE TABLE IF NOT EXISTS {DATA_TABLE} ("
                f"source TEXT NOT NULL, position INTEGER NOT NULL, {cols}, "
                f"PRIMARY KEY (source, record_id))"
            )
            self._conn.execute(
                "CREATE TABLE IF NOT EXISTS checkpoints ("
                "source TEXT PRIMARY KEY, position INTEGER NOT NULL, "
                "updated_at TEXT NOT NULL)"
            )
            self._conn.execute(
                "CREATE TABLE IF NOT EXISTS rejects ("
                "source TEXT NOT NULL, position INTEGER NOT NULL, "
                "reason TEXT NOT NULL, detail TEXT NOT NULL, payload TEXT NOT NULL, "
                "recorded_at TEXT NOT NULL, PRIMARY KEY (source, position))"
            )
            self._conn.execute(
                "CREATE TABLE IF NOT EXISTS changelog ("
                "id INTEGER PRIMARY KEY AUTOINCREMENT, ts TEXT NOT NULL, "
                "kind TEXT NOT NULL, detail TEXT NOT NULL)"
            )
            self._conn.execute(
                "CREATE TABLE IF NOT EXISTS schema_versions ("
                "version INTEGER PRIMARY KEY, applied_at TEXT NOT NULL, "
                "columns TEXT NOT NULL)"
            )
            cur = self._conn.execute("SELECT COUNT(*) FROM schema_versions")
            if cur.fetchone()[0] == 0:
                self._conn.execute(
                    "INSERT INTO schema_versions VALUES (?, ?, ?)",
                    (self.schema.version, _now(),
                     json.dumps(list(self.schema.column_names()))),
                )

    def _log(self, kind: str, detail: dict) -> None:
        self._conn.execute(
            "INSERT INTO changelog (ts, kind, detail) VALUES (?, ?, ?)",
            (_now(), kind, json.dumps(detail, ensure_ascii=False)),
        )

    # ---------- 检查点 ----------

    def get_checkpoint(self, source: str) -> int:
        with self._lock:
            cur = self._conn.execute(
                "SELECT position FROM checkpoints WHERE source = ?", (source,)
            )
            row = cur.fetchone()
            return int(row[0]) if row else 0

    # ---------- 幂等落库 ----------

    def commit_batch(
        self,
        source: str,
        records: list[NormalizedRecord],
        rejects: list[RejectedRecord],
        end_position: int,
    ) -> int:
        """单事务写入：数据 + 拒绝记录 + 位点推进。返回实际插入条数。"""
        with self._lock:
            current = self.get_checkpoint(source)
            if end_position < current:
                raise PositionRollbackError(
                    f"position rollback rejected for {source}: "
                    f"{end_position} < checkpoint {current}"
                )
            col_names = [c.name for c in self.schema.columns]
            inserted = 0
            with self._conn:  # 事务：失败整体回滚
                for rec in records:
                    values = {
                        "record_id": rec.record_id,
                        "occurred_at": rec.occurred_at,
                        "name": rec.name,
                        "value": rec.value,
                        "tags": ";".join(rec.tags),
                    }
                    # 结构演进后可能出现新列：记录未提供的列一律写缺省值
                    row = [values.get(name, self.schema.column(name).default)
                           for name in col_names]
                    cur = self._conn.execute(
                        f"INSERT OR IGNORE INTO {DATA_TABLE} "
                        f"(source, position, {', '.join(col_names)}) "
                        f"VALUES (?, ?, {', '.join('?' * len(col_names))})",
                        (rec.source, rec.position, *row),
                    )
                    inserted += cur.rowcount
                for rej in rejects:
                    self._conn.execute(
                        "INSERT OR REPLACE INTO rejects "
                        "(source, position, reason, detail, payload, recorded_at) "
                        "VALUES (?, ?, ?, ?, ?, ?)",
                        (rej.source, rej.position, rej.reason.value, rej.detail,
                         json.dumps(rej.payload, ensure_ascii=False), _now()),
                    )
                self._conn.execute(
                    "INSERT INTO checkpoints (source, position, updated_at) "
                    "VALUES (?, ?, ?) "
                    "ON CONFLICT(source) DO UPDATE SET "
                    "position=excluded.position, updated_at=excluded.updated_at",
                    (source, end_position, _now()),
                )
                self._log("commit", {
                    "source": source, "from": current, "to": end_position,
                    "inserted": inserted, "rejected": len(rejects),
                })
            return inserted

    # ---------- 结构演进 ----------

    def evolve(self, new_schema: TableSchema) -> None:
        """应用结构演进（仅新增列），并记录变更流水。"""
        with self._lock:
            evolve_schema(self.schema, new_schema)  # 校验，非法即抛 SchemaError
            old_names = set(self.schema.column_names())
            added = [c for c in new_schema.columns if c.name not in old_names]
            if not added:
                raise SchemaError("evolution must add at least one column")
            with self._conn:
                for col in added:
                    self._conn.execute(
                        f"ALTER TABLE {DATA_TABLE} ADD COLUMN "
                        f"{col.name} {col.type.value.upper()} "
                        f"DEFAULT {_default_literal(col)}"
                    )
                self._conn.execute(
                    "INSERT INTO schema_versions VALUES (?, ?, ?)",
                    (new_schema.version, _now(),
                     json.dumps(list(new_schema.column_names()))),
                )
                self._log("schema_evolve", {
                    "from": self.schema.version, "to": new_schema.version,
                    "added": [c.name for c in added],
                })
            self.schema = new_schema

    # ---------- 查询（测试与排障用） ----------

    def count_rows(self) -> int:
        with self._lock:
            cur = self._conn.execute(f"SELECT COUNT(*) FROM {DATA_TABLE}")
            return int(cur.fetchone()[0])

    def fetch_all(self) -> list[dict]:
        names = ["source", "position", *self.schema.column_names()]
        with self._lock:
            cur = self._conn.execute(
                f"SELECT {', '.join(names)} FROM {DATA_TABLE} ORDER BY source, position"
            )
            return [dict(zip(names, row)) for row in cur.fetchall()]

    def list_rejects(self) -> list[dict]:
        with self._lock:
            cur = self._conn.execute(
                "SELECT source, position, reason, detail FROM rejects "
                "ORDER BY source, position"
            )
            return [dict(zip(("source", "position", "reason", "detail"), row))
                    for row in cur.fetchall()]

    def changelog(self) -> list[dict]:
        with self._lock:
            cur = self._conn.execute(
                "SELECT id, ts, kind, detail FROM changelog ORDER BY id"
            )
            return [dict(zip(("id", "ts", "kind", "detail"), row))
                    for row in cur.fetchall()]

    def close(self) -> None:
        self._conn.close()
