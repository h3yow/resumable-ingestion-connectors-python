"""结构版本注册表：版本号、历史留痕、旧版本覆盖拒绝。

语义：
- 每次兼容变更（新增列）产生一个新版本，从 1 开始严格递增、不跳号；
- 不兼容变更（删列/改名）与旧版本覆盖被显式拒绝，拒绝尝试也记入历史；
- 重启后从库中加载最新已生效版本继续运行，不退回旧结构；
- 调用方负责在事务内调用 apply（Pipeline.apply_schema 用 BEGIN IMMEDIATE），
  保证版本分配与结构元数据更新原子生效。
"""
from __future__ import annotations

import json
import sqlite3
import time

from app.models import SchemaStatus, SchemaVersion

_INIT_SQL = """
CREATE TABLE IF NOT EXISTS schema_history (
    seq INTEGER PRIMARY KEY AUTOINCREMENT,
    version INTEGER,
    status TEXT NOT NULL,
    fields TEXT NOT NULL,
    detail TEXT NOT NULL,
    at REAL NOT NULL
)
"""


class StaleSchemaVersionError(Exception):
    """拿旧版本结构覆盖当前更新版本的尝试被显式拒绝。"""


class SchemaRegistry:
    def __init__(self, db: sqlite3.Connection) -> None:
        self._db = db

    def init_schema(self) -> None:
        self._db.execute(_INIT_SQL)

    # ---- 查询 ----

    def current_version(self) -> int:
        """当前已生效的最新版本号；尚无版本时返回 0。"""
        row = self._db.execute(
            "SELECT MAX(version) FROM schema_history WHERE status = ?",
            (SchemaStatus.APPLIED.value,),
        ).fetchone()
        return row[0] or 0

    def current_fields(self) -> dict[str, str] | None:
        """当前已生效版本的字段声明（字段名 -> 类型名）；无版本时返回 None。"""
        row = self._db.execute(
            "SELECT fields FROM schema_history WHERE status = ? "
            "ORDER BY version DESC LIMIT 1",
            (SchemaStatus.APPLIED.value,),
        ).fetchone()
        return json.loads(row[0]) if row else None

    def history(self) -> list[SchemaVersion]:
        """全部历史条目（含被拒绝的尝试），按发生顺序排列。"""
        rows = self._db.execute(
            "SELECT seq, version, status, fields, detail, at "
            "FROM schema_history ORDER BY seq"
        ).fetchall()
        return [
            SchemaVersion(
                seq=seq, version=ver, status=SchemaStatus(status),
                fields=json.loads(fields), detail=detail, at=at,
            )
            for seq, ver, status, fields, detail, at in rows
        ]

    # ---- 变更 ----

    def _record(self, status: SchemaStatus, fields: dict[str, str],
                detail: str, version: int | None = None) -> SchemaVersion:
        at = time.time()
        cur = self._db.execute(
            "INSERT INTO schema_history (version, status, fields, detail, at) "
            "VALUES (?, ?, ?, ?, ?)",
            (version, status.value, json.dumps(fields, sort_keys=True), detail, at),
        )
        return SchemaVersion(seq=cur.lastrowid, version=version, status=status,
                             fields=fields, detail=detail, at=at)

    def record_applied(self, fields: dict[str, str], detail: str) -> SchemaVersion:
        """登记一次已生效的兼容变更：版本号接在当前版本之后，不跳号。"""
        version = self.current_version() + 1
        return self._record(SchemaStatus.APPLIED, fields, detail, version=version)

    def record_rejected(self, fields: dict[str, str], detail: str) -> SchemaVersion:
        """登记一次被拒绝的变更尝试（不分配版本号，仅留痕）。"""
        return self._record(SchemaStatus.REJECTED, fields, detail)
