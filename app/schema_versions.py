"""结构版本：目标端声明式 schema 的版本化与历史留痕。

语义：
- 每次兼容的声明式结构变更产生一个新版本，版本号从 1 开始严格连续
  （不跳号），接在当前版本之后；
- 不兼容变更（删列/改名）被拒绝，拒绝尝试同样记入历史（version 为 NULL，
  status='rejected'），可审计；
- 与当前版本完全一致的重复声明是幂等 no-op，不产生新版本；
- 拿旧版本结构覆盖新版本等价于删列，按不兼容变更显式拒绝；
- 版本历史持久化在 SQLite，服务重启后按最新已生效版本继续运行。
"""
from __future__ import annotations

import json
import sqlite3
import time
from dataclasses import dataclass

_INIT_SQL = """
CREATE TABLE IF NOT EXISTS schema_history (
    seq INTEGER PRIMARY KEY AUTOINCREMENT,
    version INTEGER,
    status TEXT NOT NULL,          -- 'applied' | 'rejected'
    fields TEXT NOT NULL,          -- JSON: {字段名: 类型名}
    detail TEXT NOT NULL,
    at REAL NOT NULL
)
"""

STATUS_APPLIED = "applied"
STATUS_REJECTED = "rejected"

# 类型名 <-> Python 类型 的双向映射（持久化只存类型名）
TYPE_NAMES: dict[str, type] = {"str": str, "int": int, "float": float, "bool": bool}


def names_to_spec(fields: dict[str, str]) -> dict[str, type]:
    """把持久化的 {字段名: 类型名} 还原为归一化层使用的 FieldSpec。"""
    return {name: TYPE_NAMES[typ] for name, typ in fields.items()}


@dataclass(frozen=True)
class SchemaVersion:
    """一个已生效的结构版本。"""

    version: int
    fields: dict[str, str]         # 字段名 -> 类型名
    at: float = 0.0


@dataclass(frozen=True)
class SchemaHistoryEntry:
    """结构历史条目：含已生效版本与被拒绝的尝试。"""

    seq: int
    version: int | None            # rejected 条目为 None
    status: str
    fields: dict[str, str]
    detail: str
    at: float = 0.0


def _dump(fields: dict[str, str]) -> str:
    return json.dumps(fields, sort_keys=True)


class SchemaRegistry:
    """结构版本登记处：版本分配、历史查询、重启恢复。

    写方法（record_applied / record_rejected）须由调用方纳入其事务，
    与结构变更本身同生共死。
    """

    def __init__(self, db: sqlite3.Connection) -> None:
        self._db = db

    def init_schema(self) -> None:
        self._db.execute(_INIT_SQL)

    def current(self) -> SchemaVersion | None:
        """当前生效版本；尚无任何版本时返回 None。"""
        row = self._db.execute(
            "SELECT version, fields, at FROM schema_history "
            "WHERE status = ? ORDER BY version DESC LIMIT 1",
            (STATUS_APPLIED,),
        ).fetchone()
        if row is None:
            return None
        return SchemaVersion(version=row[0], fields=json.loads(row[1]), at=row[2])

    def versions(self) -> list[SchemaVersion]:
        """全部已生效版本（按版本号升序）。"""
        rows = self._db.execute(
            "SELECT version, fields, at FROM schema_history "
            "WHERE status = ? ORDER BY version",
            (STATUS_APPLIED,),
        ).fetchall()
        return [SchemaVersion(version=v, fields=json.loads(f), at=a) for v, f, a in rows]

    def history(self) -> list[SchemaHistoryEntry]:
        """完整历史（含被拒绝的尝试，按 seq 升序）。"""
        rows = self._db.execute(
            "SELECT seq, version, status, fields, detail, at "
            "FROM schema_history ORDER BY seq"
        ).fetchall()
        return [
            SchemaHistoryEntry(seq=s, version=v, status=st,
                               fields=json.loads(f), detail=d, at=a)
            for s, v, st, f, d, a in rows
        ]

    def record_applied(self, fields: dict[str, str], detail: str) -> SchemaVersion:
        """登记一个兼容变更产生的新版本（版本号 = 当前最大 + 1，不跳号）。"""
        current = self.current()
        version = 1 if current is None else current.version + 1
        at = time.time()
        self._db.execute(
            "INSERT INTO schema_history (version, status, fields, detail, at) "
            "VALUES (?, ?, ?, ?, ?)",
            (version, STATUS_APPLIED, _dump(fields), detail, at),
        )
        return SchemaVersion(version=version, fields=dict(fields), at=at)

    def record_rejected(self, fields: dict[str, str], detail: str) -> None:
        """登记一次被拒绝的不兼容变更尝试（version 为 NULL）。"""
        self._db.execute(
            "INSERT INTO schema_history (version, status, fields, detail, at) "
            "VALUES (NULL, ?, ?, ?, ?)",
            (STATUS_REJECTED, _dump(fields), detail, time.time()),
        )
