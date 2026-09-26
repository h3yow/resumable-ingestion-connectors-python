"""位点/检查点存储：SQLite 持久化，拒绝位点回退。

语义：
- 每个 source 一条位点记录，last_offset 为已确认落库的最大 offset；
- commit 要求新位点严格大于当前位点，否则抛 OffsetRegressionError，
  绝不"从中间继续"；
- 写入与落库在同一事务中提交（由 Pipeline 保证），重启后从位点续传。
"""
from __future__ import annotations

import sqlite3
import time

from app.models import Checkpoint

_INIT_SQL = """
CREATE TABLE IF NOT EXISTS checkpoints (
    source TEXT PRIMARY KEY,
    last_offset INTEGER NOT NULL,
    updated_at REAL NOT NULL
)
"""


class OffsetRegressionError(Exception):
    """位点回退被显式拒绝。"""


class CheckpointStore:
    def __init__(self, db: sqlite3.Connection) -> None:
        self._db = db

    def init_schema(self) -> None:
        self._db.execute(_INIT_SQL)

    def get(self, source: str) -> Checkpoint:
        """不存在时返回 last_offset=-1 的初始位点。"""
        row = self._db.execute(
            "SELECT last_offset, updated_at FROM checkpoints WHERE source = ?",
            (source,),
        ).fetchone()
        if row is None:
            return Checkpoint(source=source, last_offset=-1)
        return Checkpoint(source=source, last_offset=row[0], updated_at=row[1])

    def commit(self, source: str, offset: int) -> Checkpoint:
        """提交位点；offset 必须严格大于当前位点，否则抛 OffsetRegressionError。"""
        current = self.get(source)
        if offset <= current.last_offset:
            raise OffsetRegressionError(
                f"source '{source}': refusing offset regression "
                f"{current.last_offset} -> {offset}"
            )
        now = time.time()
        self._db.execute(
            "INSERT INTO checkpoints (source, last_offset, updated_at) VALUES (?, ?, ?) "
            "ON CONFLICT(source) DO UPDATE SET last_offset = excluded.last_offset, "
            "updated_at = excluded.updated_at",
            (source, offset, now),
        )
        return Checkpoint(source=source, last_offset=offset, updated_at=now)
