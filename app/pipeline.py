"""摄入流水线：读 -> 归一化 -> 演进/落库 -> 提交位点（单事务）。

语义：
- 一个批次内：落库、拒绝记录持久化、位点提交在同一事务，崩溃不产生
  半提交状态；重启后从位点续传，幂等 UPSERT 保证不重复；
- 被拒绝的记录持久化到 rejections 表（按 (source, offset) 幂等），
  位点照常越过，合法记录不受影响；
- 位点只允许前进，回退由 CheckpointStore 显式拒绝。
"""
from __future__ import annotations

import itertools
import json
import logging
import sqlite3
import time

from app.checkpoint import CheckpointStore
from app.connectors.base import SourceConnector
from app.models import BatchResult
from app.normalize import FieldSpec, normalize_batch
from app.sink import Sink

log = logging.getLogger("pipeline")


def connect_db(path: str) -> sqlite3.Connection:
    """创建自动提交模式的连接；事务由 Pipeline 用 BEGIN IMMEDIATE 显式管理。

    BEGIN IMMEDIATE 在事务开始时即获取写锁，使并发消费者的"读位点 ->
    落库 -> 提交位点"整体串行化，位点回退检查不会被快照隔离绕过。
    """
    return sqlite3.connect(path, timeout=30, isolation_level=None,
                           check_same_thread=False)

_REJECT_SQL = """
CREATE TABLE IF NOT EXISTS rejections (
    source TEXT NOT NULL,
    offset INTEGER NOT NULL,
    reason TEXT NOT NULL,
    detail TEXT NOT NULL,
    payload TEXT,
    at REAL NOT NULL,
    PRIMARY KEY (source, offset)
)
"""


class Pipeline:
    def __init__(self, db: sqlite3.Connection, schema: FieldSpec) -> None:
        self._db = db
        self._schema = schema
        self.checkpoints = CheckpointStore(db)
        self.sink = Sink(db)

    def init(self) -> None:
        self.checkpoints.init_schema()
        self.sink.init_schema()
        self._db.execute(_REJECT_SQL)
        self.sink.apply_declared_schema(self._schema)
        self._db.commit()

    def _persist_rejections(self, rejected: list) -> None:
        now = time.time()
        for r in rejected:
            self._db.execute(
                "INSERT OR REPLACE INTO rejections "
                "(source, offset, reason, detail, payload, at) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                (r.source, r.offset, r.reason.value, r.detail,
                 json.dumps(r.payload, ensure_ascii=False, default=str), now),
            )

    def run_once(self, connector: SourceConnector, batch_size: int = 100) -> BatchResult:
        """从当前位点消费一个批次并落库，返回批次结果（含拒绝明细与位点）。"""
        source = connector.name
        start = self.checkpoints.get(source)
        raws = list(itertools.islice(connector.read(start.last_offset), batch_size))
        result = BatchResult(source=source)
        if not raws:
            result.checkpoint = start
            return result

        accepted, rejected = normalize_batch(raws, self._schema)
        self._db.execute("BEGIN IMMEDIATE")
        try:
            changes = []
            for rec in accepted:
                changes.extend(self.sink.ensure_columns(rec.fields))
            result.accepted = self.sink.upsert(accepted)
            self._persist_rejections(rejected)
            result.rejected = rejected
            result.changes = changes
            last_offset = max(r.offset for r in raws)
            result.checkpoint = self.checkpoints.commit(source, last_offset)
            self._db.execute("COMMIT")
        except Exception:
            self._db.execute("ROLLBACK")
            raise

        log.info(
            "batch source=%s offsets=%d..%d accepted=%d rejected=%d checkpoint=%d",
            source, raws[0].offset, raws[-1].offset,
            result.accepted, len(rejected), result.checkpoint.last_offset,
        )
        for r in rejected:
            log.info("rejected source=%s offset=%d reason=%s detail=%s",
                     r.source, r.offset, r.reason.value, r.detail)
        for c in changes:
            log.info("change seq=%s kind=%s column=%s detail=%s",
                     c.seq, c.kind.value, c.column, c.detail)
        return result

    def rejections(self, source: str | None = None) -> list[tuple]:
        """查询已持久化的拒绝记录。"""
        if source is None:
            return self._db.execute(
                "SELECT source, offset, reason, detail FROM rejections ORDER BY source, offset"
            ).fetchall()
        return self._db.execute(
            "SELECT source, offset, reason, detail FROM rejections "
            "WHERE source = ? ORDER BY offset",
            (source,),
        ).fetchall()
