"""摄入流水线：拉取 -> 归一 -> 幂等落库 -> 推进位点。

同一批次内：合法记录正常落库，非法记录进 rejects 表（带可区分原因），
互不影响；位点推进到批次末尾（含被拒记录），保证重启后不重不漏。
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field

from app.connectors.base import SourceConnector
from app.models import NormalizedRecord, RejectedRecord
from app.store import PositionRollbackError, TargetStore

logger = logging.getLogger("ingestion.pipeline")


@dataclass
class IngestResult:
    fetched: int = 0
    inserted: int = 0
    rejected: list[RejectedRecord] = field(default_factory=list)
    committed_position: int = 0


class IngestionPipeline:
    """把来源连接器的数据摄入目标端存储。"""

    def __init__(self, connector: SourceConnector, store: TargetStore) -> None:
        self.connector = connector
        self.store = store

    def run_once(self, limit: int = 100) -> IngestResult:
        """从检查点位点继续拉取一个批次并落库。"""
        source = self.connector.name
        start = self.store.get_checkpoint(source)
        batch = self.connector.fetch_batch(start, limit)

        good: list[NormalizedRecord] = []
        bad: list[RejectedRecord] = []
        for raw in batch.records:
            out = self.connector.normalize(raw)
            if isinstance(out, RejectedRecord):
                bad.append(out)
                logger.info("reject source=%s pos=%s reason=%s detail=%s",
                            out.source, out.position, out.reason.value, out.detail)
            else:
                good.append(out)

        try:
            inserted = self.store.commit_batch(source, good, bad, batch.end_position)
        except PositionRollbackError:
            # 并发消费者已提交覆盖本批次的位点区间：本批次数据已被事务性提交，
            # 直接采用最新检查点继续，不重灌、不丢失。
            current = self.store.get_checkpoint(source)
            logger.warning(
                "batch superseded source=%s range (%s, %s] already covered by "
                "checkpoint %s; skipping", source, start, batch.end_position, current)
            return IngestResult(
                fetched=len(batch.records),
                inserted=0,
                rejected=bad,
                committed_position=current,
            )
        logger.info("commit source=%s position %s -> %s fetched=%d inserted=%d rejected=%d",
                    source, start, batch.end_position,
                    len(batch.records), inserted, len(bad))
        return IngestResult(
            fetched=len(batch.records),
            inserted=inserted,
            rejected=bad,
            committed_position=batch.end_position,
        )

    def run_until_drained(self, limit: int = 100, max_rounds: int = 1000) -> IngestResult:
        """连续拉取直到来源无新数据，返回汇总结果。"""
        total = IngestResult()
        for _ in range(max_rounds):
            r = self.run_once(limit)
            total.fetched += r.fetched
            total.inserted += r.inserted
            total.rejected.extend(r.rejected)
            total.committed_position = r.committed_position
            if r.fetched == 0:
                break
        return total
