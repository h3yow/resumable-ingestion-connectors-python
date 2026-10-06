"""历史重放：在干净实例上按真实顺序重放操作流水，并逐项比对最终状态。

- replay：把导出的操作流水在一个全新实例上按序执行——摄入操作按记录的
  批次大小重新消费对应来源，结构变更按记录的 fields/base_version 重新应用，
  被拒绝的变更要求重放时同样被拒绝；
- snapshot / diff_states：抽取目标端数据、各来源位点、拒绝记录、结构历史、
  变更流水与操作流水（均不含时间戳等不确定字段），逐项比对。
"""
from __future__ import annotations

import logging
import sqlite3
from typing import Any

from app.connectors.base import SourceConnector
from app.models import OpKind, OpRecord
from app.normalize import TYPE_NAMES
from app.pipeline import Pipeline
from app.schema_version import StaleSchemaVersionError
from app.sink import SchemaEvolutionError

log = logging.getLogger("pipeline")


class ReplayMismatchError(Exception):
    """重放结果与原实例不一致，或被拒绝的操作在重放时意外成功。"""


def replay(ops: list[OpRecord], pipeline: Pipeline,
           sources: dict[str, SourceConnector]) -> None:
    """在干净实例的 pipeline 上按序重放操作流水。"""
    for op in ops:
        p = op.payload
        if op.kind is OpKind.INGEST:
            connector = sources[p["source"]]
            result = pipeline.run_once(connector, batch_size=p["records"])
            log.info("replay ingest seq=%d source=%s offsets=%d..%d accepted=%d",
                     op.seq, p["source"], p["from_offset"], p["to_offset"],
                     result.accepted)
        else:
            spec = {k: TYPE_NAMES[v] for k, v in p["fields"].items()}
            if p["status"] == "applied":
                entry = pipeline.apply_schema(spec, base_version=p["base_version"])
                log.info("replay schema seq=%d applied v%s", op.seq, entry.version)
            else:
                try:
                    pipeline.apply_schema(spec, base_version=p["base_version"])
                except (SchemaEvolutionError, StaleSchemaVersionError) as exc:
                    log.info("replay schema seq=%d rejected as expected: %s",
                             op.seq, exc)
                else:
                    raise ReplayMismatchError(
                        f"op #{op.seq}: schema change rejected in history "
                        f"but succeeded on replay: {p}"
                    )


def snapshot(db: sqlite3.Connection) -> dict[str, Any]:
    """抽取实例的确定性状态（不含时间戳），用于跨实例逐项比对。"""
    records_cur = db.execute("SELECT * FROM records ORDER BY record_id")
    names = [d[0] for d in records_cur.description]
    return {
        "records": [dict(zip(names, row)) for row in records_cur.fetchall()],
        "checkpoints": dict(db.execute(
            "SELECT source, last_offset FROM checkpoints ORDER BY source"
        ).fetchall()),
        "rejections": db.execute(
            "SELECT source, offset, reason, detail FROM rejections "
            "ORDER BY source, offset"
        ).fetchall(),
        "schema_history": db.execute(
            "SELECT version, status, fields, detail FROM schema_history ORDER BY seq"
        ).fetchall(),
        "change_log": db.execute(
            "SELECT kind, column_name, detail FROM change_log ORDER BY seq"
        ).fetchall(),
        "op_log": db.execute(
            "SELECT kind, payload FROM op_log ORDER BY seq"
        ).fetchall(),
    }


def diff_states(original: sqlite3.Connection,
                replica: sqlite3.Connection) -> dict[str, tuple[Any, Any]]:
    """逐项比对两个实例的状态；返回空 dict 表示完全一致。"""
    a, b = snapshot(original), snapshot(replica)
    return {key: (a[key], b[key]) for key in a if a[key] != b[key]}
