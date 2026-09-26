"""CSV 文件连接器：以本地 CSV 文件模拟来源端。

首行为表头，数据行行号即位点（从 1 开始，不含表头）。列名与 JSONL 不同，
由本连接器负责归一到统一模型：
  event_id -> record_id, event_time -> occurred_at,
  title -> name, amount -> value, labels -> tags（以 ';' 分隔）
"""
from __future__ import annotations

import csv
from datetime import datetime
from pathlib import Path

from app.connectors import register
from app.connectors.jsonl import _reject
from app.models import Batch, NormalizedRecord, RawRecord, RejectReason

_HEADER = ("event_id", "event_time", "title", "amount", "labels")


@register
class CsvConnector:
    name = "csv"

    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)

    def fetch_batch(self, after_position: int, limit: int) -> Batch:
        records: list[RawRecord] = []
        end = after_position
        if not self.path.exists():
            return Batch(source=self.name, records=(), end_position=end)
        with self.path.open("r", encoding="utf-8", newline="") as fh:
            reader = csv.reader(fh)
            header = next(reader, None)
            if header is None or tuple(h.strip() for h in header) != _HEADER:
                # 表头不符：整个文件无法解释，产出一条格式非法记录占位
                return Batch(
                    source=self.name,
                    records=(RawRecord(
                        source=self.name, position=0,
                        payload={"__parse_error__": f"bad header: {header}"},
                    ),),
                    end_position=end,
                )
            for pos, row in enumerate(reader, start=1):
                if pos <= after_position:
                    continue
                if len(records) >= limit:
                    break
                end = pos
                payload = dict(zip(_HEADER, row))
                if len(row) != len(_HEADER):
                    payload["__parse_error__"] = f"expected {len(_HEADER)} cols, got {len(row)}"
                records.append(RawRecord(
                    source=self.name, position=pos, payload=payload,
                    record_id=payload.get("event_id", ""),
                ))
        return Batch(source=self.name, records=tuple(records), end_position=end)

    def normalize(self, record: RawRecord) -> NormalizedRecord | RejectedRecord:
        p = record.payload
        if "__parse_error__" in p:
            return _reject(record, RejectReason.INVALID_FORMAT,
                           f"malformed csv: {p['__parse_error__']}")
        for f in _HEADER:
            if not p.get(f):
                if f == "labels":
                    continue  # labels 允许为空
                return _reject(record, RejectReason.MISSING_FIELD, f"missing field: {f}")
        try:
            ts = datetime.fromisoformat(p["event_time"])
        except ValueError:
            return _reject(record, RejectReason.INVALID_FORMAT,
                           f"event_time not ISO8601: {p['event_time']!r}")
        try:
            value = float(p["amount"])
        except ValueError:
            return _reject(record, RejectReason.TYPE_MISMATCH,
                           f"amount not a number: {p['amount']!r}")
        tags = tuple(t for t in p.get("labels", "").split(";") if t)
        return NormalizedRecord(
            record_id=p["event_id"],
            source=record.source,
            position=record.position,
            occurred_at=ts.isoformat(),
            name=p["title"],
            value=value,
            tags=tags,
        )
