"""JSONL 文件连接器：以本地 JSONL 文件模拟来源端。

每行一个 JSON 对象，行号即位点（从 1 开始）。期望字段：
  id: str           业务主键（必填）
  ts: str           ISO8601 时间（必填，格式非法即拒绝）
  name: str         （必填）
  value: int/float  （必填，bool 视为非法类型）
  tags: list[str]   （可选，缺省为空）
"""
from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any

from app.connectors import register
from app.models import (
    Batch,
    NormalizedRecord,
    RawRecord,
    RejectedRecord,
    RejectReason,
)

_REQUIRED = ("id", "ts", "name", "value")


def _reject(record: RawRecord, reason: RejectReason, detail: str) -> RejectedRecord:
    return RejectedRecord(
        source=record.source,
        position=record.position,
        reason=reason,
        detail=detail,
        payload=record.payload,
    )


@register
class JsonlConnector:
    name = "jsonl"

    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)

    def fetch_batch(self, after_position: int, limit: int) -> Batch:
        records: list[RawRecord] = []
        end = after_position
        if not self.path.exists():
            return Batch(source=self.name, records=(), end_position=end)
        with self.path.open("r", encoding="utf-8") as fh:
            for lineno, line in enumerate(fh, start=1):
                if lineno <= after_position:
                    continue
                if len(records) >= limit:
                    break
                end = lineno
                text = line.strip()
                try:
                    payload: dict[str, Any] = json.loads(text)
                    if not isinstance(payload, dict):
                        raise ValueError("not a JSON object")
                    record_id = str(payload.get("id", ""))
                except (json.JSONDecodeError, ValueError) as exc:
                    payload = {"__parse_error__": str(exc), "__raw__": text}
                    record_id = ""
                records.append(
                    RawRecord(
                        source=self.name,
                        position=lineno,
                        payload=payload,
                        record_id=record_id,
                    )
                )
        return Batch(source=self.name, records=tuple(records), end_position=end)

    def normalize(self, record: RawRecord) -> NormalizedRecord | RejectedRecord:
        p = record.payload
        if "__parse_error__" in p:
            return _reject(record, RejectReason.INVALID_FORMAT,
                           f"malformed json: {p['__parse_error__']}")
        for f in _REQUIRED:
            if f not in p or p[f] is None:
                return _reject(record, RejectReason.MISSING_FIELD, f"missing field: {f}")
        if not isinstance(p["id"], str) or not p["id"]:
            return _reject(record, RejectReason.TYPE_MISMATCH, "id must be non-empty str")
        if not isinstance(p["name"], str):
            return _reject(record, RejectReason.TYPE_MISMATCH, "name must be str")
        if isinstance(p["value"], bool) or not isinstance(p["value"], (int, float)):
            return _reject(record, RejectReason.TYPE_MISMATCH, "value must be number")
        if not isinstance(p["ts"], str):
            return _reject(record, RejectReason.TYPE_MISMATCH, "ts must be str")
        try:
            ts = datetime.fromisoformat(p["ts"])
        except ValueError:
            return _reject(record, RejectReason.INVALID_FORMAT,
                           f"ts not ISO8601: {p['ts']!r}")
        tags = p.get("tags", [])
        if not isinstance(tags, list) or not all(isinstance(t, str) for t in tags):
            return _reject(record, RejectReason.TYPE_MISMATCH, "tags must be list[str]")
        return NormalizedRecord(
            record_id=p["id"],
            source=record.source,
            position=record.position,
            occurred_at=ts.isoformat(),
            name=p["name"],
            value=float(p["value"]),
            tags=tuple(tags),
        )
