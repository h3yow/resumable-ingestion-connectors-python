"""归一化与校验：拒绝原因可区分，合法记录不受同批畸形记录影响。

规则：
- payload 必须是对象（dict），否则 INVALID_FORMAT；
- schema 中声明的字段为必填，缺失记 MISSING_FIELD；
- 类型按 schema 校验并做安全强转（如 "42" -> 42），失败记 TYPE_MISMATCH；
- schema 之外的字段允许通过，交由目标端做结构演进（新增列）。
"""
from __future__ import annotations

from typing import Any

from app.models import NormalizedRecord, RawRecord, RejectedRecord, RejectReason

FieldSpec = dict[str, type]

ID_FIELD = "id"


def _coerce(value: Any, target: type) -> tuple[bool, Any]:
    """安全类型强转；返回 (是否成功, 强转后的值)。"""
    if target is str:
        return (True, value) if isinstance(value, str) else (False, None)
    if target is int:
        if isinstance(value, bool):
            return False, None
        if isinstance(value, int):
            return True, value
        if isinstance(value, str):
            try:
                return True, int(value.strip())
            except ValueError:
                return False, None
        return False, None
    if target is float:
        if isinstance(value, bool):
            return False, None
        if isinstance(value, (int, float)):
            return True, float(value)
        if isinstance(value, str):
            try:
                return True, float(value.strip())
            except ValueError:
                return False, None
        return False, None
    return (True, value) if isinstance(value, target) else (False, None)


def normalize_one(raw: RawRecord, schema: FieldSpec) -> NormalizedRecord | RejectedRecord:
    """校验并归一化单条记录；失败时返回带可区分原因的 RejectedRecord。"""

    def reject(reason: RejectReason, detail: str) -> RejectedRecord:
        return RejectedRecord(
            source=raw.source, offset=raw.offset,
            reason=reason, detail=detail, payload=raw.payload,
        )

    if not isinstance(raw.payload, dict) or "__malformed__" in raw.payload:
        return reject(RejectReason.INVALID_FORMAT, "payload is not a well-formed object")

    payload: dict[str, Any] = raw.payload
    for name in schema:
        if name not in payload or payload[name] is None:
            return reject(RejectReason.MISSING_FIELD, f"required field '{name}' is missing")

    out: dict[str, Any] = {}
    for name, value in payload.items():
        target = schema.get(name)
        if target is None:
            out[name] = value  # 未声明字段原样通过，由 sink 演进
            continue
        ok, coerced = _coerce(value, target)
        if not ok:
            return reject(
                RejectReason.TYPE_MISMATCH,
                f"field '{name}' expects {target.__name__}, got {value!r}",
            )
        out[name] = coerced

    record_id = f"{raw.source}#{out[ID_FIELD]}"
    fields = {k: v for k, v in out.items() if k != ID_FIELD}
    return NormalizedRecord(record_id=record_id, fields=fields)


def normalize_batch(
    raws: list[RawRecord], schema: FieldSpec
) -> tuple[list[NormalizedRecord], list[RejectedRecord]]:
    """批量归一化：合法记录全部保留，畸形记录逐条给出拒绝原因。"""
    accepted: list[NormalizedRecord] = []
    rejected: list[RejectedRecord] = []
    for raw in raws:
        result = normalize_one(raw, schema)
        if isinstance(result, RejectedRecord):
            rejected.append(result)
        else:
            accepted.append(result)
    return accepted, rejected
