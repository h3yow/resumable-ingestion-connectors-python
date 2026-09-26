"""核心数据模型：原始记录、归一化记录、拒绝原因、位点与批次。"""
from __future__ import annotations

import enum
from dataclasses import dataclass, field
from typing import Any


class RejectReason(str, enum.Enum):
    """可区分的记录拒绝原因。"""

    MISSING_FIELD = "missing_field"        # 必填字段缺失
    TYPE_MISMATCH = "type_mismatch"        # 字段类型不匹配
    INVALID_FORMAT = "invalid_format"      # 字段格式非法（如时间戳、枚举值）
    UNKNOWN_CONNECTOR = "unknown_connector"  # 未知来源连接器


@dataclass(frozen=True)
class RawRecord:
    """来源端产出的一条原始记录。"""

    source: str                 # 来源连接器名
    position: int               # 来源内单调递增位点
    payload: dict[str, Any]     # 原始负载
    record_id: str = ""         # 来源侧业务主键（用于幂等）


@dataclass(frozen=True)
class NormalizedRecord:
    """归一化后的记录：统一列名与类型。"""

    record_id: str
    source: str
    position: int
    occurred_at: str            # ISO8601 字符串
    name: str
    value: float
    tags: tuple[str, ...] = ()


@dataclass(frozen=True)
class RejectedRecord:
    """被拒绝的记录及其可区分原因。"""

    source: str
    position: int
    reason: RejectReason
    detail: str
    payload: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class Batch:
    """一次拉取的批次：记录 + 批次结束位点。"""

    source: str
    records: tuple[RawRecord, ...]
    end_position: int
