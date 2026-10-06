"""核心数据模型：记录、拒绝原因、位点/检查点、结构变更流水。"""
from __future__ import annotations

import enum
import time
from dataclasses import dataclass, field
from typing import Any


class RejectReason(str, enum.Enum):
    """可区分的记录拒绝原因。"""

    MISSING_FIELD = "missing_field"        # 必填字段缺失
    TYPE_MISMATCH = "type_mismatch"        # 字段类型与目标 schema 不匹配
    INVALID_FORMAT = "invalid_format"      # 记录整体格式非法（如非 JSON、非对象）
    OFFSET_REGRESSION = "offset_regression"  # 位点回退，必须显式拒绝
    SCHEMA_INCOMPATIBLE = "schema_incompatible"  # 与目标端结构演进规则冲突


class ChangeKind(str, enum.Enum):
    """目标端结构演进事件类型。"""

    COLUMN_ADDED = "column_added"
    COLUMN_DROP_REJECTED = "column_drop_rejected"
    COLUMN_RENAME_REJECTED = "column_rename_rejected"


@dataclass(frozen=True)
class RawRecord:
    """连接器产出的原始记录。offset 在同一 source 内严格单调递增。"""

    source: str
    offset: int
    payload: Any


@dataclass(frozen=True)
class NormalizedRecord:
    """归一化后的记录，id 为幂等键。"""

    record_id: str
    fields: dict[str, Any]


@dataclass(frozen=True)
class RejectedRecord:
    """被拒绝的记录及其可区分原因。"""

    source: str
    offset: int
    reason: RejectReason
    detail: str
    payload: Any = None


@dataclass(frozen=True)
class Checkpoint:
    """位点：某 source 已确认落库的最大 offset。"""

    source: str
    last_offset: int
    updated_at: float = field(default_factory=time.time)


@dataclass(frozen=True)
class ChangeEvent:
    """结构演进流水条目。"""

    kind: ChangeKind
    column: str
    detail: str
    seq: int = 0
    at: float = field(default_factory=time.time)


@dataclass
class BatchResult:
    """一个批次摄入的结果汇总。"""

    source: str
    accepted: int = 0
    rejected: list[RejectedRecord] = field(default_factory=list)
    changes: list[ChangeEvent] = field(default_factory=list)
    checkpoint: Checkpoint | None = None


class SchemaStatus(str, enum.Enum):
    """一次结构变更尝试的结果。"""

    APPLIED = "applied"    # 兼容变更，已生效并分配新版本号
    REJECTED = "rejected"  # 不兼容变更或旧版本覆盖，已拒绝并留痕


@dataclass(frozen=True)
class SchemaVersion:
    """结构版本历史条目。

    version 仅对 applied 条目分配，从 1 开始严格递增、不跳号；
    rejected 条目 version 为 None，仅留痕。
    """

    seq: int
    status: SchemaStatus
    fields: dict[str, str]  # 字段名 -> 类型名
    detail: str
    version: int | None = None
    at: float = field(default_factory=time.time)


class OpKind(str, enum.Enum):
    """历史操作类型。"""

    INGEST = "ingest"  # 一次摄入批次
    SCHEMA = "schema"  # 一次结构变更尝试（含被拒绝的）


@dataclass(frozen=True)
class OpRecord:
    """历史操作流水条目，seq 为真实发生顺序，可用于导出与重放。"""

    seq: int
    kind: OpKind
    payload: dict[str, Any]
    at: float = field(default_factory=time.time)
