"""连接器协议：所有来源连接器实现统一接口。"""
from __future__ import annotations

from typing import Protocol

from app.models import Batch, NormalizedRecord, RawRecord, RejectedRecord


class SourceConnector(Protocol):
    """来源连接器协议：按位点拉取批次，并把原始记录归一化。"""

    name: str

    def fetch_batch(self, after_position: int, limit: int) -> Batch:
        """拉取位点大于 after_position 的一批原始记录。"""
        ...

    def normalize(self, record: RawRecord) -> NormalizedRecord | RejectedRecord:
        """归一化一条原始记录；非法记录返回带原因的 RejectedRecord。"""
        ...
