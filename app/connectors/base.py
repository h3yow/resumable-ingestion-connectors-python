"""连接器协议：所有来源连接器统一实现该接口。"""
from __future__ import annotations

from typing import Iterator, Protocol

from app.models import RawRecord


class SourceConnector(Protocol):
    """来源连接器协议。

    - name: 来源标识，用于位点与幂等键命名空间。
    - read(from_offset): 从指定位点之后开始产出 RawRecord，offset 严格递增。
    """

    @property
    def name(self) -> str: ...

    def read(self, from_offset: int) -> Iterator[RawRecord]: ...
