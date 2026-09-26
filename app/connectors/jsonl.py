"""本地 JSON Lines 文件连接器（模拟来源端）。

每行一个 JSON 值；行号（从 0 开始）即 offset，严格单调递增。
非 JSON 行不跳过，而是以原始字符串形式产出，交由归一化层判定为
INVALID_FORMAT，保证畸形数据可追踪、不静默丢失。
"""
from __future__ import annotations

import json
from typing import Iterator

from app.models import RawRecord


class JsonlConnector:
    def __init__(self, path: str, name: str | None = None) -> None:
        self._path = path
        self._name = name or f"jsonl:{path}"

    @property
    def name(self) -> str:
        return self._name

    def read(self, from_offset: int) -> Iterator[RawRecord]:
        with open(self._path, "r", encoding="utf-8") as fh:
            for lineno, line in enumerate(fh):
                if lineno <= from_offset:
                    continue
                text = line.strip()
                if not text:
                    continue  # 空行不占用位点语义之外的记录，直接跳过
                try:
                    payload = json.loads(text)
                except json.JSONDecodeError:
                    payload = text  # 保留原文，归一化层判 INVALID_FORMAT
                yield RawRecord(source=self._name, offset=lineno, payload=payload)
