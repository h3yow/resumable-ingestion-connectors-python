"""本地 CSV 文件连接器（模拟来源端）。

首行为表头；数据行行号（从 0 开始，含表头行）即 offset，严格单调递增。
列数与表头不一致的行不静默跳过，以 {"__malformed__": [...]} 形式产出，
交由归一化层判定为 INVALID_FORMAT。
"""
from __future__ import annotations

import csv
from typing import Iterator

from app.models import RawRecord


class CsvConnector:
    def __init__(self, path: str, name: str | None = None) -> None:
        self._path = path
        self._name = name or f"csv:{path}"

    @property
    def name(self) -> str:
        return self._name

    def read(self, from_offset: int) -> Iterator[RawRecord]:
        with open(self._path, "r", encoding="utf-8", newline="") as fh:
            reader = csv.reader(fh)
            header: list[str] | None = None
            for lineno, row in enumerate(reader):
                if lineno == 0:
                    header = row
                    if from_offset < 0:
                        continue
                if lineno <= from_offset:
                    continue
                assert header is not None
                if len(row) != len(header):
                    payload = {"__malformed__": row}
                else:
                    payload = dict(zip(header, row))
                yield RawRecord(source=self._name, offset=lineno, payload=payload)
