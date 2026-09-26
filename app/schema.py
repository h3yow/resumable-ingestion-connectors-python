"""目标端结构（schema）定义与演进规则。

演进规则（确定且可复现）：
1. 只允许在末尾追加新列；新列必须 required=False，缺省值即 Column.default
   （可为 None，但语义确定：历史行读取时恒为该缺省值）。
2. 已有列的名称、类型、顺序不得变化；删列、改名、改类型、重排一律
   抛出 SchemaError，绝不静默错列。
3. 版本号必须严格 +1，保证规则更新后历史结论可复现。
"""
from __future__ import annotations

import enum
from dataclasses import dataclass
from typing import Any


class ColumnType(str, enum.Enum):
    TEXT = "text"
    REAL = "real"
    INTEGER = "integer"


@dataclass(frozen=True)
class Column:
    name: str
    type: ColumnType
    default: Any = None          # 新增列的缺省值（确定且稳定）
    required: bool = False


@dataclass(frozen=True)
class TableSchema:
    """目标表的某一版本结构。"""

    version: int
    columns: tuple[Column, ...]

    def column_names(self) -> tuple[str, ...]:
        return tuple(c.name for c in self.columns)

    def column(self, name: str) -> Column:
        for c in self.columns:
            if c.name == name:
                return c
        raise KeyError(name)


class SchemaError(Exception):
    """结构演进非法（如删列、改名、改类型、重排）。"""


def evolve_schema(old: TableSchema, new: TableSchema) -> TableSchema:
    """校验并应用结构演进：仅允许末尾新增带确定缺省值的列。"""
    if new.version != old.version + 1:
        raise SchemaError(
            f"version must increment by 1: {old.version} -> {new.version}"
        )
    old_cols, new_cols = old.columns, new.columns
    if len(new_cols) < len(old_cols):
        raise SchemaError("dropping columns is not allowed")
    for i, oc in enumerate(old_cols):
        nc = new_cols[i]
        if nc.name != oc.name:
            raise SchemaError(
                f"column #{i} renamed/reordered: {oc.name!r} -> {nc.name!r}"
            )
        if nc.type != oc.type:
            raise SchemaError(f"column {oc.name!r} type changed")
    for nc in new_cols[len(old_cols):]:
        if nc.required:
            raise SchemaError(
                f"new column {nc.name!r} must be optional with a deterministic default"
            )
    return new
