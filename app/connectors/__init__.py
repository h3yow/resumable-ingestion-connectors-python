"""内置连接器注册表。"""
from __future__ import annotations

from app.connectors.base import SourceConnector

_REGISTRY: dict[str, type] = {}


def register(cls: type) -> type:
    _REGISTRY[cls.name] = cls
    return cls


def create_connector(name: str, **kwargs) -> SourceConnector:
    if name not in _REGISTRY:
        raise KeyError(f"unknown connector: {name}")
    return _REGISTRY[name](**kwargs)


def available() -> tuple[str, ...]:
    return tuple(sorted(_REGISTRY))
