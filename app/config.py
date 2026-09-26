"""服务配置：全部可用环境变量覆盖，默认使用本地 ./data 目录。"""
from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path


@dataclass(frozen=True)
class Settings:
    """本地可复现的运行配置，不依赖任何外部服务或真实凭据。"""

    data_dir: Path = field(default_factory=lambda: Path(os.environ.get("INGEST_DATA_DIR", "./data")))
    db_name: str = "target.db"
    batch_limit: int = 100

    @property
    def db_path(self) -> Path:
        return self.data_dir / self.db_name

    def source_path(self, source: str) -> Path:
        """来源文件路径：jsonl -> events.jsonl, csv -> events.csv。"""
        suffix = {"jsonl": "events.jsonl", "csv": "events.csv"}.get(source)
        if suffix is None:
            raise KeyError(f"unknown source: {source}")
        return self.data_dir / suffix
