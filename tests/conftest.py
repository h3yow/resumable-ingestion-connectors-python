from __future__ import annotations

import json
import logging
import pathlib

import pytest

from app.store import TargetStore

logger = logging.getLogger("ingestion.test")


@pytest.fixture()
def data_dir(tmp_path: pathlib.Path) -> pathlib.Path:
    return tmp_path


@pytest.fixture()
def store(data_dir: pathlib.Path):
    s = TargetStore(data_dir / "target.db")
    yield s
    s.close()


def write_jsonl(path: pathlib.Path, rows: list[dict | str]) -> None:
    path.write_text(
        "\n".join(json.dumps(r) if isinstance(r, dict) else r for r in rows) + "\n",
        encoding="utf-8",
    )


def good_row(rid: str, **over) -> dict:
    row = {"id": rid, "ts": "2026-09-26T10:00:00", "name": f"n-{rid}", "value": 1.0}
    row.update(over)
    return row


def log_state(store: TargetStore, source: str, label: str) -> None:
    """在单测日志中打印位点与变更流水。"""
    logger.info("[%s] checkpoint(%s)=%d rows=%d", label, source,
                store.get_checkpoint(source), store.count_rows())
    for entry in store.changelog():
        logger.info("[%s] changelog #%s %s %s", label,
                    entry["id"], entry["kind"], entry["detail"])
