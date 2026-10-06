"""FastAPI 入口：本地可运行的可恢复摄入服务。

配置项（环境变量）：
- INGEST_DB: SQLite 数据库文件路径，默认 ./ingest.db
"""
from __future__ import annotations

import os
from typing import Any

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from app.checkpoint import OffsetRegressionError
from app.connectors.csv_source import CsvConnector
from app.connectors.jsonl import JsonlConnector
from app.normalize import TYPE_NAMES
from app.pipeline import Pipeline, connect_db
from app.schema_version import StaleSchemaVersionError
from app.sink import SchemaEvolutionError

DEFAULT_SCHEMA: dict[str, type] = {"id": str, "name": str}


class SourceIn(BaseModel):
    type: str  # "jsonl" | "csv"
    path: str
    name: str | None = None


class SchemaIn(BaseModel):
    fields: dict[str, str]  # 字段名 -> 类型名（str/int/float/bool）
    base_version: int | None = None  # 乐观并发：客户端基于的版本号，过期则 409


def _make_connector(spec: SourceIn):
    if spec.type == "jsonl":
        return JsonlConnector(spec.path, spec.name)
    if spec.type == "csv":
        return CsvConnector(spec.path, spec.name)
    raise HTTPException(status_code=400, detail=f"unknown connector type: {spec.type}")


def create_app(db_path: str | None = None, schema: dict[str, type] | None = None) -> FastAPI:
    db_path = db_path or os.environ.get("INGEST_DB", "./ingest.db")
    db = connect_db(db_path)
    pipeline = Pipeline(db, schema or dict(DEFAULT_SCHEMA))
    pipeline.init()

    app = FastAPI(title="resumable-ingestion")
    connectors: dict[str, Any] = {}

    @app.get("/health")
    def health() -> dict[str, str]:
        return {"status": "ok"}

    @app.post("/sources")
    def register_source(spec: SourceIn) -> dict[str, str]:
        connector = _make_connector(spec)
        connectors[connector.name] = connector
        return {"source": connector.name}

    @app.post("/ingest/{source}")
    def ingest(source: str, batch_size: int = 100) -> dict[str, Any]:
        connector = connectors.get(source)
        if connector is None:
            raise HTTPException(status_code=404, detail=f"unknown source: {source}")
        try:
            result = pipeline.run_once(connector, batch_size=batch_size)
        except OffsetRegressionError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        return {
            "source": result.source,
            "accepted": result.accepted,
            "rejected": [
                {"offset": r.offset, "reason": r.reason.value, "detail": r.detail}
                for r in result.rejected
            ],
            "changes": [
                {"seq": c.seq, "kind": c.kind.value, "column": c.column}
                for c in result.changes
            ],
            "checkpoint": result.checkpoint.last_offset if result.checkpoint else None,
        }

    @app.get("/checkpoints")
    def checkpoints() -> dict[str, int]:
        rows = db.execute("SELECT source, last_offset FROM checkpoints").fetchall()
        return {s: o for s, o in rows}

    @app.get("/records")
    def records() -> list[dict[str, Any]]:
        return pipeline.sink.fetch_all()

    @app.get("/rejections")
    def rejections() -> list[dict[str, Any]]:
        return [
            {"source": s, "offset": o, "reason": r, "detail": d}
            for s, o, r, d in pipeline.rejections()
        ]

    @app.get("/changes")
    def changes() -> list[dict[str, Any]]:
        return [
            {"seq": c.seq, "kind": c.kind.value, "column": c.column, "detail": c.detail}
            for c in pipeline.sink.change_log()
        ]

    @app.post("/schema")
    def update_schema(spec: SchemaIn) -> dict[str, Any]:
        try:
            incoming = {k: TYPE_NAMES[v] for k, v in spec.fields.items()}
        except KeyError as exc:
            raise HTTPException(status_code=400, detail=f"unknown type: {exc}") from exc
        try:
            entry = pipeline.apply_schema(incoming, base_version=spec.base_version)
        except (SchemaEvolutionError, StaleSchemaVersionError) as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        return {"status": "ok", "version": entry.version}

    @app.get("/schema")
    def current_schema() -> dict[str, Any]:
        return {
            "version": pipeline.sink.registry.current_version(),
            "fields": pipeline.sink.registry.current_fields(),
        }

    @app.get("/schema/history")
    def schema_history() -> list[dict[str, Any]]:
        return [
            {"seq": e.seq, "version": e.version, "status": e.status.value,
             "fields": e.fields, "detail": e.detail}
            for e in pipeline.sink.registry.history()
        ]

    @app.get("/history")
    def export_history() -> list[dict[str, Any]]:
        """导出全部历史操作（摄入与结构变更，按真实发生顺序），可用于重放。"""
        return [
            {"seq": op.seq, "kind": op.kind.value, "payload": op.payload}
            for op in pipeline.history.export()
        ]

    return app


app = create_app()
