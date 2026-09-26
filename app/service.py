"""FastAPI 应用：摄入、检查点、拒绝记录、结构演进与变更流水端点。"""
from __future__ import annotations

from contextlib import asynccontextmanager
from typing import Any

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

from app.config import Settings
from app.connectors import available, create_connector
from app.pipeline import IngestionPipeline
from app.schema import Column, ColumnType, SchemaError, TableSchema
from app.store import PositionRollbackError, TargetStore

# 确保内置连接器完成注册
import app.connectors.jsonl  # noqa: F401
import app.connectors.csv_source  # noqa: F401


class EvolveRequest(BaseModel):
    """结构演进请求：只允许在末尾追加带确定缺省值的新列。"""

    add_columns: list[dict[str, Any]] = Field(default_factory=list)


def _result_dict(r) -> dict[str, Any]:
    return {
        "fetched": r.fetched,
        "inserted": r.inserted,
        "rejected": [
            {"position": x.position, "reason": x.reason.value, "detail": x.detail}
            for x in r.rejected
        ],
        "committed_position": r.committed_position,
    }


def create_app(settings: Settings | None = None) -> FastAPI:
    settings = settings or Settings()
    settings.data_dir.mkdir(parents=True, exist_ok=True)
    store = TargetStore(settings.db_path)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        yield
        store.close()

    app = FastAPI(title="resumable-ingestion", lifespan=lifespan)
    app.state.store = store
    app.state.settings = settings

    def pipeline_for(source: str) -> IngestionPipeline:
        if source not in available():
            raise HTTPException(404, f"unknown source: {source}")
        connector = create_connector(source, path=settings.source_path(source))
        return IngestionPipeline(connector, store)

    @app.get("/health")
    def health() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/sources")
    def sources() -> dict[str, Any]:
        return {"sources": list(available())}

    @app.post("/ingest/{source}")
    def ingest(source: str, limit: int | None = None, drain: bool = False):
        pipe = pipeline_for(source)
        try:
            r = (pipe.run_until_drained(limit or settings.batch_limit)
                 if drain else pipe.run_once(limit or settings.batch_limit))
        except PositionRollbackError as exc:
            raise HTTPException(409, str(exc))
        return _result_dict(r)

    @app.get("/checkpoints/{source}")
    def checkpoint(source: str) -> dict[str, Any]:
        return {"source": source, "position": store.get_checkpoint(source)}

    @app.get("/rejects")
    def rejects() -> dict[str, Any]:
        return {"rejects": store.list_rejects()}

    @app.get("/rows")
    def rows() -> dict[str, Any]:
        return {"count": store.count_rows(), "rows": store.fetch_all()}

    @app.get("/changelog")
    def changelog() -> dict[str, Any]:
        return {"changelog": store.changelog()}

    @app.post("/schema/evolve")
    def evolve(req: EvolveRequest) -> dict[str, Any]:
        try:
            new_cols = tuple(
                Column(c["name"], ColumnType(c["type"]), default=c.get("default"))
                for c in req.add_columns
            )
            new_schema = TableSchema(store.schema.version + 1, store.schema.columns + new_cols)
            store.evolve(new_schema)
        except (SchemaError, KeyError, ValueError) as exc:
            raise HTTPException(422, str(exc))
        return {"version": store.schema.version,
                "columns": list(store.schema.column_names())}

    return app
