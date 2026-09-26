"""FastAPI 端点：摄入、检查点、拒绝记录、结构演进、变更流水。"""
from __future__ import annotations

import logging

import pytest
from fastapi.testclient import TestClient

from app.config import Settings
from app.service import create_app

from .conftest import good_row, write_jsonl

logger = logging.getLogger("ingestion.test")


@pytest.fixture()
def client(data_dir):
    write_jsonl(data_dir / "events.jsonl", [
        good_row("a1"),
        good_row("a2", ts="bad-ts"),
        good_row("a3"),
    ])
    (data_dir / "events.csv").write_text(
        "event_id,event_time,title,amount,labels\n"
        "c1,2026-09-26T09:00:00,alpha,3.5,x;y\n"
    )
    app = create_app(Settings(data_dir=data_dir))
    with TestClient(app) as c:
        yield c


def test_ingest_drain_and_idempotent_reconsume(client, caplog):
    with caplog.at_level(logging.INFO):
        r1 = client.post("/ingest/jsonl", params={"drain": True}).json()
        r2 = client.post("/ingest/jsonl", params={"drain": True}).json()
    assert (r1["fetched"], r1["inserted"]) == (3, 2)
    assert len(r1["rejected"]) == 1
    assert r1["rejected"][0]["reason"] == "invalid_format"
    assert (r2["fetched"], r2["inserted"]) == (0, 0)   # 重复消费幂等
    cp = client.get("/checkpoints/jsonl").json()
    logger.info("checkpoint after drain: %s", cp)
    assert cp["position"] == 3
    for rec in caplog.records:
        if "commit source=" in rec.message:
            logger.info("position log: %s", rec.message)


def test_multi_source_unified_target(client):
    client.post("/ingest/jsonl", params={"drain": True})
    client.post("/ingest/csv", params={"drain": True})
    rows = client.get("/rows").json()
    assert rows["count"] == 3                          # jsonl 2 + csv 1
    csv_row = [r for r in rows["rows"] if r["source"] == "csv"][0]
    assert csv_row["record_id"] == "c1" and csv_row["value"] == 3.5
    assert csv_row["tags"] == "x;y"
    assert client.get("/checkpoints/csv").json()["position"] == 1


def test_unknown_source_404(client):
    assert client.post("/ingest/nope").status_code == 404


def test_schema_evolve_endpoint_and_rejects(client):
    client.post("/ingest/jsonl", params={"drain": True})
    ok = client.post("/schema/evolve", json={
        "add_columns": [{"name": "score", "type": "real", "default": 0.0}]})
    assert ok.status_code == 200 and ok.json()["version"] == 2
    rows = client.get("/rows").json()["rows"]
    assert all(r["score"] == 0.0 for r in rows)        # 缺省语义确定
    bad = client.post("/schema/evolve", json={"add_columns": []})
    assert bad.status_code == 422                      # 空演进被拒绝
    rejects = client.get("/rejects").json()["rejects"]
    assert rejects[0]["reason"] == "invalid_format"
    kinds = [e["kind"] for e in client.get("/changelog").json()["changelog"]]
    logger.info("changelog kinds: %s", kinds)
    assert "commit" in kinds and "schema_evolve" in kinds
