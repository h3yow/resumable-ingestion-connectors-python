"""FastAPI 端到端：注册来源 -> 摄入 -> 查询位点/记录/拒绝/流水/结构演进。"""
import json

from fastapi.testclient import TestClient

from app.main import create_app


def _client(tmp_path):
    app = create_app(db_path=str(tmp_path / "ingest.db"),
                     schema={"id": str, "name": str})
    return TestClient(app)


def test_ingest_flow_over_http(tmp_path):
    src = tmp_path / "src.jsonl"
    src.write_text(
        json.dumps({"id": "a", "name": "alice", "extra": 1}) + "\n"
        + json.dumps({"name": "no-id"}) + "\n"
        + "not json\n",
        encoding="utf-8",
    )
    client = _client(tmp_path)

    assert client.get("/health").json() == {"status": "ok"}

    r = client.post("/sources", json={"type": "jsonl", "path": str(src), "name": "api"})
    assert r.status_code == 200 and r.json() == {"source": "api"}

    r = client.post("/ingest/api")
    body = r.json()
    assert body["accepted"] == 1
    assert [x["reason"] for x in body["rejected"]] == ["missing_field", "invalid_format"]
    assert body["checkpoint"] == 2

    # 重复触发摄入：幂等，不产生重复
    r = client.post("/ingest/api")
    assert r.json()["accepted"] == 0
    records = client.get("/records").json()
    assert len(records) == 1 and records[0]["record_id"] == "api#a"
    assert records[0]["extra"] == 1  # 未声明字段经结构演进落库

    assert client.get("/checkpoints").json() == {"api": 2}
    rejections = client.get("/rejections").json()
    assert {x["reason"] for x in rejections} == {"missing_field", "invalid_format"}
    kinds = {c["kind"] for c in client.get("/changes").json()}
    assert kinds == {"column_added"}
    print("位点:", client.get("/checkpoints").json(),
          "流水:", client.get("/changes").json())


def test_schema_update_rules_over_http(tmp_path):
    client = _client(tmp_path)
    # 加列：允许
    r = client.post("/schema", json={"fields": {"id": "str", "name": "str", "age": "int"}})
    assert r.status_code == 200
    # 删列：409 拒绝
    r = client.post("/schema", json={"fields": {"id": "str", "name": "str"}})
    assert r.status_code == 409 and "drop" in r.json()["detail"]
    # 改名：409 拒绝
    r = client.post("/schema", json={"fields": {"id": "str", "fullname": "str", "age": "int"}})
    assert r.status_code == 409 and "rename" in r.json()["detail"]
    kinds = [c["kind"] for c in client.get("/changes").json()]
    assert "column_drop_rejected" in kinds and "column_rename_rejected" in kinds
