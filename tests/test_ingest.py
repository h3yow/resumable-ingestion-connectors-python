"""畸形记录归类与批次隔离：合法记录不受同批畸形记录影响。"""
import json
import logging

from app.connectors.csv_source import CsvConnector
from app.connectors.jsonl import JsonlConnector
from app.models import RejectReason


def _write_jsonl(path, lines):
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return str(path)


def test_malformed_records_classified_and_good_records_unaffected(pipeline, tmp_path, caplog):
    src = _write_jsonl(tmp_path / "src.jsonl", [
        json.dumps({"id": "a", "name": "alice", "age": 30}),          # 合法
        json.dumps({"name": "no-id", "age": 1}),                      # 缺 id -> missing_field
        json.dumps({"id": "b", "name": "bob"}),                       # 缺 age -> missing_field
        json.dumps({"id": "c", "name": "carol", "age": "not-a-num"}), # 类型不符 -> type_mismatch
        "this is not json",                                           # 非法格式 -> invalid_format
        json.dumps(["not", "an", "object"]),                          # 非对象 -> invalid_format
        json.dumps({"id": "d", "name": "dave", "age": "41"}),         # 合法（"41" 可强转）
    ])
    result = pipeline.run_once(JsonlConnector(src, name="src"))

    assert result.accepted == 2
    reasons = [r.reason for r in result.rejected]
    assert reasons == [
        RejectReason.MISSING_FIELD,
        RejectReason.MISSING_FIELD,
        RejectReason.TYPE_MISMATCH,
        RejectReason.INVALID_FORMAT,
        RejectReason.INVALID_FORMAT,
    ]
    # 每种原因可区分
    assert len({r.reason for r in result.rejected}) == 3

    rows = pipeline.sink.fetch_all()
    assert [r["record_id"] for r in rows] == ["src#a", "src#d"]
    assert rows[1]["age"] == 41  # 字符串 "41" 被确定性强转为 int

    # 拒绝记录已持久化，重启后仍可审计
    persisted = pipeline.rejections("src")
    assert len(persisted) == 5
    assert {p[2] for p in persisted} == {
        "missing_field", "type_mismatch", "invalid_format"
    }

    # 日志包含位点信息
    assert any("checkpoint=6" in r.getMessage() for r in caplog.records)
    assert any("reason=type_mismatch" in r.getMessage() for r in caplog.records)
    print("位点:", result.checkpoint.last_offset,
          "拒绝:", [(r.offset, r.reason.value) for r in result.rejected])


def test_csv_connector_malformed_rows(pipeline, tmp_path):
    src = tmp_path / "src.csv"
    src.write_text(
        "id,name,age\n"
        "a,alice,30\n"
        "b,bob\n"                      # 列数不足 -> invalid_format
        "c,carol,notnum\n"             # 类型不符 -> type_mismatch
        "d,dave,40\n",
        encoding="utf-8",
    )
    result = pipeline.run_once(CsvConnector(str(src), name="csvsrc"))
    assert result.accepted == 2
    assert [r.reason for r in result.rejected] == [
        RejectReason.INVALID_FORMAT, RejectReason.TYPE_MISMATCH,
    ]
