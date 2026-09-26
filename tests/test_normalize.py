"""归一化与拒绝原因分类：畸形记录不得静默跳过，合法记录不受影响。"""
from __future__ import annotations

import logging

from app.connectors.csv_source import CsvConnector
from app.connectors.jsonl import JsonlConnector
from app.models import NormalizedRecord, RejectedRecord, RejectReason
from app.pipeline import IngestionPipeline

from .conftest import good_row, log_state, write_jsonl

logger = logging.getLogger("ingestion.test")


def test_jsonl_reject_reasons_are_distinguishable(data_dir, store, caplog):
    src = data_dir / "events.jsonl"
    write_jsonl(src, [
        good_row("ok1"),                                  # 合法
        {"id": "miss", "name": "x", "value": 1.0},        # 缺 ts -> missing_field
        good_row("badtype", value="abc"),                 # value 类型错 -> type_mismatch
        good_row("badfmt", ts="not-a-time"),              # ts 格式非法 -> invalid_format
        "{not valid json",                                # 非法 JSON -> invalid_format
        good_row("ok2"),                                  # 合法
    ])
    pipe = IngestionPipeline(JsonlConnector(src), store)
    with caplog.at_level(logging.INFO):
        r = pipe.run_until_drained()
    log_state(store, "jsonl", "reject-reasons")

    reasons = [x.reason for x in r.rejected]
    assert reasons == [
        RejectReason.MISSING_FIELD,
        RejectReason.TYPE_MISMATCH,
        RejectReason.INVALID_FORMAT,
        RejectReason.INVALID_FORMAT,
    ]
    # 同批次合法记录不受影响
    assert r.inserted == 2
    assert store.count_rows() == 2
    assert {row["record_id"] for row in store.fetch_all()} == {"ok1", "ok2"}
    # 拒绝记录落表且原因可区分
    assert {x["reason"] for x in store.list_rejects()} == {
        "missing_field", "type_mismatch", "invalid_format"}
    # 位点推进到批次末尾（含被拒记录）
    assert store.get_checkpoint("jsonl") == 6
    assert any("position 0 -> 6" in rec.message for rec in caplog.records)


def test_csv_normalizes_to_unified_model(data_dir, store):
    src = data_dir / "events.csv"
    src.write_text(
        "event_id,event_time,title,amount,labels\n"
        "c1,2026-09-26T09:00:00,alpha,3.5,a;b\n"
        "c2,oops,beta,4,x\n"                              # invalid_format
        "c3,2026-09-26T09:02:00,gamma,notnum,\n"          # type_mismatch
        "c4,2026-09-26T09:03:00,,1.0,\n"                  # missing_field
    )
    pipe = IngestionPipeline(CsvConnector(src), store)
    r = pipe.run_until_drained()
    log_state(store, "csv", "csv-normalize")

    assert r.inserted == 1
    row = store.fetch_all()[0]
    assert row["record_id"] == "c1" and row["name"] == "alpha"
    assert row["value"] == 3.5 and row["tags"] == "a;b"
    assert [x.reason for x in r.rejected] == [
        RejectReason.INVALID_FORMAT,
        RejectReason.TYPE_MISMATCH,
        RejectReason.MISSING_FIELD,
    ]


def test_two_connectors_share_one_target(data_dir, store):
    """多种来源连接器统一归一后写入同一目标端。"""
    jsrc = data_dir / "events.jsonl"
    csrc = data_dir / "events.csv"
    write_jsonl(jsrc, [good_row("j1")])
    csrc.write_text("event_id,event_time,title,amount,labels\n"
                    "c1,2026-09-26T09:00:00,alpha,3.5,\n")
    rj = IngestionPipeline(JsonlConnector(jsrc), store).run_until_drained()
    rc = IngestionPipeline(CsvConnector(csrc), store).run_until_drained()
    log_state(store, "jsonl", "multi-jsonl")
    log_state(store, "csv", "multi-csv")

    assert (rj.inserted, rc.inserted) == (1, 1)
    assert store.count_rows() == 2
    # 各来源位点独立
    assert store.get_checkpoint("jsonl") == 1
    assert store.get_checkpoint("csv") == 1
    assert isinstance(rj.rejected, list)
