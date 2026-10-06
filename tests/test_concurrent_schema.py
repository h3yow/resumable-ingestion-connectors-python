"""并发摄入与结构变更并存：任意时刻只见完整版本，失败变更整体不生效。"""
import json
import threading

import pytest

from app.checkpoint import OffsetRegressionError
from app.connectors.jsonl import JsonlConnector
from app.pipeline import Pipeline, connect_db
from app.schema_versions import names_to_spec
from app.sink import SchemaEvolutionError

SCHEMA = {"id": str, "name": str, "age": int}
N = 12000          # 上万条记录
CONSUMERS = 4
BATCH = 200
COMPAT_CHANGES = 6


def _make(db_path):
    p = Pipeline(connect_db(str(db_path)), dict(SCHEMA))
    p.init()
    return p


def test_concurrent_ingest_with_schema_evolution(tmp_path, caplog):
    src = tmp_path / "big.jsonl"
    src.write_text(
        "\n".join(json.dumps({"id": f"r{i}", "name": f"n{i}", "age": i % 100})
                  for i in range(N)) + "\n",
        encoding="utf-8",
    )
    db_path = tmp_path / "ingest.db"
    _make(db_path)._db.close()  # 建表

    violations: list[str] = []
    stop_monitor = threading.Event()

    def consumer(tag):
        p = _make(db_path)
        try:
            while p.checkpoints.get("shared").last_offset < N - 1:
                try:
                    p.run_once(JsonlConnector(str(src), name="shared"),
                               batch_size=BATCH)
                except OffsetRegressionError:
                    continue  # 位点竞争失败：从新位点重新读，不从中间继续
        finally:
            p._db.close()

    def schema_changer():
        p = _make(db_path)
        try:
            for i in range(COMPAT_CHANGES):
                current = p.sink.registry.current()
                spec = names_to_spec(current.fields)
                spec[f"extra_{i}"] = str
                p.apply_schema(spec)  # 兼容：加列 -> 新版本
                # 不兼容：删列必须被明确拒绝
                bad = {k: v for k, v in spec.items() if k != "age"}
                with pytest.raises(SchemaEvolutionError):
                    p.apply_schema(bad)
        finally:
            p._db.close()

    def monitor():
        """采样：任何时刻 declared_fields 都必须等于某个完整已生效版本。"""
        p = _make(db_path)
        try:
            while not stop_monitor.is_set():
                declared = {n: t for n, t in p._db.execute(
                    "SELECT name, type FROM declared_fields").fetchall()}
                versions = {tuple(sorted(v.fields.items()))
                            for v in p.sink.registry.versions()}
                if declared and tuple(sorted(declared.items())) not in versions:
                    violations.append(f"中间状态: {sorted(declared)}")
        finally:
            p._db.close()

    threads = ([threading.Thread(target=consumer, args=(f"c{i}",))
                for i in range(CONSUMERS)]
               + [threading.Thread(target=schema_changer),
                  threading.Thread(target=monitor)])
    for t in threads:
        t.start()
    for t in threads[:-1]:
        t.join()
    stop_monitor.set()
    threads[-1].join()

    verify = _make(db_path)
    rows = verify.sink.fetch_all()
    # 不重复、不丢失
    assert len(rows) == N
    assert len({r["record_id"] for r in rows}) == N
    assert verify.checkpoints.get("shared").last_offset == N - 1
    # 结构版本：初始 v1 + 每次兼容变更接续一个新版本，不跳号
    versions = [v.version for v in verify.sink.registry.versions()]
    assert versions == list(range(1, COMPAT_CHANGES + 2))
    # 不兼容尝试全部被拒且留痕
    rejected = [e for e in verify.sink.registry.history() if e.status == "rejected"]
    assert len(rejected) == COMPAT_CHANGES
    # 从未观察到改了一半的中间结构
    assert violations == []

    # 静默期验证：失败的变更整体不生效，位点和数据停在变更前
    before_cp = verify.checkpoints.get("shared").last_offset
    before_rows = len(verify.sink.fetch_all())
    before_version = verify.sink.registry.current().version
    with pytest.raises(SchemaEvolutionError):
        verify.apply_schema({"id": str, "name": str})
    assert verify.checkpoints.get("shared").last_offset == before_cp
    assert len(verify.sink.fetch_all()) == before_rows
    assert verify.sink.registry.current().version == before_version

    print("行数:", len(rows), "位点:", before_cp, "版本:", versions)
    print("被拒结构变更:", [(e.seq, e.detail) for e in rejected][:3], "...")
    messages = [r.getMessage() for r in caplog.records]
    assert any("offsets=" in m and "checkpoint=" in m for m in messages)
    assert any("schema change applied: version=" in m for m in messages)
    assert any("schema change rejected" in m for m in messages)
