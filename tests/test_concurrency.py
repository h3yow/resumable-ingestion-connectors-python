"""并发消费：目标端不重复、不丢失，位点竞争失败者被显式拒绝。"""
import json
import threading

from app.checkpoint import OffsetRegressionError
from app.connectors.jsonl import JsonlConnector
from app.pipeline import Pipeline, connect_db

SCHEMA = {"id": str, "name": str, "age": int}


class BarrierConnector:
    """包装连接器：read 时在屏障处等待，保证所有线程读到相同起始位点后再竞争。"""

    def __init__(self, inner, barrier):
        self._inner = inner
        self._barrier = barrier

    @property
    def name(self):
        return self._inner.name

    def read(self, from_offset):
        self._barrier.wait(timeout=10)
        yield from self._inner.read(from_offset)


def test_concurrent_consumption_no_duplicates_no_loss(tmp_path, caplog):
    n = 20
    src = tmp_path / "src.jsonl"
    src.write_text(
        "\n".join(json.dumps({"id": f"r{i}", "name": f"n{i}", "age": i}) for i in range(n)) + "\n",
        encoding="utf-8",
    )
    db_path = tmp_path / "ingest.db"

    def make_pipeline():
        # 每线程独立连接，busy timeout 避免写锁瞬时冲突
        p = Pipeline(connect_db(str(db_path)), dict(SCHEMA))
        p.init()
        return p

    make_pipeline().init()  # 建表

    outcomes: list[tuple[str, object]] = []

    barrier = threading.Barrier(4)

    def worker(tag):
        p = make_pipeline()
        connector = BarrierConnector(JsonlConnector(str(src), name="shared"), barrier)
        try:
            result = p.run_once(connector, batch_size=n)
            outcomes.append((tag, ("ok", result.accepted)))
        except OffsetRegressionError as exc:
            outcomes.append((tag, ("regression_rejected", str(exc))))
        finally:
            p._db.close()

    threads = [threading.Thread(target=worker, args=(f"t{i}",)) for i in range(4)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    # 至少一个线程成功；竞争到位点的线程被显式拒绝而非从中间继续
    statuses = [s for _, (s, *_) in outcomes]
    assert "ok" in statuses
    assert "regression_rejected" in statuses

    # 目标端不重复、不丢失
    verify = make_pipeline()
    rows = verify.sink.fetch_all()
    assert len(rows) == n
    assert len({r["record_id"] for r in rows}) == n
    assert verify.checkpoints.get("shared").last_offset == n - 1

    print("并发结果:", outcomes)
    print("最终位点:", verify.checkpoints.get("shared").last_offset, "行数:", len(rows))
    print("变更流水:", [(c.seq, c.kind.value, c.column) for c in verify.sink.change_log()])
