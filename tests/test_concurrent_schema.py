"""并发摄入中的结构变更：任意时刻只观察到完整版本，变更失败整体不生效，不重复不丢。"""
import json
import threading

from app.checkpoint import OffsetRegressionError
from app.connectors.jsonl import JsonlConnector
from app.models import SchemaStatus
from app.pipeline import Pipeline, connect_db
from app.sink import SchemaEvolutionError

SCHEMA = {"id": str, "name": str, "age": int}
N = 12000
CONSUMERS = 4


def test_schema_change_during_concurrent_ingest(tmp_path):
    src = tmp_path / "src.jsonl"
    src.write_text(
        "\n".join(json.dumps({"id": f"r{i}", "name": f"n{i}", "age": i})
                  for i in range(N)) + "\n",
        encoding="utf-8",
    )
    db_path = str(tmp_path / "ingest.db")

    def make_pipeline():
        p = Pipeline(connect_db(db_path), dict(SCHEMA))
        p.init()
        return p

    make_pipeline().init()  # 建表与 v1

    errors: list[tuple[str, Exception]] = []
    outcomes: list[tuple[str, str]] = []
    ingest_started = threading.Event()

    def consumer(tag):
        p = make_pipeline()
        connector = JsonlConnector(str(src), name="shared")
        try:
            while True:
                try:
                    result = p.run_once(connector, batch_size=200)
                except OffsetRegressionError:
                    continue  # 位点竞争失败：显式拒绝后重读位点继续
                cp = result.checkpoint
                if cp is not None and cp.last_offset >= N - 1:
                    break
                ingest_started.set()
        except Exception as exc:
            errors.append((tag, exc))
        finally:
            p._db.close()

    def schema_changer():
        # 等摄入进行到一定量之后再发起结构变更
        watcher = make_pipeline()
        try:
            while watcher.checkpoints.get("shared").last_offset < N // 4:
                if not ingest_started.wait(timeout=0.01):
                    pass
            entry = watcher.apply_schema({**SCHEMA, "email": str})  # 兼容 -> v2
            outcomes.append(("applied", f"v{entry.version}"))
            try:
                watcher.apply_schema({"id": str, "name": str})      # 不兼容 -> 拒绝
            except SchemaEvolutionError as exc:
                outcomes.append(("rejected", str(exc)))
        except Exception as exc:
            errors.append(("changer", exc))
        finally:
            watcher._db.close()

    consistency_violations: list[str] = []

    def schema_reader():
        # 变更期间反复读取：任何时刻版本与字段必须是某个完整版本
        p = make_pipeline()
        try:
            while not done.is_set():
                p._db.execute("BEGIN")
                try:
                    version = p.sink.registry.current_version()
                    fields = p.sink.registry.current_fields()
                    declared = {r[0] for r in p._db.execute(
                        "SELECT name FROM declared_fields").fetchall()}
                finally:
                    p._db.execute("COMMIT")
                expected = (dict(SCHEMA) if version == 1
                            else {**SCHEMA, "email": str})
                expected_names = {k: v.__name__ for k, v in expected.items()}
                if version not in (1, 2) or fields != expected_names:
                    consistency_violations.append(f"v{version} fields={fields}")
                if declared != set(expected_names):
                    consistency_violations.append(
                        f"v{version} declared={sorted(declared)}")
        finally:
            p._db.close()

    done = threading.Event()
    threads = ([threading.Thread(target=consumer, args=(f"c{i}",))
                for i in range(CONSUMERS)]
               + [threading.Thread(target=schema_changer),
                  threading.Thread(target=schema_reader)])
    for t in threads:
        t.start()
    for t in threads[:CONSUMERS + 1]:
        t.join()
    done.set()
    threads[-1].join()

    assert not errors, f"并发执行出现异常: {errors}"
    assert not consistency_violations, f"观察到中间态结构: {consistency_violations[:5]}"
    assert ("applied", "v2") in outcomes
    assert any(o[0] == "rejected" and "drop" in o[1] for o in outcomes)

    # 不重复、不丢失；位点到底
    verify = make_pipeline()
    rows = verify.sink.fetch_all()
    assert len(rows) == N
    assert len({r["record_id"] for r in rows}) == N
    assert verify.checkpoints.get("shared").last_offset == N - 1

    # 变更结果：v2 生效，拒绝留痕，结构停在完整 v2
    assert verify.sink.registry.current_version() == 2
    history = verify.sink.registry.history()
    assert [(e.version, e.status) for e in history] == [
        (1, SchemaStatus.APPLIED), (2, SchemaStatus.APPLIED),
        (None, SchemaStatus.REJECTED),
    ]
    assert verify.sink.registry.current_fields() == {
        "id": "str", "name": "str", "age": "int", "email": "str",
    }
    print("并发摄入+结构变更: 行数=%d 位点=%d 版本=v%d 结果=%s" % (
        len(rows), verify.checkpoints.get("shared").last_offset,
        verify.sink.registry.current_version(), outcomes,
    ))
