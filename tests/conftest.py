import logging
import pytest

from app.pipeline import Pipeline, connect_db

SCHEMA = {"id": str, "name": str, "age": int}


@pytest.fixture()
def db(tmp_path):
    conn = connect_db(str(tmp_path / "ingest.db"))
    yield conn
    conn.close()


@pytest.fixture()
def pipeline(db):
    p = Pipeline(db, dict(SCHEMA))
    p.init()
    return p


@pytest.fixture(autouse=True)
def pipeline_logs(caplog):
    """单测日志中打印位点与变更流水。"""
    with caplog.at_level(logging.INFO, logger="pipeline"):
        yield caplog
