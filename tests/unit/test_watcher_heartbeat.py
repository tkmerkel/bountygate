import os

import pytest
from sqlalchemy import create_engine, text

from bountygate.watcher_heartbeat import heartbeat


@pytest.fixture
def engine():
    url = os.environ["DATABASE_URL"]
    if url.startswith("postgres://"):
        url = url.replace("postgres://", "postgresql+psycopg2://", 1)
    e = create_engine(url)
    with e.begin() as c:
        c.execute(text("DELETE FROM watcher_heartbeats WHERE name LIKE 'test-%'"))
    yield e
    with e.begin() as c:
        c.execute(text("DELETE FROM watcher_heartbeats WHERE name LIKE 'test-%'"))


def test_heartbeat_inserts_first_call(engine):
    heartbeat(
        "test-w1",
        is_running=True,
        pending_count=0,
        expected_interval_s=60,
    )
    with engine.connect() as c:
        row = c.execute(
            text("SELECT name, is_running, pending_count, expected_interval_s FROM watcher_heartbeats WHERE name='test-w1'")
        ).one()
    assert row.name == "test-w1"
    assert row.is_running is True
    assert row.pending_count == 0
    assert row.expected_interval_s == 60


def test_heartbeat_upserts_on_subsequent_calls(engine):
    heartbeat("test-w2", is_running=True, pending_count=0, expected_interval_s=60)
    heartbeat("test-w2", is_running=False, pending_count=3, expected_interval_s=60, last_error="boom")
    with engine.connect() as c:
        row = c.execute(
            text("SELECT is_running, pending_count, last_error FROM watcher_heartbeats WHERE name='test-w2'")
        ).one()
    assert row.is_running is False
    assert row.pending_count == 3
    assert row.last_error == "boom"
