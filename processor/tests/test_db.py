import pytest

from processor.src.io import db


class FakeConn:
    def __init__(self, fetchval_result=None):
        self.fetchval_result = fetchval_result
        self.executed = []

    async def fetchval(self, sql, *args):
        self.executed.append(sql)
        return self.fetchval_result

    async def execute(self, sql, *args):
        self.executed.append(sql)


class FakeAcquire:
    def __init__(self, conn):
        self.conn = conn

    async def __aenter__(self):
        return self.conn

    async def __aexit__(self, exc_type, exc, tb):
        return False


class FakePool:
    def __init__(self, conn):
        self.conn = conn

    def acquire(self):
        return FakeAcquire(self.conn)


@pytest.mark.asyncio
async def test_insert_price_returns_bool(monkeypatch):
    conn = FakeConn(fetchval_result=1)
    pool = FakePool(conn)

    async def fake_get_pool():
        return pool

    monkeypatch.setattr(db, "get_pool", fake_get_pool)

    inserted = await db.insert_price("2026-01-27T12:00:00Z", "btcusdt", 100.0)
    assert inserted is True
    assert any("RETURNING 1" in sql for sql in conn.executed)

    conn2 = FakeConn(fetchval_result=None)
    pool2 = FakePool(conn2)

    async def fake_get_pool2():
        return pool2

    monkeypatch.setattr(db, "get_pool", fake_get_pool2)

    inserted = await db.insert_price("2026-01-27T12:00:01Z", "btcusdt", 100.0)
    assert inserted is False


@pytest.mark.asyncio
async def test_insert_metric_uses_upsert(monkeypatch):
    conn = FakeConn()
    pool = FakePool(conn)

    async def fake_get_pool():
        return pool

    monkeypatch.setattr(db, "get_pool", fake_get_pool)

    await db.insert_metric("2026-01-27T12:00:00Z", "btcusdt", {"return_1m": 0.01})
    assert any("ON CONFLICT (time, symbol) DO UPDATE" in sql for sql in conn.executed)
