import types
import pytest
from fastapi.testclient import TestClient

from backend.app import main, db


class DummyPool:
    def __init__(self, val):
        self.val = val

    async def fetchval(self, *_args, **_kwargs):
        return self.val


def test_health_ok(monkeypatch):
    async def fake_get_pool():
        return DummyPool(1)

    monkeypatch.setattr(db, "get_pool", fake_get_pool)

    client = TestClient(main.app)
    resp = client.get("/health")
    assert resp.status_code == 200
    assert resp.json() == {"status": "ok", "db": 1}


def test_health_failure(monkeypatch):
    class FailingPool:
        async def fetchval(self, *_args, **_kwargs):
            raise RuntimeError("db down")

    async def fake_get_pool():
        return FailingPool()

    monkeypatch.setattr(db, "get_pool", fake_get_pool)

    client = TestClient(main.app)
    resp = client.get("/health")
    assert resp.status_code == 503
    assert "db_unhealthy" in resp.json()["detail"]

def test_prices_endpoint(monkeypatch):
    calls = {}

    async def fake_fetch_prices(symbol, limit):
        calls["symbol"] = symbol
        calls["limit"] = limit
        return [
            {"time": "2026-01-27T12:00:00Z", "symbol": symbol, "price": 123.45},
        ]

    monkeypatch.setattr(db, "fetch_prices", fake_fetch_prices)

    client = TestClient(main.app)
    resp = client.get("/prices", params={"symbol": "BTCUSDT", "limit": 3})
    assert resp.status_code == 200
    body = resp.json()
    assert calls["symbol"] == "btcusdt"
    assert calls["limit"] == 3
    assert isinstance(body, list) and len(body) == 1
    assert set(body[0].keys()) == {"time", "symbol", "price"}
