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
