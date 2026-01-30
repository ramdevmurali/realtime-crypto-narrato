from fastapi.testclient import TestClient

from backend.app import main, db


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
