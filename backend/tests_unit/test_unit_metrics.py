from fastapi.testclient import TestClient

from backend.app import main, db


def test_metrics_latest_ok(monkeypatch):
    async def fake_fetch_latest_metrics(symbol):
        return {
            "time": "2026-01-27T12:00:00Z",
            "symbol": symbol,
            "return_1m": 0.02,
            "return_z_ewma_1m": 1.5,
            "vol_z_1m": 0.8,
            "vol_spike_1m": False,
            "p05_return_1m": -0.03,
            "p95_return_1m": 0.06,
        }

    monkeypatch.setattr(db, "fetch_latest_metrics", fake_fetch_latest_metrics)

    client = TestClient(main.app)
    resp = client.get("/metrics/latest", params={"symbol": "ETHUSDT"})
    assert resp.status_code == 200
    body = resp.json()
    assert body["symbol"] == "ethusdt"
    assert body["return_1m"] == 0.02
    assert body["return_z_ewma_1m"] == 1.5
    assert body["vol_z_1m"] == 0.8
    assert body["vol_spike_1m"] is False
    assert body["p05_return_1m"] == -0.03
    assert body["p95_return_1m"] == 0.06


def test_metrics_latest_not_found(monkeypatch):
    async def fake_fetch_latest_metrics(symbol):
        return None

    monkeypatch.setattr(db, "fetch_latest_metrics", fake_fetch_latest_metrics)

    client = TestClient(main.app)
    resp = client.get("/metrics/latest", params={"symbol": "XRPUSDT"})
    assert resp.status_code == 404
    assert resp.json()["detail"] == "metrics_not_found"
