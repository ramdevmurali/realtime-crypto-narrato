import asyncio
import json
from datetime import datetime, timezone

from fastapi.testclient import TestClient

from backend.app import main, db


def test_alerts(monkeypatch):
    calls = {}

    async def fake_fetch_alerts(limit):
        calls["limit"] = limit
        return [
            {
                "time": "2026-01-27T12:00:00Z",
                "symbol": "btc",
                "window": "1m",
                "direction": "down",
                "return": -0.05,
                "threshold": 0.05,
                "summary": "btc -5% in 1m",
                "headline": "Breaking",
                "sentiment": -0.3,
            }
        ]

    monkeypatch.setattr(db, "fetch_alerts", fake_fetch_alerts)

    client = TestClient(main.app)
    resp = client.get("/alerts", params={"limit": 4})
    assert resp.status_code == 200
    body = resp.json()
    assert calls["limit"] == 4
    assert isinstance(body, list) and len(body) == 1
    expected_keys = {"time", "symbol", "window", "direction", "return", "threshold", "summary", "headline", "sentiment"}
    assert set(body[0].keys()) == expected_keys


def test_alerts_stream_payload(monkeypatch):
    ts = datetime(2026, 1, 27, 12, 0, 0, tzinfo=timezone.utc)

    async def fake_fetch_alerts(limit):
        return [
            {
                "time": ts,
                "symbol": "btc",
                "window": "1m",
                "direction": "down",
                "return": -0.05,
                "threshold": 0.05,
                "summary": "btc -5% in 1m",
                "headline": "Breaking",
                "sentiment": -0.3,
            }
        ]

    monkeypatch.setattr(db, "fetch_alerts", fake_fetch_alerts)

    async def _get_one_event():
        gen = main._alerts_event_generator(limit=1, interval=0.01)
        return await gen.__anext__()

    payload = asyncio.run(_get_one_event())
    assert payload.startswith("data: ")
    data = json.loads(payload[len("data: ") :].strip())
    assert data["count"] == 1
    item = data["items"][0]
    assert item["time"] == ts.isoformat()
    expected_keys = {"time", "symbol", "window", "direction", "return", "threshold", "summary", "headline", "sentiment"}
    assert set(item.keys()) == expected_keys
