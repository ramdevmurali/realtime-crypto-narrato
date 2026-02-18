from datetime import datetime, timezone

from fastapi.testclient import TestClient

from backend.app import main, db


def test_headlines(monkeypatch):
    calls = {}
    ts = datetime(2026, 1, 27, 12, 0, 0, tzinfo=timezone.utc)

    async def fake_fetch_headlines(limit, since=None):
        calls["limit"] = limit
        return [
            {
                "time": ts,
                "title": "Breaking",
                "url": "http://example.com",
                "source": "rss",
                "sentiment": -0.1,
            }
        ]

    monkeypatch.setattr(db, "fetch_headlines", fake_fetch_headlines)

    client = TestClient(main.app)
    resp = client.get("/headlines", params={"limit": 5})
    assert resp.status_code == 200
    body = resp.json()
    assert calls["limit"] == 5
    assert isinstance(body, list) and len(body) == 1
    assert set(body[0].keys()) == {"time", "title", "url", "source", "sentiment"}
    assert body[0]["time"] == ts.isoformat()


def test_headlines_since_passed(monkeypatch):
    calls = {}
    expected_since = datetime(2026, 1, 27, 12, 0, 0, tzinfo=timezone.utc)

    async def fake_fetch_headlines(limit, since=None):
        calls["limit"] = limit
        calls["since"] = since
        return []

    monkeypatch.setattr(db, "fetch_headlines", fake_fetch_headlines)

    client = TestClient(main.app)
    resp = client.get("/headlines", params={"limit": 3, "since": "2026-01-27T12:00:00Z"})
    assert resp.status_code == 200
    assert calls["limit"] == 3
    assert calls["since"] == expected_since
