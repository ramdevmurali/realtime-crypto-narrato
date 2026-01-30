from fastapi.testclient import TestClient

from backend.app import main, db


def test_headlines(monkeypatch):
    calls = {}

    async def fake_fetch_headlines(limit):
        calls["limit"] = limit
        return [
            {
                "time": "2026-01-27T12:00:00Z",
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
