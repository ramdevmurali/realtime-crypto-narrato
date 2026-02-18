import asyncio
import json

import pytest


@pytest.mark.asyncio
async def test_alerts(client, seed_data):
    resp = await client.get("/alerts", params={"limit": 5})
    assert resp.status_code == 200
    items = resp.json()
    assert any(item["symbol"] == "testcoin" and item["summary"] == "test summary" for item in items)


@pytest.mark.asyncio
async def test_alerts_stream_emits_event(client, seed_data):
    async def _read_one_event():
        async with client.stream("GET", "/alerts/stream", params={"limit": 1, "interval": 0.5}) as resp:
            async for chunk in resp.aiter_text():
                line = next((l for l in chunk.splitlines() if l.startswith("data: ")), None)
                if line:
                    payload = json.loads(line[len("data: ") :])
                    return payload
        return None

    payload = await asyncio.wait_for(_read_one_event(), timeout=3.0)
    assert payload is not None, "no sse data received"
    assert payload["count"] == 1
    item = payload["items"][0]
    expected_keys = {"time", "symbol", "window", "direction", "return", "threshold", "summary", "headline", "sentiment"}
    assert set(item.keys()) == expected_keys
