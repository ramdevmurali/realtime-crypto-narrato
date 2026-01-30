import pytest


@pytest.mark.asyncio
async def test_alerts(client, seed_data):
    resp = await client.get("/alerts", params={"limit": 5})
    assert resp.status_code == 200
    items = resp.json()
    assert any(item["symbol"] == "testcoin" and item["summary"] == "test summary" for item in items)
