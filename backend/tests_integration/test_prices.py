import pytest


@pytest.mark.asyncio
async def test_prices(client, seed_data):
    resp = await client.get("/prices", params={"symbol": "testcoin", "limit": 5})
    assert resp.status_code == 200
    items = resp.json()
    assert len(items) == 3
    assert all(item["symbol"] == "testcoin" for item in items)
    times = [item["time"] for item in items]
    assert times == sorted(times, reverse=True)
