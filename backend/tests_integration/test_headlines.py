import pytest


@pytest.mark.asyncio
async def test_headlines(client, seed_data):
    resp = await client.get("/headlines", params={"limit": 5})
    assert resp.status_code == 200
    items = resp.json()
    assert any(item["title"] == "Test headline" and item["sentiment"] == -0.2 for item in items)
