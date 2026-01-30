import pytest


@pytest.mark.asyncio
async def test_metrics_latest(client, seed_data):
    resp = await client.get("/metrics/latest", params={"symbol": "testcoin"})
    assert resp.status_code == 200
    body = resp.json()
    assert body["symbol"] == "testcoin"
    assert pytest.approx(body.get("return_1m", 0), rel=1e-6) == 0.05
