from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Query
import uvicorn

from .config import settings
from . import db


@asynccontextmanager
async def lifespan(_app: FastAPI):
    await db.get_pool()
    try:
        yield
    finally:
        await db.close_pool()


app = FastAPI(title="Realtime Crypto Backend", lifespan=lifespan)


@app.get("/health")
async def health():
    try:
        pool = await db.get_pool()
        val = await pool.fetchval("SELECT 1;")
        return {"status": "ok", "db": val}
    except Exception as exc:  # pragma: no cover
        raise HTTPException(status_code=503, detail=f"db_unhealthy: {exc}")


@app.get("/prices")
async def get_prices(
    symbol: str = Query(..., description="symbol, e.g. btcusdt"),
    limit: int = Query(200, ge=1, le=2000),
):
    rows = await db.fetch_prices(symbol.lower(), limit)
    return [
        {"time": r["time"], "symbol": r["symbol"], "price": float(r["price"])}
        for r in rows
    ]


@app.get("/metrics/latest")
async def get_latest_metrics(symbol: str = Query(..., description="symbol, e.g. btcusdt")):
    row = await db.fetch_latest_metrics(symbol.lower())
    if not row:
        raise HTTPException(status_code=404, detail="metrics_not_found")
    return {k: row[k] for k in row.keys()}


@app.get("/headlines")
async def get_headlines(limit: int = Query(20, ge=1, le=200)):
    rows = await db.fetch_headlines(limit)
    return [
        {
            "time": r["time"],
            "title": r["title"],
            "url": r["url"],
            "source": r["source"],
            "sentiment": r["sentiment"],
        }
        for r in rows
    ]


@app.get("/alerts")
async def get_alerts(limit: int = Query(20, ge=1, le=200)):
    rows = await db.fetch_alerts(limit)
    return [
        {
            "time": r["time"],
            "symbol": r["symbol"],
            "window": r["window"],
            "direction": r["direction"],
            "return": r["return"],
            "threshold": r["threshold"],
            "summary": r["summary"],
            "headline": r["headline"],
            "sentiment": r["sentiment"],
        }
        for r in rows
    ]


if __name__ == "__main__":
    uvicorn.run(
        "app.main:app",
        host=settings.api_host,
        port=settings.api_port,
        reload=True,
    )
