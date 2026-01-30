# Processor tests cheat sheet

## Coverage by file
- `test_windows.py`
  - PriceWindow helper
  - Prunes >16m
  - Returns 1m/5m/15m happy path and None when insufficient history
  - Volatility: None with <3 points; positive stddev matches expected (pytest.approx)

- `test_metrics.py`
  - compute_metrics glue on PriceWindow
  - None when only latest tick
  - Attention = max(|return|/threshold)
  - Returns and vol propagate exactly (pytest.approx)
  - Raw return z-scores computed over intra-window returns
  - EWMA return z-scores smooth raw z, cap at ±6, None when no raw z

- `test_anomaly.py`
  - Alert trigger logic (no real Kafka/DB)
  - Fires on threshold cross; no fire below threshold
  - 60s rate limit enforced
  - Direction up/down correct
  - Payload carries latest headline/sentiment
  - Empty metrics → no alert, state unchanged

- `test_ingest.py`
  - Ingest helpers without real feeds
  - process_feed_entry dedupes via seen_ids (publish/insert once)
  - price_ingest_task publishes one price from a mocked miniTicker websocket

## How to run
From repo root or `processor/`:
```
python3 -m pytest -q processor/tests
```
Notes:
- Uses pytest-asyncio for async cases.
- No external services required; Kafka/DB are mocked.
