# Processor tests

- `test_windows.py`
  - What it is: checks the PriceWindow helper.
  - Tests:
    - prunes old points beyond ~16m.
    - get_return: builds a fake timeline with prices at -15m, -5m, -1m, and now, then checks that the 1m/5m/15m returns match the expected percentages; also proves it returns None when you only have the latest tick (no history).
    - get_vol: feeds a short series of prices; with fewer than 3 points it must return None, and with enough ticks it should spit out a positive stddev that matches the hand-calculated value (using pytest.approx).

- `test_metrics.py`
  - What it is: checks compute_metrics glue on top of PriceWindow.
  - Tests:
    - returns None when the window only has the latest tick (no history to compute returns).
    - attention scales off the biggest return/threshold ratio (e.g., 10% over 5m vs 8% threshold → 1.25).
    - propagates both returns and volatility exactly as PriceWindow computes them (hand-calculated return/vol matched with pytest.approx).

- `test_anomaly.py`
  - What it is: checks alert triggering logic end-to-end (without real Kafka/DB).
  - Tests:
    - triggers and updates state when return crosses threshold (last_alert set, alert sent, anomaly insert called).
    - below threshold → no alert, no insert, no last_alert entry.
    - rate limit: second alert within 60s is suppressed.
    - direction field flips correctly for positive vs negative returns.
    - payload carries latest headline + sentiment when present.
    - empty metrics → no alert, no insert, last_alert untouched.

- `test_ingest.py`
  - What it is: checks ingest helpers without hitting real feeds.
  - Tests:
    - process_feed_entry dedupes via seen_ids: first time publishes + inserts, repeat id skips both.
    - price_ingest_task publishes one price message when the websocket yields a single miniTicker (mocked connect).

## How to run
- `python3 -m pytest -q processor/tests`
- Async tests use pytest-asyncio.
