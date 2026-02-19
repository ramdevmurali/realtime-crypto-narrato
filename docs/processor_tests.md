# Processor tests cheat sheet

## Coverage by file
- `test_windows.py`
  - PriceWindow helper
  - Prunes by max window + resample step
  - Strict-window returns/vol (no spillover outside window)
  - Returns 1m/5m/15m happy path and None when insufficient history or stale data
  - Volatility: None with <3 points; resampled stddev matches expected (pytest.approx)
  - Out-of-order insertions and future timestamps handled correctly

- `test_metrics.py`
  - compute_metrics glue on PriceWindow
  - None when only latest tick
  - Attention = max(|return|/threshold)
  - Returns and vol propagate exactly (pytest.approx)
  - Raw return z-scores computed over intra-window returns
  - EWMA return z-scores smooth raw z, cap at ±6, None when no raw z
  - Vol z-scores per window and spike flags when z exceeds threshold
  - Return percentile bands (p05/p95) per window with None when insufficient data
  - Math validation: constant/monotonic/mixed series, scale/shift invariants
  - Numpy reference check for returns/vol/percentiles (skips if numpy missing)

- `test_anomaly.py`
  - Alert trigger logic (no real Kafka/DB)
  - Fires on threshold cross; no fire below threshold
  - 60s rate limit enforced
  - Direction up/down correct
  - Payload carries latest headline/sentiment
  - Empty metrics → no alert, state unchanged
  - Each alert emits two Kafka messages: summary-request to `summaries` and the alert to `alerts`.

- `test_ingest.py`
  - Ingest helpers without real feeds
  - build_news_msg/publish_news_msg dedupe and publish path (publish/insert once)
  - price_ingest_task publishes one price from a mocked miniTicker websocket
- `test_summary_sidecar.py`
  - Sidecar handler consumes summary request, calls LLM (stubbed), updates anomalies summary field, and republishes enriched alert to `alerts`.
- `test_sentiment_model.py`
  - Stub fallback for sentiment model wrapper
  - ONNX path missing → fallback behavior
- `test_sentiment_sidecar.py`
  - Enriches news sentiment, upserts headlines, publishes `news-enriched`
  - DLQ path on failure + commit
  - `news-enriched` is produced by the sentiment sidecar (not the processor)
- `test_config.py`
  - Config validation: window labels, llm provider, percentiles, positive values
- `test_anomaly_decision.py`
  - Pure anomaly decision logic (detect_anomalies) without side effects
- `test_streaming_core_metrics.py`
  - /metrics handler for processor returns JSON snapshot
- `test_utils.py`
  - Retry/backoff helpers and ISO datetime parsing
- `test_migrate_db.py`
  - Migration DDL/retention policy logic (unit-level)
- `integration/test_e2e.py`
  - Kafka+DB happy path (prices -> metrics/anomalies)
  - Retry path (transient DB error -> eventual success)
  - DLQ path (invalid price -> deadletter + commit)
- `integration/test_sentiment_sidecar.py`
  - News message → headline sentiment overwrite + `news-enriched` publish

## How to run
From repo root or `processor/`:
```
python3 -m pytest -q processor/tests
```
Notes:
- Uses pytest-asyncio for async cases.
- Unit tests require no external services; Kafka/DB are mocked.

## Smoke test (unit + integration)
From repo root:
```
make smoke-test
```
Notes:
- Runs unit tests first, then integration tests with `RUN_INTEGRATION=1`.

## Integration tests
Requires docker compose services:
```
scripts/integration_test.sh
```
Notes:
- Runs against redpanda + timescaledb from `infra/docker-compose.yml`.
- Set `RUN_INTEGRATION=1` or run `-m integration` directly to include these tests.
