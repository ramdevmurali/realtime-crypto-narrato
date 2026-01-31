# Processor (streaming layer) cheat sheet

## What it is
- Async ingest + compute service.
- Streams price ticks (Binance WS) and news (RSS), computes rolling metrics, detects anomalies, persists to Timescale, and publishes to Kafka.

## Resilience (built-in)
- Backoff + jitter on reconnects; retries with backoff for DB/Kafka.
- Circuit-breaker counters to lengthen backoff after repeated failures.
- Graceful cancellation of tasks; startup healthcheck for DB/Kafka.
- Structured logging and lightweight counters (price/news publishes, alerts).
- Deduped price inserts via `ON CONFLICT (time, symbol) DO NOTHING`.

## Modules
- `config.py` — pydantic settings (DB URL, Kafka brokers/topics, Binance stream, symbols, RSS, thresholds, LLM keys). CSV env parsing for brokers/symbols.
- `db.py` — asyncpg pool; creates Timescale hypertables (prices, metrics, headlines, anomalies); insert helpers.
- `utils.py` — now_utc, simple_sentiment stub, llm_summarize (stub/OpenAI/Gemini), backoff helpers (`sleep_backoff`, `with_retries`).
- `windows.py` — in-memory PriceWindow: add/prune (>16m), returns (1m/5m/15m), volatility per window, keeps smoothed z-score state.
- `metrics.py` — computes rolling returns/vol per symbol from PriceWindow; emits raw return z-scores, EWMA-smoothed return z-scores, volatility z-scores + spike flags, and 5th/95th return percentiles per window.
- `anomaly.py` — threshold checks, rate-limit (60s), direction, summarizes, persists + publishes alerts.
- `ingest.py` — tasks:
  - `price_ingest_task`: Binance WS → Kafka `prices` → Timescale `prices`.
  - `news_ingest_task`: RSS → dedupe → sentiment stub → Kafka `news` → Timescale `headlines`.
  - Includes backoff/jitter, counters, graceful cancel.
- `processor.py` — orchestrator: healthcheck DB/Kafka, start producer/consumer, run process_prices_task, manage state (price windows, last alert, latest headline).
- `main.py` — entrypoint (`python -m src.processor`).

## Data written to Timescale
- `prices(time, symbol, price, PK (time, symbol))`
- `metrics(time, symbol, return_1m/5m/15m, vol_1m/5m/15m, return_z_1m/5m/15m, return_z_ewma_1m/5m/15m, vol_z_1m/5m/15m, vol_spike_1m/5m/15m, p05_return_1m/5m/15m, p95_return_1m/5m/15m, attention)`
- `headlines(time, title, source, url, sentiment)`
- `anomalies(time, symbol, window_name, direction, return_value, threshold, headline, sentiment, summary)`

## Alert path and summaries
- Threshold check & cooldown happen in `anomaly.py`; if tripped, the alert is persisted to Timescale and published to the `alerts` topic.
- The processor no longer calls the LLM in the hot path. It publishes a summary-request message to the `summaries` topic with `{time, symbol, window, direction, ret, threshold, headline, sentiment}`. Alerts still carry the base/stub summary.
- A future sidecar can consume `summaries`, call the configured LLM, and backfill richer summaries asynchronously.
  - Sidecar knobs: `SUMMARY_CONSUMER_GROUP`, `SUMMARY_POLL_TIMEOUT_MS`, `SUMMARY_BATCH_MAX` (optional) control consumption behavior.

## How to run
```
cd infra
docker compose up -d processor redpanda timescaledb
```

## Tests (processor)
- Unit coverage for PriceWindow (prune/returns/vol), metrics propagation, anomaly triggers/rate limit/direction, ingest dedupe/news processing.
