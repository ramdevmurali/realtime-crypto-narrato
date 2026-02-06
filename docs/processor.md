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
- Bad price guards: JSON decode errors are counted, warning is rate-limited (first + every 50th), and the raw payload is sent to a price DLQ topic; processing continues.

## Modules
- `config.py` — pydantic settings (DB URL, Kafka brokers/topics, Binance stream, symbols, RSS, thresholds, LLM keys). CSV env parsing for brokers/symbols.
  - DLQ knob: `price_dlq_topic` (default `prices-deadletter`).
- `db.py` — asyncpg pool; creates Timescale hypertables (prices, metrics, headlines, anomalies); insert helpers.
- `utils.py` — now_utc, simple_sentiment stub, llm_summarize (stub/OpenAI/Gemini), backoff helpers (`sleep_backoff`, `with_retries`).
- `windows.py` — in-memory PriceWindow: add/prune (max window + resample step), strict-window returns/vol, keeps smoothed z-score state.
- `metrics.py` — computes rolling returns/vol per symbol from PriceWindow; emits raw return z-scores, EWMA-smoothed return z-scores, volatility z-scores + spike flags, and 5th/95th return percentiles per window.
- `anomaly.py` — threshold checks, rate-limit (60s), direction, summarizes, persists + publishes alerts.
- `ingest.py` — tasks:
  - `price_ingest_task`: Binance WS → Kafka `prices` → Timescale `prices`.
  - `news_ingest_task`: RSS → dedupe → sentiment stub → Kafka `news` → Timescale `headlines`.
  - Includes backoff/jitter, counters, graceful cancel.
- `app.py` (entrypoint: `python -m src.app`) — orchestrator: healthcheck DB/Kafka, start producer/consumer, run process_prices_task, manage state (price windows, last alert, latest headline).
- `main.py` — removed in favor of the single entrypoint `python -m src.app`.

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

## Topic payloads (required fields)
Canonical payload models live in `processor/src/models/messages.py`.
- `prices`: `symbol`, `price`, `time`
- `news`: `time`, `title`, `source`, `sentiment` (optional: `url`)
- `summaries` (summary-request): `time`, `symbol`, `window`, `direction`, `ret`, `threshold` (optional: `headline`, `sentiment`)
- `alerts`: `time`, `symbol`, `window`, `direction`, `ret`, `threshold`, `summary` (optional: `headline`, `sentiment`)

Non-goals for now:
- No schema registry (JSON/Avro) yet; models + tests enforce contracts.

## Interfaces (module inputs/outputs)
- `ingest.py`: inputs — Binance WS ticks + RSS feed; outputs — Kafka `prices`/`news`, Timescale `prices`/`headlines`, updates `processor.latest_headline`.
- `metrics.py`: inputs — `PriceWindow` state per symbol; outputs — metrics dict (returns/vol/z/percentiles) for the current tick.
- `anomaly.py`: inputs — symbol, timestamp, metrics; outputs — Timescale `anomalies`, Kafka `alerts`, Kafka `summaries` request.
- `app.py`: orchestrator; owns Kafka producer/consumer, `price_windows`, `last_alert`, `latest_headline`, DLQ counters.
- `summary_sidecar.py`: inputs — Kafka `summaries` requests; outputs — Timescale `anomalies` summary upsert, Kafka `alerts` enriched.

## How to run
```
cd infra
docker compose up -d redpanda timescaledb processor summary-sidecar backend   # containers use PYTHONPATH=/app
```
Local/tests PYTHONPATH: `PYTHONPATH=processor/src:.`

SSE stream for headlines (rudimentary sentiment):
```
curl -N 'http://localhost:8000/headlines/stream?limit=5&interval=2'
```

## Tests (processor)
- Unit coverage for PriceWindow (prune/returns/vol), metrics propagation, anomaly triggers/rate limit/direction, ingest dedupe/news processing.
- Integration coverage (Kafka + Timescale) for happy path, retry path, and DLQ routing (see `scripts/integration_test.sh`).

## Window/Retention Policy
- Window labels and durations are configured via `WINDOW_LABELS` (default `1m,5m,15m`).
- Prune horizon is derived from the largest window plus the resample step:
  `max_window + VOL_RESAMPLE_SEC`.
- Returns/vol are **strict-window**: prices outside the window are not used.
- Gap handling is controlled by `WINDOW_MAX_GAP_FACTOR` but is clamped to the
  window size to prevent older spillover.

## Runtime Policies
- Late message tolerance: `LATE_PRICE_TOLERANCE_SEC` (drop if older than last seen minus tolerance).
- Anomaly cooldown: `ANOMALY_COOLDOWN_SEC` (per symbol+window).
- Log cadence: `BAD_PRICE_LOG_EVERY`, `LATE_PRICE_LOG_EVERY`,
  `PRICE_PUBLISH_LOG_EVERY`, `NEWS_PUBLISH_LOG_EVERY`.
- Backoff tuning:
  `PRICE_BACKOFF_*`, `NEWS_BACKOFF_*`, `RETRY_MAX_ATTEMPTS`,
  `RETRY_BACKOFF_BASE_SEC`, `RETRY_BACKOFF_CAP_SEC`.
- RSS dedupe: `RSS_SEEN_TTL_SEC`, `RSS_SEEN_MAX`.
