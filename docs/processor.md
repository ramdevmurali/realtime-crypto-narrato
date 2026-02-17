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
- `io/db.py` — asyncpg pool; creates Timescale hypertables (prices, metrics, headlines, anomalies); insert helpers.
- `utils.py` — now_utc, simple_sentiment stub, llm_summarize (stub/OpenAI/Gemini), backoff helpers (`sleep_backoff`, `with_retries`).
- `domain/windows.py` — in-memory PriceWindow: add/prune (max window + resample step), strict-window returns/vol, keeps smoothed z-score state.
- `domain/metrics.py` — computes rolling returns/vol per symbol from PriceWindow; emits raw return z-scores, EWMA-smoothed return z-scores, volatility z-scores + spike flags, and 5th/95th return percentiles per window.
- `domain/anomaly.py` — threshold checks, rate-limit (60s), direction, summarizes, persists + publishes alerts.
- `services/ingest.py` — tasks:
  - `price_ingest_task`: Binance WS → Kafka `prices` → Timescale `prices`.
  - `news_ingest_task`: RSS → dedupe → sentiment stub → Kafka `news` → Timescale `headlines`.
  - Includes backoff/jitter, counters, graceful cancel.
- `services/price_consumer.py` — Kafka consume loop + commit policy.
- `services/price_pipeline.py` — DB + metrics + anomaly pipeline.
- `services/runtime.py` — startup config logging.
- `services/health.py` — DB/Kafka health checks.
- `services/summary_sidecar.py` — enriches alerts with LLM summaries from `summaries` topic.
- `services/sentiment_model.py` — sentiment inference wrapper (stub/onnx).
- `services/sentiment_sidecar.py` — enriches news sentiment and publishes `news-enriched`.
- `streaming_core.py` — orchestrator + shared state.
- `app.py` (entrypoint: `python -m src.app`) — thin entrypoint for `StreamProcessor`.

Note: legacy folders like `processor/src/models` and `processor/src/processor/services` are removed; use `io/models` and `services/` instead.

## Data written to Timescale
- `prices(time, symbol, price, PK (time, symbol))`
- `metrics(time, symbol, return_1m/5m/15m, vol_1m/5m/15m, return_z_1m/5m/15m, return_z_ewma_1m/5m/15m, vol_z_1m/5m/15m, vol_spike_1m/5m/15m, p05_return_1m/5m/15m, p95_return_1m/5m/15m, attention)`
- `headlines(time, title, source, url, sentiment)`
- `anomalies(time, symbol, window_name, direction, return_value, threshold, headline, sentiment, summary)`

## Alert path and summaries
- Threshold check & cooldown happen in `domain/anomaly.py`; if tripped, the alert is persisted to Timescale and published to the `alerts` topic.
- The processor no longer calls the LLM in the hot path. It publishes a summary-request message to the `summaries` topic with `{time, symbol, window, direction, ret, threshold, headline, sentiment}`. Alerts still carry the base/stub summary.
- The summary sidecar consumes `summaries`, calls the configured LLM, and backfills richer summaries asynchronously.
  - Sidecar knobs: `SUMMARY_CONSUMER_GROUP`, `SUMMARY_POLL_TIMEOUT_MS`, `SUMMARY_BATCH_MAX` (optional) control consumption behavior.

## Topic payloads (required fields)
Canonical payload models live in `processor/src/io/models/messages.py`.
- `prices`: `symbol`, `price`, `time`
- `news`: `time`, `title`, `source`, `sentiment` (optional: `url`)
- `summaries` (summary-request): `time`, `symbol`, `window`, `direction`, `ret`, `threshold` (optional: `headline`, `sentiment`)
- `alerts`: `time`, `symbol`, `window`, `direction`, `ret`, `threshold`, `summary` (optional: `headline`, `sentiment`)
- `news-enriched`: same as `news` plus optional `label`, `confidence` (sentiment sidecar output)
- `news-deadletter`: raw news messages that failed enrichment (poison-pill avoidance)

Sentiment fallback behavior: if the sentiment sidecar is down or errors, the raw `news` topic remains valid
and carries the stub sentiment; consumers can continue using `news` until `news-enriched` is available.

Non-goals for now:
- No schema registry (JSON/Avro) yet; models + tests enforce contracts.

## Interfaces (module inputs/outputs)
- `services/ingest.py`: inputs — Binance WS ticks + RSS feed; outputs — Kafka `prices`/`news`, Timescale `prices`/`headlines`, updates `processor.latest_headline`.
- `domain/metrics.py`: inputs — `PriceWindow` state per symbol; outputs — metrics dict (returns/vol/z/percentiles) for the current tick.
- `domain/anomaly.py`: inputs — symbol, timestamp, metrics; outputs — Timescale `anomalies`, Kafka `alerts`, Kafka `summaries` request.
- `streaming_core.py`: orchestrator; owns Kafka producer/consumer, `price_windows`, `last_alert`, `latest_headline`, DLQ counters.
- `services/summary_sidecar.py`: inputs — Kafka `summaries` requests; outputs — Timescale `anomalies` summary upsert, Kafka `alerts` enriched.
- `services/sentiment_sidecar.py`: inputs — Kafka `news`; outputs — Timescale `headlines` sentiment overwrite, Kafka `news-enriched`.

## How to run
```
cd infra
docker compose up -d redpanda timescaledb processor summary-sidecar sentiment-sidecar backend   # containers use PYTHONPATH=/app
```
Local/tests PYTHONPATH: `PYTHONPATH=processor/src:.`

Sentiment sidecar (local):
```
PYTHONPATH=processor/src:. .venv/bin/python -m src.services.sentiment_sidecar
```

Sentiment model assets (local ONNX):
- Place model files under `models/finbert/`:
  `model.onnx`, plus tokenizer files (`tokenizer.json` or `vocab.txt`, `config.json`, etc.).
- Model assets are not committed; download them separately and drop them in the folder.
- Docker compose mounts `../models/finbert` into `/models/finbert`.
- Set `SENTIMENT_MODEL_PATH=/models/finbert` (override in `.env` to switch models/paths).

Replay recent headlines into Kafka (for sidecar testing):
```
DATABASE_URL=postgres://postgres:postgres@localhost:5432/anomalies \
KAFKA_BROKERS_RAW=localhost:9092 \
PYTHONPATH=processor/src:. .venv/bin/python scripts/replay_headlines.py --since-hours 24 --limit 50
```

SSE stream for headlines (rudimentary sentiment):
```
curl -N 'http://localhost:8000/headlines/stream?limit=5&interval=2'
```

## Tests (processor)
- Unit coverage for PriceWindow (prune/returns/vol), metrics propagation, anomaly triggers/rate limit/direction, ingest dedupe/news processing.
- Integration coverage (Kafka + Timescale) for happy path, retry path, DLQ routing, and sentiment sidecar enrichment (see `scripts/integration_test.sh`).

## Window/Retention Policy
- Window labels and durations are configured via `WINDOW_LABELS` (default `1m,5m,15m`).
- Window history size for z-score history is `WINDOW_HISTORY_MAXLEN` (default `300`).
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
  `RETRY_BACKOFF_BASE_SEC`, `RETRY_BACKOFF_CAP_SEC`,
  `RETRY_JITTER_MIN`, `RETRY_JITTER_MAX`.
- Kafka consumer offset reset: `KAFKA_AUTO_OFFSET_RESET`.
- Processor consumer group: `PROCESSOR_CONSUMER_GROUP`.
- Metrics tuning: `EWMA_Z_CAP`, `PERCENTILE_MIN_SAMPLES`.
- Alert logging: `ALERT_LOG_EVERY`.
- Summary sidecar: `SUMMARY_LLM_CONCURRENCY`.
- LLM generation: `LLM_MAX_TOKENS`, `LLM_TEMPERATURE`.
- RSS dedupe: `RSS_SEEN_TTL_SEC`, `RSS_SEEN_MAX`.
- RSS defaults: `RSS_DEFAULT_TITLE`, `RSS_DEFAULT_SOURCE`.
- Sentiment sidecar: `SENTIMENT_PROVIDER`, `SENTIMENT_MODEL_PATH`, `SENTIMENT_BATCH_SIZE`,
  `SENTIMENT_MAX_LATENCY_MS`, `SENTIMENT_FALLBACK_ON_SLOW`, `SENTIMENT_FAIL_FAST`,
  `SENTIMENT_LIGHT_RUNTIME`, `SENTIMENT_METRICS_HOST`, `SENTIMENT_METRICS_PORT`,
  `SENTIMENT_POS_THRESHOLD`, `SENTIMENT_NEG_THRESHOLD`, `SENTIMENT_MAX_SEQ_LEN`,
  `SENTIMENT_SIDECAR_GROUP`, `NEWS_ENRICHED_TOPIC`, `NEWS_DLQ_TOPIC`.
- Sentiment perf logs: `sentiment_infer_ms`, `batch_size`, `queue_lag_ms`, `fallback_used`.
  If `SENTIMENT_MAX_LATENCY_MS` is set, slow batches log `sentiment_batch_slow`.
- Startup check: if `SENTIMENT_PROVIDER=onnx`, the sidecar attempts a model load on start.
  Failure logs `sentiment_model_load_failed` and continues unless `SENTIMENT_FAIL_FAST=true`.
- Metrics endpoint: sentiment sidecar exposes `/metrics` (JSON) on
  `SENTIMENT_METRICS_HOST:SENTIMENT_METRICS_PORT` and includes rolling stats for
  inference and queue lag plus counters (`sentiment_batches`, `sentiment_fallbacks`,
  `sentiment_dlq`, `sentiment_errors`).
- Light runtime: set `SENTIMENT_LIGHT_RUNTIME=true` to use `tokenizers` directly
  and avoid importing `transformers` (useful for slimmer images).
  For Docker builds, set `SENTIMENT_LIGHT_RUNTIME=true` so the image installs
  `requirements-light.txt` (no `transformers`); otherwise it uses `requirements.txt`.
