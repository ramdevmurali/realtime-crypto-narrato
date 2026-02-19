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
  - Supports deterministic diagnostics mode via `ANOMALY_TEST_MODE`, `ANOMALY_TEST_THRESHOLD`, `ANOMALY_TEST_COOLDOWN_SEC`.
  - Supports multi-feed news polling via `NEWS_RSS_URLS` (fallback to `NEWS_RSS`).
  - DLQ knob: `price_dlq_topic` (default `prices-deadletter`).
- `io/db.py` — asyncpg pool; creates Timescale hypertables (prices, metrics, headlines, anomalies); insert helpers.
- `utils.py` — now_utc, simple_sentiment stub, llm_summarize (stub/OpenAI/Gemini), backoff helpers (`sleep_backoff`, `with_retries`).
- `domain/windows.py` — in-memory PriceWindow: add/prune (max window + resample step), strict-window returns/vol, keeps smoothed z-score state. Same-timestamp updates overwrite the last price (no duplicate points).
- `domain/metrics.py` — computes rolling returns/vol per symbol from PriceWindow; emits raw return z-scores, EWMA-smoothed return z-scores, volatility z-scores + spike flags, and 5th/95th return percentiles per window.
- `domain/anomaly.py` — **pure** anomaly decision logic (threshold checks, rate-limit, direction). Side effects live in `services/anomaly_service.py`.
- `services/ingest.py` — tasks:
  - `price_ingest_task`: Binance WS → Kafka `prices` (Timescale writes happen in the consumer/pipeline).
  - `news_ingest_task`: one or many RSS feeds → dedupe → sentiment stub → Kafka `news` → Timescale `headlines`.
    - Freshness SLO visibility: `HEADLINE_STALE_WARN_SEC`, `NEWS_STALE_LOG_EVERY`.
  - Includes backoff/jitter, counters, graceful cancel.
- `services/price_consumer.py` — Kafka consume loop + commit policy.
- `services/price_pipeline.py` — DB + metrics + anomaly pipeline.
- `services/runtime.py` — startup config logging.
- `services/health.py` — DB/Kafka health checks.
- `services/summary_sidecar.py` — enriches alerts with LLM summaries from `summaries` topic.
- `services/sentiment_model.py` — sentiment inference wrapper (stub/onnx).
- `services/sentiment_sidecar.py` — enriches news sentiment and publishes `news-enriched`.
- `streaming_core.py` — orchestrator + shared state.
- `app.py` (entrypoint: `PYTHONPATH=processor/src:. .venv/bin/python -m app`) — thin entrypoint for `StreamProcessor`.

Note: legacy folders like `processor/src/models` and `processor/src/processor/services` are removed; use `io/models` and `services/` instead.

## Data written to Timescale
- `prices(time, symbol, price, PK (time, symbol))`
- `metrics(time, symbol, return_1m/5m/15m, vol_1m/5m/15m, return_z_1m/5m/15m, return_z_ewma_1m/5m/15m, vol_z_1m/5m/15m, vol_spike_1m/5m/15m, p05_return_1m/5m/15m, p95_return_1m/5m/15m, attention)`
- `headlines(time, title, source, url, sentiment)`
  - Note: the `headlines` table stores persisted records from the Kafka `news` topic (raw headline feed).
- `anomalies(time, symbol, window_name, direction, return_value, threshold, headline, sentiment, summary)`
  - Note: DB column `window_name` corresponds to `window` in Kafka payloads.
  - Note: the `anomalies` table stores persisted alert records.

Terminology note: the `metrics` table stores **price metrics** (returns/vol/z/percentiles). This is distinct from **runtime telemetry metrics** (counters/rolling stats) exposed via `/metrics`.

## Alert path and summaries
- Threshold check & cooldown happen in `domain/anomaly.py`; if tripped, the alert is persisted to Timescale (in `anomalies`) and published to the Kafka `alerts` topic. These represent the same event across DB and Kafka.
- Normal mode uses static thresholds (`ALERT_THRESHOLD_1M/5M/15M`) and `ANOMALY_COOLDOWN_SEC`.
- Test mode is explicit and off by default: when `ANOMALY_TEST_MODE=true`, thresholds are overridden by `ANOMALY_TEST_THRESHOLD` and cooldown by `ANOMALY_TEST_COOLDOWN_SEC`.
- The processor no longer calls the LLM in the hot path. It publishes a **summary request** message to the `summaries` topic with `{time, symbol, window, direction, ret, threshold, headline, sentiment}`. Alerts still carry the base/stub summary.
  - Note: the Kafka `summaries` topic contains **requests**, not completed summaries.
- The summary sidecar consumes `summaries`, calls the configured LLM, and backfills richer summaries asynchronously.
  - Sidecar knobs: `SUMMARY_CONSUMER_GROUP`, `SUMMARY_POLL_TIMEOUT_MS`, `SUMMARY_BATCH_MAX` (optional) control consumption behavior.

## Topic payloads (required fields)
Canonical payload models live in `processor/src/io/models/messages.py`.
- `prices`: `symbol`, `price`, `time`
- `news`: `time`, `title`, `source`, `sentiment` (optional: `url`)
  - Note: `news` is the raw headline feed; it is persisted to the `headlines` table and represented by `NewsMsg` / `latest_headline` in code.
- `summaries` (summary-request): `time`, `symbol`, `window`, `direction`, `ret`, `threshold` (optional: `event_id`, `headline`, `sentiment`)
  - Note: this topic carries **summary requests**, not completed summaries.
- `alerts`: `event_id`, `time`, `symbol`, `window`, `direction`, `ret`, `threshold`, `summary` (optional: `headline`, `sentiment`)
  - Note: `window` in Kafka payloads maps to `window_name` in the `anomalies` table.
- `news-enriched`: same as `news` plus optional `label`, `confidence`, and `event_id` (sentiment sidecar output)
- `prices-deadletter`: raw price messages that failed processing
- `news-deadletter`: raw news messages that failed enrichment (poison-pill avoidance)
- `summaries-deadletter`: raw summary request messages that failed processing

Timestamp consistency: all Kafka payload `time` fields are ISO 8601 strings (e.g., `2026-02-01T00:00:00+00:00`).
Downstream consumers should parse all `time` fields as ISO 8601 strings.
We may standardize internal model types further later, but the wire format is consistent.

Event ID formats (stable, for tracing/dedupe):
- `summaries`/`alerts`: `event_id = "{time}:{symbol}:{window}"` (time is ISO 8601).
- `news-enriched`: `event_id = "news:{source}:{sha256(time|source|title|url)[:12]}"` (title lowercased/trimmed; url may be empty).
Note: event_id is deterministic and collision‑resistant; intended for dedupe/trace.

Sentiment fallback behavior: if the sentiment sidecar is down or errors, the raw `news` topic remains valid
and carries the stub sentiment; consumers can continue using `news` until `news-enriched` is available.

## Anomaly observability
Processor telemetry includes explicit anomaly-path decisions (namespaced under `processor.`):
- `processor.anomaly_candidates`
- `processor.anomaly_emitted`
- `processor.anomaly_suppressed_threshold`
- `processor.anomaly_suppressed_cooldown`
- `processor.anomaly_emitted_without_headline`
- `processor.news_entries_ingested`
- `processor.news_feed_errors`
- `processor.news_stale_polls`
- rolling `processor.latest_headline_age_sec`

Decision logs are sampled (`anomaly_decision_sample`) to avoid per-tick spam and still explain emit/suppress reasons.

Non-goals for now:
- No schema registry (JSON/Avro) yet; models + tests enforce contracts.

## Interfaces (module inputs/outputs)
- `services/ingest.py`: inputs — Binance WS ticks + RSS feed; outputs — Kafka `prices`/`news`, Timescale `prices`/`headlines`, updates `processor.latest_headline`.
- `domain/metrics.py`: inputs — `PriceWindow` state per symbol; outputs — metrics dict (returns/vol/z/percentiles) for the current tick.
- `domain/anomaly.py`: inputs — symbol, timestamp, metrics; outputs — Timescale `anomalies`, Kafka `alerts`, Kafka `summaries` request.
- `streaming_core.py`: orchestrator; owns Kafka producer/consumer, `price_windows`, `last_alert`, `latest_headline`, DLQ counters.
- `services/summary_sidecar.py`: inputs — Kafka `summaries` requests; outputs — Timescale `anomalies` summary upsert, Kafka `alerts` enriched.
- `services/sentiment_sidecar.py`: inputs — Kafka `news` (headlines); outputs — Timescale `headlines` sentiment overwrite, Kafka `news-enriched`.

## How to run
Run migrations before starting services:
`PYTHONPATH=processor/src:. .venv/bin/python scripts/migrate_db.py`

```
cd infra
docker compose up -d redpanda timescaledb processor summary-sidecar sentiment-sidecar backend   # containers use PYTHONPATH=/app
```
Local/tests PYTHONPATH: `PYTHONPATH=processor/src:.`

Sentiment sidecar (local):
```
PYTHONPATH=processor/src:. .venv/bin/python -m services.sentiment_sidecar
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

Replay buffered summary requests (if summary DLQ send failed):
```
KAFKA_BROKERS_RAW=localhost:9092 \
PYTHONPATH=processor/src:. .venv/bin/python scripts/replay_summary_dlq.py --path summary_dlq_buffer.jsonl
```
Note: default replays to `summaries` (may re‑LLM). To avoid re‑LLM, replay to DLQ:
`--topic summaries-deadletter`.
Warning: replaying to `summaries` will re‑trigger the LLM and can duplicate alerts.
Prefer `--topic summaries-deadletter` unless you explicitly want re‑LLM.

Deterministic anomaly probe (no config edits required):
```
PYTHONPATH=processor/src:. .venv/bin/python scripts/probe_anomaly_path.py --check-summaries
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
  Supported labels are currently limited to `1m`, `5m`, and `15m` (validation will fail otherwise).
- Window history size for z-score history is `WINDOW_HISTORY_MAXLEN` (default `300`).
- Prune horizon is derived from the largest window plus the resample step:
  `max_window + VOL_RESAMPLE_SEC`.
- Returns/vol are **strict-window**: prices outside the window are not used.
- Gap handling is controlled by `WINDOW_MAX_GAP_FACTOR` but is clamped to the
  window size to prevent older spillover.
  Vol gap handling can be tightened separately via `VOL_MAX_GAP_FACTOR`
  (defaults to `WINDOW_MAX_GAP_FACTOR`).
Note: this section is about **in‑memory window retention**, not DB retention. See **DB retention (optional)** below.

## DB retention (optional)
Schema creation is out‑of‑band: run `scripts/migrate_db.py` before starting the processor.
The processor does **not** run DDL on startup.

Retention policies are applied only when you run `scripts/migrate_db.py`.
Set any of these env vars to enable:
- `RETENTION_PRICES_DAYS` (default 30 if set)
- `RETENTION_METRICS_DAYS` (default 30 if set)
- `RETENTION_HEADLINES_DAYS` (default 90 if set)
- `RETENTION_ANOMALIES_DAYS` (default 90 if set)
If a value is unset/empty, no retention policy is applied for that table.

## Runtime Policies
This list is intentionally brief to avoid drift. Source of truth lives in
`processor/src/config.py` and `infra/.env.example`.

Categories:
- Ingest/backoff and retry tuning
- Kafka consumer settings
- DB init/migration safety
- Metrics endpoints + telemetry
- Sidecar + sentiment/LLM behavior

Examples of critical knobs:
- `ENABLE_DB_INIT` (used by `init_tables()` in dev/scripts; processor no longer runs DDL at startup)
- `KAFKA_AUTO_OFFSET_RESET`, `PROCESSOR_CONSUMER_GROUP`
- `LATE_PRICE_TOLERANCE_SEC`, `ANOMALY_COOLDOWN_SEC`
- `ANOMALY_TEST_MODE`, `ANOMALY_TEST_THRESHOLD`, `ANOMALY_TEST_COOLDOWN_SEC`
- `NEWS_RSS`, `NEWS_RSS_URLS`, `HEADLINE_STALE_WARN_SEC`
- `PROCESSOR_METRICS_PORT`, `SUMMARY_METRICS_PORT`
- `SENTIMENT_PROVIDER`, `SENTIMENT_MODEL_PATH`
- `LLM_MAX_TOKENS`, `LLM_TEMPERATURE`
