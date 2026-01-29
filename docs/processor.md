# Processor

## Resilience notes
- Backoff + jitter on ingest reconnects (websocket, RSS)
- Retries with backoff for DB inserts and Kafka sends
- Circuit-breaker counters (failures) for ingest loops
- Graceful cancellation handling in ingest and processor tasks
- Startup healthcheck: DB (SELECT 1) and Kafka connect
- Structured logging with counters (price/news publishes, alerts)

## Processor src modules

- `config.py`
    - What it is:
      Loads all the environment driven settings to the processor.
    - Decisions:
      Defaults match docker compose (DB URL, Kafka brokers, Redis URL, Binance stream URL, Symbols list, RSS feed URL, alert thresholds, LLM provider/keys and topic names) it also parses csv env for rhe brokers and symbols into python lists and exposes a settings instance used across the processor .

- `db.py`
    - What it is:
      A helper that talks to Timescale/postgres via asyncpg. it sets up four tables (prices, metrics, headlines, anomalies) as hypertables and provides simple insert functions 
    - Decisions:
      use one async connection pool (asyncpg) for speed
      create tables/indexes on startup for safety 
      make timescale hypertables for time- series 
      add symbol/ time indexes for fast  queries 
      use ON CONFLICT DO NOTHING to avoid duplicates  

- `utils.py`
    - What it is:
      it  contains the  utility  helpers
    - Decisions:
      now utc = timezone aware datetime
      simple sentiment - tiny keyword based sentiment scorer returning - 1 to 1
      llm summarize - builds an alert summary , uses stub if no key/provides or or calls gemini/open ai when configured safe fallback to the stub  
      (also has backoff/retry helpers: sleep_backoff, with_retries)

- `windows.py`
    - What it is:
      a lightweight in memory time window for each symbol it stores the recent prices in order, trim old ones and gives fast calculations 
    - add(ts, price) - append a new tick and prune everything older than 16 mins
    - _oldest_for_window (ts,window) - checks the oldest price point in a window which helps as a reference for the returns function
    - get_return(ts,window) uses the the oldest_for_window function to get the past price  uses that to compute the percentage of change compared to the most recent price
    - get_vol = checks the volatility within the window ie the standard deviation within various steps in the window

- `ingest.py`
    - What it is: ingest tasks module
      price_ingest_task(processor): connect to Binance WS for configured symbols, streams miniTicker updates, stamps current UTC time, and publishes {symbol, price, time} to the prices kafka topic; reconnects on errors.
      news_ingest_task(processor): polls the RSS feed every 60s, skips seen items, derives timestamp/title/link/source, computes simple sentiment, writes headline to DB, updates the latest_headline and publishes  to the news kafka topic; ignores transient errors.
      deduplicate items via seen_ids, poll interval 60s
      publish everything to kafka so that downstream consumers can subscribe.
      (has backoff+jitter, counters, graceful cancellation)

- `metrics.py`
    - What it is:
      compute rolling metrics for a symbol at  time ts using its price window 

- `anomaly.py`
    - What it is:
      anomaly detectr/publisher
    - Decisions:
      checks each windows return against its threshold
      rate  limit  alerts per to 60s
      builds direction calls the llm summarizer for context, persists the anomaly row , publishes  an alert message to kafka and records last alert time .
      (logs alerts, uses backoff/retries)

- `processor.py`
    - What it is: core orchestrator class
      holds a shared state , kafka produce/ consumer, per symbol price Windows, last alert time and last headline 
      start() - initializes the db schema, start producer/consumer, launch processing tasks  (runs healthcheck db/kafka, structured logging)
      stop(): cleanly stop consumer/ producer
      process_prices_task() : consumer price messages, store tick, update window, compute metrics, run anomaly checks 

- `main.py`
    - What it is: Entry point
