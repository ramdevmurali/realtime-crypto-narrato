-- Timescale schema for realtime-crypto-narrato
CREATE EXTENSION IF NOT EXISTS timescaledb;

-- Raw price ticks
CREATE TABLE IF NOT EXISTS prices (
    time        TIMESTAMPTZ NOT NULL,
    symbol      TEXT NOT NULL,
    price       DOUBLE PRECISION NOT NULL,
    PRIMARY KEY (time, symbol)
);
SELECT create_hypertable('prices', 'time', if_not_exists => TRUE);
CREATE INDEX IF NOT EXISTS idx_prices_symbol_time_desc ON prices(symbol, time DESC);

-- Rolling metrics
CREATE TABLE IF NOT EXISTS metrics (
    time        TIMESTAMPTZ NOT NULL,
    symbol      TEXT NOT NULL,
    return_1m   DOUBLE PRECISION,
    return_5m   DOUBLE PRECISION,
    return_15m  DOUBLE PRECISION,
    vol_1m      DOUBLE PRECISION,
    vol_5m      DOUBLE PRECISION,
    vol_15m     DOUBLE PRECISION,
    attention   DOUBLE PRECISION,
    PRIMARY KEY (time, symbol)
);
SELECT create_hypertable('metrics', 'time', if_not_exists => TRUE);
CREATE INDEX IF NOT EXISTS idx_metrics_symbol_time_desc ON metrics(symbol, time DESC);

-- News headlines
CREATE TABLE IF NOT EXISTS headlines (
    time        TIMESTAMPTZ NOT NULL,
    title       TEXT NOT NULL,
    source      TEXT,
    url         TEXT,
    sentiment   DOUBLE PRECISION,
    PRIMARY KEY (time, title)
);
SELECT create_hypertable('headlines', 'time', if_not_exists => TRUE);
CREATE INDEX IF NOT EXISTS idx_headlines_time_desc ON headlines(time DESC);

-- Detected anomalies / alerts
CREATE TABLE IF NOT EXISTS anomalies (
    time        TIMESTAMPTZ NOT NULL,
    symbol      TEXT NOT NULL,
    window      TEXT NOT NULL,
    direction   TEXT,
    ret         DOUBLE PRECISION,
    threshold   DOUBLE PRECISION,
    headline    TEXT,
    sentiment   DOUBLE PRECISION,
    summary     TEXT,
    PRIMARY KEY (time, symbol, window)
);
SELECT create_hypertable('anomalies', 'time', if_not_exists => TRUE);
CREATE INDEX IF NOT EXISTS idx_anomalies_symbol_time_desc ON anomalies(symbol, time DESC);
