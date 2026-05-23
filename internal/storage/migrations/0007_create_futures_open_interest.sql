CREATE TABLE IF NOT EXISTS futures_open_interest (
    time          TIMESTAMPTZ NOT NULL,
    symbol        TEXT NOT NULL,
    open_interest DOUBLE PRECISION NOT NULL,
    period        TEXT NOT NULL DEFAULT 'realtime',
    market        TEXT NOT NULL DEFAULT 'futures',
    exchange      TEXT NOT NULL DEFAULT 'binance',
    created_at    TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (time, symbol, period)
);

SELECT create_hypertable('futures_open_interest', 'time', chunk_time_interval => INTERVAL '1 day', if_not_exists => TRUE);
