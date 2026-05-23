CREATE TABLE IF NOT EXISTS futures_orderbook (
	time       TIMESTAMPTZ NOT NULL,
	symbol     TEXT NOT NULL,
	market     TEXT NOT NULL DEFAULT 'futures',
	exchange   TEXT NOT NULL DEFAULT 'binance',
	bids       JSONB NOT NULL,
	asks       JSONB NOT NULL,
	created_at TIMESTAMPTZ DEFAULT NOW(),
	PRIMARY KEY (time, symbol)
);

SELECT create_hypertable('futures_orderbook', 'time', chunk_time_interval => INTERVAL '1 day', if_not_exists => TRUE);
