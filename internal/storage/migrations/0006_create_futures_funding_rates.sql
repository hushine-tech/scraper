CREATE TABLE IF NOT EXISTS futures_funding_rates (
	time              TIMESTAMPTZ NOT NULL,
	symbol            TEXT NOT NULL,
	market            TEXT NOT NULL DEFAULT 'futures',
	exchange          TEXT NOT NULL DEFAULT 'binance',
	funding_rate      DOUBLE PRECISION NOT NULL,
	mark_price        DOUBLE PRECISION NOT NULL,
	next_funding_time TIMESTAMPTZ NOT NULL,
	created_at        TIMESTAMPTZ DEFAULT NOW(),
	PRIMARY KEY (time, symbol)
);

SELECT create_hypertable('futures_funding_rates', 'time', chunk_time_interval => INTERVAL '1 day', if_not_exists => TRUE);
