CREATE TABLE IF NOT EXISTS spot_klines (
	time         TIMESTAMPTZ NOT NULL,
	symbol       TEXT NOT NULL,
	market       TEXT NOT NULL DEFAULT 'spot',
	exchange     TEXT NOT NULL DEFAULT 'binance',
	open_time    TIMESTAMPTZ NOT NULL,
	close_time   TIMESTAMPTZ NOT NULL,
	open         DOUBLE PRECISION NOT NULL,
	high         DOUBLE PRECISION NOT NULL,
	low          DOUBLE PRECISION NOT NULL,
	close        DOUBLE PRECISION NOT NULL,
	volume       DOUBLE PRECISION NOT NULL,
	quote_volume DOUBLE PRECISION NOT NULL,
	num_trades   BIGINT NOT NULL DEFAULT 0,
	created_at   TIMESTAMPTZ DEFAULT NOW(),
	PRIMARY KEY (time, symbol)
);

SELECT create_hypertable('spot_klines', 'time', chunk_time_interval => INTERVAL '1 day', if_not_exists => TRUE);

ALTER TABLE IF EXISTS spot_klines
	ADD COLUMN IF NOT EXISTS num_trades BIGINT NOT NULL DEFAULT 0;
