-- Symbol + year partitioning helper functions.
-- Legacy fixed tables are intentionally preserved for historical reads.

CREATE OR REPLACE FUNCTION symbol_year_table_name(
	p_market TEXT,
	p_data_type TEXT,
	p_symbol TEXT,
	p_year INTEGER
) RETURNS TEXT AS $$
BEGIN
	RETURN format(
		'%s_%s_%s_%s',
		LOWER(p_market),
		LOWER(p_data_type),
		UPPER(p_symbol),
		p_year
	);
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION ensure_symbol_year_hypertable(
	p_market TEXT,
	p_data_type TEXT,
	p_symbol TEXT,
	p_year INTEGER,
	p_exchange TEXT DEFAULT 'binance'
) RETURNS VOID AS $$
DECLARE
	v_table_name TEXT;
BEGIN
	v_table_name := symbol_year_table_name(p_market, p_data_type, p_symbol, p_year);

	IF p_data_type = 'klines' THEN
		EXECUTE format(
			'CREATE TABLE IF NOT EXISTS %I (
				time         TIMESTAMPTZ NOT NULL,
				symbol       TEXT NOT NULL,
				market       TEXT NOT NULL DEFAULT %L,
				exchange     TEXT NOT NULL DEFAULT %L,
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
			)',
			v_table_name, LOWER(p_market), p_exchange
		);
	ELSIF p_data_type = 'orderbook' THEN
		EXECUTE format(
			'CREATE TABLE IF NOT EXISTS %I (
				time       TIMESTAMPTZ NOT NULL,
				symbol     TEXT NOT NULL,
				market     TEXT NOT NULL DEFAULT %L,
				exchange   TEXT NOT NULL DEFAULT %L,
				bids       JSONB NOT NULL,
				asks       JSONB NOT NULL,
				created_at TIMESTAMPTZ DEFAULT NOW(),
				PRIMARY KEY (time, symbol)
			)',
			v_table_name, LOWER(p_market), p_exchange
		);
	ELSIF p_data_type = 'funding_rates' THEN
		EXECUTE format(
			'CREATE TABLE IF NOT EXISTS %I (
				time              TIMESTAMPTZ NOT NULL,
				symbol            TEXT NOT NULL,
				market            TEXT NOT NULL DEFAULT ''futures'',
				exchange          TEXT NOT NULL DEFAULT %L,
				funding_rate      DOUBLE PRECISION NOT NULL,
				mark_price        DOUBLE PRECISION NOT NULL,
				next_funding_time TIMESTAMPTZ NOT NULL,
				created_at        TIMESTAMPTZ DEFAULT NOW(),
				PRIMARY KEY (time, symbol)
			)',
			v_table_name, p_exchange
		);
	ELSIF p_data_type = 'open_interest' THEN
		EXECUTE format(
			'CREATE TABLE IF NOT EXISTS %I (
				time          TIMESTAMPTZ NOT NULL,
				symbol        TEXT NOT NULL,
				open_interest DOUBLE PRECISION NOT NULL,
				period        TEXT NOT NULL DEFAULT ''realtime'',
				market        TEXT NOT NULL DEFAULT ''futures'',
				exchange      TEXT NOT NULL DEFAULT %L,
				created_at    TIMESTAMPTZ DEFAULT NOW(),
				PRIMARY KEY (time, symbol, period)
			)',
			v_table_name, p_exchange
		);
	ELSE
		RAISE EXCEPTION 'unsupported data type: %', p_data_type;
	END IF;

	PERFORM create_hypertable(
		v_table_name,
		'time',
		chunk_time_interval => INTERVAL '1 month',
		if_not_exists => TRUE
	);
END;
$$ LANGUAGE plpgsql;
