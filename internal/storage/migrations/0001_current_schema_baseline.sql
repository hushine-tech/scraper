
--
-- Name: timescaledb; Type: EXTENSION; Schema: -; Owner: -
--

CREATE EXTENSION IF NOT EXISTS timescaledb WITH SCHEMA public;


--
-- Name: EXTENSION timescaledb; Type: COMMENT; Schema: -; Owner: -
--

COMMENT ON EXTENSION timescaledb IS 'Enables scalable inserts and complex queries for time-series data (Community Edition)';


--
-- Name: ensure_symbol_year_hypertable(text, text, text, integer, text); Type: FUNCTION; Schema: public; Owner: -
--

CREATE OR REPLACE FUNCTION ensure_symbol_year_hypertable(p_market text, p_data_type text, p_symbol text, p_year integer, p_exchange text DEFAULT 'binance'::text) RETURNS void
    LANGUAGE plpgsql
    AS $$
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
$$;


--
-- Name: symbol_year_table_name(text, text, text, integer); Type: FUNCTION; Schema: public; Owner: -
--

CREATE OR REPLACE FUNCTION symbol_year_table_name(p_market text, p_data_type text, p_symbol text, p_year integer) RETURNS text
    LANGUAGE plpgsql
    AS $$
BEGIN
	RETURN format(
		'%s_%s_%s_%s',
		LOWER(p_market),
		LOWER(p_data_type),
		UPPER(p_symbol),
		p_year
	);
END;
$$;


SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- Name: futures_funding_rates; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE IF NOT EXISTS futures_funding_rates (
    "time" timestamp with time zone NOT NULL,
    symbol text NOT NULL,
    market text DEFAULT 'futures'::text NOT NULL,
    exchange text DEFAULT 'binance'::text NOT NULL,
    funding_rate double precision NOT NULL,
    mark_price double precision NOT NULL,
    next_funding_time timestamp with time zone NOT NULL,
    created_at timestamp with time zone DEFAULT now()
);


--
-- Name: futures_klines; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE IF NOT EXISTS futures_klines (
    "time" timestamp with time zone NOT NULL,
    symbol text NOT NULL,
    market text DEFAULT 'futures'::text NOT NULL,
    exchange text DEFAULT 'binance'::text NOT NULL,
    open_time timestamp with time zone NOT NULL,
    close_time timestamp with time zone NOT NULL,
    open double precision NOT NULL,
    high double precision NOT NULL,
    low double precision NOT NULL,
    close double precision NOT NULL,
    volume double precision NOT NULL,
    quote_volume double precision NOT NULL,
    num_trades bigint DEFAULT 0 NOT NULL,
    created_at timestamp with time zone DEFAULT now()
);


--
-- Name: futures_open_interest; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE IF NOT EXISTS futures_open_interest (
    "time" timestamp with time zone NOT NULL,
    symbol text NOT NULL,
    open_interest double precision NOT NULL,
    period text DEFAULT 'realtime'::text NOT NULL,
    market text DEFAULT 'futures'::text NOT NULL,
    exchange text DEFAULT 'binance'::text NOT NULL,
    created_at timestamp with time zone DEFAULT now()
);


--
-- Name: futures_orderbook; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE IF NOT EXISTS futures_orderbook (
    "time" timestamp with time zone NOT NULL,
    symbol text NOT NULL,
    market text DEFAULT 'futures'::text NOT NULL,
    exchange text DEFAULT 'binance'::text NOT NULL,
    bids jsonb NOT NULL,
    asks jsonb NOT NULL,
    created_at timestamp with time zone DEFAULT now()
);


--
-- Name: spot_klines; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE IF NOT EXISTS spot_klines (
    "time" timestamp with time zone NOT NULL,
    symbol text NOT NULL,
    market text DEFAULT 'spot'::text NOT NULL,
    exchange text DEFAULT 'binance'::text NOT NULL,
    open_time timestamp with time zone NOT NULL,
    close_time timestamp with time zone NOT NULL,
    open double precision NOT NULL,
    high double precision NOT NULL,
    low double precision NOT NULL,
    close double precision NOT NULL,
    volume double precision NOT NULL,
    quote_volume double precision NOT NULL,
    num_trades bigint DEFAULT 0 NOT NULL,
    created_at timestamp with time zone DEFAULT now()
);


--
-- Name: spot_orderbook; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE IF NOT EXISTS spot_orderbook (
    "time" timestamp with time zone NOT NULL,
    symbol text NOT NULL,
    market text DEFAULT 'spot'::text NOT NULL,
    exchange text DEFAULT 'binance'::text NOT NULL,
    bids jsonb NOT NULL,
    asks jsonb NOT NULL,
    created_at timestamp with time zone DEFAULT now()
);


--
-- Name: futures_funding_rates futures_funding_rates_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

DO $baseline$
BEGIN
IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conrelid = to_regclass('futures_funding_rates') AND conname = 'futures_funding_rates_pkey'
) THEN
ALTER TABLE ONLY futures_funding_rates
    ADD CONSTRAINT futures_funding_rates_pkey PRIMARY KEY ("time", symbol);
END IF;
END
$baseline$;


--
-- Name: futures_klines futures_klines_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

DO $baseline$
BEGIN
IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conrelid = to_regclass('futures_klines') AND conname = 'futures_klines_pkey'
) THEN
ALTER TABLE ONLY futures_klines
    ADD CONSTRAINT futures_klines_pkey PRIMARY KEY ("time", symbol);
END IF;
END
$baseline$;


--
-- Name: futures_open_interest futures_open_interest_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

DO $baseline$
BEGIN
IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conrelid = to_regclass('futures_open_interest') AND conname = 'futures_open_interest_pkey'
) THEN
ALTER TABLE ONLY futures_open_interest
    ADD CONSTRAINT futures_open_interest_pkey PRIMARY KEY ("time", symbol, period);
END IF;
END
$baseline$;


--
-- Name: futures_orderbook futures_orderbook_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

DO $baseline$
BEGIN
IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conrelid = to_regclass('futures_orderbook') AND conname = 'futures_orderbook_pkey'
) THEN
ALTER TABLE ONLY futures_orderbook
    ADD CONSTRAINT futures_orderbook_pkey PRIMARY KEY ("time", symbol);
END IF;
END
$baseline$;


--
-- Name: spot_klines spot_klines_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

DO $baseline$
BEGIN
IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conrelid = to_regclass('spot_klines') AND conname = 'spot_klines_pkey'
) THEN
ALTER TABLE ONLY spot_klines
    ADD CONSTRAINT spot_klines_pkey PRIMARY KEY ("time", symbol);
END IF;
END
$baseline$;


--
-- Name: spot_orderbook spot_orderbook_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

DO $baseline$
BEGIN
IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conrelid = to_regclass('spot_orderbook') AND conname = 'spot_orderbook_pkey'
) THEN
ALTER TABLE ONLY spot_orderbook
    ADD CONSTRAINT spot_orderbook_pkey PRIMARY KEY ("time", symbol);
END IF;
END
$baseline$;


--
-- Name: futures_funding_rates_time_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX IF NOT EXISTS futures_funding_rates_time_idx ON futures_funding_rates USING btree ("time" DESC);


--
-- Name: futures_klines_time_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX IF NOT EXISTS futures_klines_time_idx ON futures_klines USING btree ("time" DESC);


--
-- Name: futures_open_interest_time_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX IF NOT EXISTS futures_open_interest_time_idx ON futures_open_interest USING btree ("time" DESC);


--
-- Name: futures_orderbook_time_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX IF NOT EXISTS futures_orderbook_time_idx ON futures_orderbook USING btree ("time" DESC);


--
-- Name: spot_klines_time_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX IF NOT EXISTS spot_klines_time_idx ON spot_klines USING btree ("time" DESC);


--
-- Name: spot_orderbook_time_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX IF NOT EXISTS spot_orderbook_time_idx ON spot_orderbook USING btree ("time" DESC);


--
-- Name: futures_funding_rates ts_insert_blocker; Type: TRIGGER; Schema: public; Owner: -
--



--
-- Name: futures_klines ts_insert_blocker; Type: TRIGGER; Schema: public; Owner: -
--



--
-- Name: futures_open_interest ts_insert_blocker; Type: TRIGGER; Schema: public; Owner: -
--



--
-- Name: futures_orderbook ts_insert_blocker; Type: TRIGGER; Schema: public; Owner: -
--



--
-- Name: spot_klines ts_insert_blocker; Type: TRIGGER; Schema: public; Owner: -
--



--
-- Name: spot_orderbook ts_insert_blocker; Type: TRIGGER; Schema: public; Owner: -
--



--
-- PostgreSQL database dump complete
--

-- Restore TimescaleDB metadata omitted by pg_dump's extension-owned catalog.
SELECT create_hypertable('spot_klines', 'time',
    chunk_time_interval => INTERVAL '1 day', if_not_exists => TRUE);
SELECT create_hypertable('futures_klines', 'time',
    chunk_time_interval => INTERVAL '1 day', if_not_exists => TRUE);
SELECT create_hypertable('spot_orderbook', 'time',
    chunk_time_interval => INTERVAL '1 day', if_not_exists => TRUE);
SELECT create_hypertable('futures_orderbook', 'time',
    chunk_time_interval => INTERVAL '1 day', if_not_exists => TRUE);
SELECT create_hypertable('futures_funding_rates', 'time',
    chunk_time_interval => INTERVAL '1 day', if_not_exists => TRUE);
SELECT create_hypertable('futures_open_interest', 'time',
    chunk_time_interval => INTERVAL '1 day', if_not_exists => TRUE);
