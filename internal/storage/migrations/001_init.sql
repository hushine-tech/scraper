-- Migration: 001_init.sql
-- Description: Initial schema with all tables (spot_klines, futures_klines, spot_orderbook, futures_orderbook, futures_funding_rates)

CREATE EXTENSION IF NOT EXISTS timescaledb;

-- K线表：现货和期货分表
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

CREATE TABLE IF NOT EXISTS futures_klines (
    time         TIMESTAMPTZ NOT NULL,
    symbol       TEXT NOT NULL,
    market       TEXT NOT NULL DEFAULT 'futures',
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
SELECT create_hypertable('futures_klines', 'time', chunk_time_interval => INTERVAL '1 day', if_not_exists => TRUE);

-- 订单簿表：现货和期货分表
CREATE TABLE IF NOT EXISTS spot_orderbook (
    time     TIMESTAMPTZ NOT NULL,
    symbol   TEXT NOT NULL,
    market   TEXT NOT NULL DEFAULT 'spot',
    exchange TEXT NOT NULL DEFAULT 'binance',
    bids     JSONB NOT NULL,
    asks     JSONB NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (time, symbol)
);
SELECT create_hypertable('spot_orderbook', 'time', chunk_time_interval => INTERVAL '1 day', if_not_exists => TRUE);

CREATE TABLE IF NOT EXISTS futures_orderbook (
    time     TIMESTAMPTZ NOT NULL,
    symbol   TEXT NOT NULL,
    market   TEXT NOT NULL DEFAULT 'futures',
    exchange TEXT NOT NULL DEFAULT 'binance',
    bids     JSONB NOT NULL,
    asks     JSONB NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (time, symbol)
);
SELECT create_hypertable('futures_orderbook', 'time', chunk_time_interval => INTERVAL '1 day', if_not_exists => TRUE);

-- 资金费率表：仅期货
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

ALTER TABLE IF EXISTS spot_klines
    ADD COLUMN IF NOT EXISTS num_trades BIGINT NOT NULL DEFAULT 0;
ALTER TABLE IF EXISTS futures_klines
    ADD COLUMN IF NOT EXISTS num_trades BIGINT NOT NULL DEFAULT 0;
