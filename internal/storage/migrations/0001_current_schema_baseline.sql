-- Symbol-keyed market-data tables are created lazily by the scraper on their
-- first write. A fresh year database needs only TimescaleDB before collection
-- begins; the migration runner owns the schema_migrations ledger separately.

CREATE EXTENSION IF NOT EXISTS timescaledb WITH SCHEMA public;

COMMENT ON EXTENSION timescaledb IS
    'Enables scalable inserts and complex queries for time-series data (Community Edition)';
