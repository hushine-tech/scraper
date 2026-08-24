# Migrations

Migration files follow the naming convention: `NNNN_description.sql`

- `NNNN` is a 4-digit sequential number (e.g., `0001`, `0002`, `0003`)
- `description` is a short kebab-case description of what the migration does
- Files are executed in lexical/numerical order
- Only `.sql` files are considered for migration

## Fresh-baseline behavior

- Scraper startup uses migration-first schema initialization via `internal/storage/migrations/`.
- `0001_current_schema_baseline.sql` is the only fresh-bootstrap migration.
- The baseline installs TimescaleDB. The migration runner creates and maintains
  `schema_migrations` separately.
- Symbol-keyed tables are created lazily on their first write and promoted to
  monthly TimescaleDB hypertables.
- Migration execution is idempotent by design (`IF NOT EXISTS` / `if_not_exists => TRUE`).
- SQL migration execution is logged with `type=sql` entries using `statement=migration:<file>`.
- Current writes use `{market}_klines_{symbol_lower}_{interval_lower}` for K-lines
  and `{market}_{data_type}_{symbol_lower}` for orderbook / funding / OI.
  Historical backfill separates years by database (`{exchange}_{year}`), not by
  table suffix.

## Mode behavior reference

- `forward` mode runs realtime collection and defaults to all 5 data kinds when no forward flags are explicitly enabled.
- `reverse` mode runs time-range backfill and only supports:
  - spot/futures `kline`
  - futures `fundingrate`
- Reverse `orderbook` flags are auto-skipped with warning logs.
