# Migrations

Migration files follow the naming convention: `NNNN_description.sql`

- `NNNN` is a 4-digit sequential number (e.g., `0001`, `0002`, `0003`)
- `description` is a short kebab-case description of what the migration does
- Files are executed in lexical/numerical order
- Only `.sql` files are considered for migration

## Runtime behavior

- Scraper startup uses migration-first schema initialization via `internal/storage/migrations/`.
- Migration execution is idempotent by design (`IF NOT EXISTS` / `if_not_exists => TRUE`).
- SQL migration execution is logged with `type=sql` entries using `statement=migration:<file>`.
- Current writes use `{market}_klines_{symbol_lower}_{interval_lower}` for K-lines
  and `{market}_{data_type}_{symbol_lower}` for orderbook / funding / OI.
  Historical backfill separates years by database (`{exchange}_{year}`), not by
  table suffix.
- `0008_symbol_year_partitioning.sql` keeps legacy symbol-year helper functions
  for old environments and read fallback only. It is not the current write path.
- Legacy fixed tables (for example `futures_klines`) are retained as read-only fallback for historical data.
- Legacy symbol-year tables (for example `futures_klines_BTCUSDT_2026`) may still
  exist in old environments, but they are not part of fresh bootstrap.
- Dynamically-created per-symbol tables use monthly chunks (`chunk_time_interval => INTERVAL '1 month'`).

## Mode behavior reference

- `forward` mode runs realtime collection and defaults to all 5 data kinds when no forward flags are explicitly enabled.
- `reverse` mode runs time-range backfill and only supports:
  - spot/futures `kline`
  - futures `fundingrate`
- Reverse `orderbook` flags are auto-skipped with warning logs.
