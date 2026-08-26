package storage

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/hushine-tech/scraper/internal/models"
)

func TestFreshBaselineCreatesCurrentSymbolKeyedSchema(t *testing.T) {
	dsn := os.Getenv("SCRAPER_TEST_DSN")
	if dsn == "" {
		t.Skip("SCRAPER_TEST_DSN is required for migration schema tests")
	}

	store, err := NewTimescaleDB(dsn, "binance", "migrations")
	if err != nil {
		t.Fatalf("open TimescaleDB: %v", err)
	}
	defer store.Close()
	store.db.SetMaxOpenConns(1)
	store.db.SetMaxIdleConns(1)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	schema := fmt.Sprintf("migration_symbol_schema_%d", time.Now().UnixNano())
	if _, err := store.db.ExecContext(ctx, `CREATE SCHEMA `+quoteSchemaIdent(schema)); err != nil {
		t.Fatalf("create schema: %v", err)
	}
	defer func() {
		_, _ = store.db.ExecContext(context.Background(), `SET search_path TO public`)
		_, _ = store.db.ExecContext(context.Background(), `DROP SCHEMA `+quoteSchemaIdent(schema)+` CASCADE`)
	}()
	if _, err := store.db.ExecContext(ctx, `SET search_path TO `+quoteSchemaIdent(schema)+`, public`); err != nil {
		t.Fatalf("set search_path: %v", err)
	}

	if err := store.InitSchema(ctx); err != nil {
		t.Fatalf("fresh baseline: %v", err)
	}
	if err := store.InitSchema(ctx); err != nil {
		t.Fatalf("idempotent second baseline run: %v", err)
	}

	now := time.Date(2026, 8, 24, 12, 0, 0, 0, time.UTC)
	for _, kline := range []models.Kline{
		{Time: now, Symbol: "BTCUSDT", Market: "spot", Exchange: "binance", Interval: "1m", OpenTime: now, CloseTime: now.Add(time.Minute), Open: 1, High: 2, Low: 0.5, Close: 1.5, Volume: 3, QuoteVolume: 4, NumTrades: 5},
		{Time: now, Symbol: "BTCUSDT", Market: "futures", Exchange: "binance", Interval: "1m", OpenTime: now, CloseTime: now.Add(time.Minute), Open: 1, High: 2, Low: 0.5, Close: 1.5, Volume: 3, QuoteVolume: 4, NumTrades: 5},
	} {
		if err := store.InsertKline(ctx, []models.Kline{kline}); err != nil {
			t.Fatalf("insert %s kline: %v", kline.Market, err)
		}
	}
	for _, orderBook := range []models.OrderBook{
		{Time: now, Symbol: "BTCUSDT", Market: "spot", Exchange: "binance", Bids: json.RawMessage(`[["1","2"]]`), Asks: json.RawMessage(`[["2","1"]]`)},
		{Time: now, Symbol: "BTCUSDT", Market: "futures", Exchange: "binance", Bids: json.RawMessage(`[["1","2"]]`), Asks: json.RawMessage(`[["2","1"]]`)},
	} {
		if err := store.InsertOrderBook(ctx, orderBook); err != nil {
			t.Fatalf("insert %s order book: %v", orderBook.Market, err)
		}
	}
	nextFundingTime := time.Date(2026, 8, 24, 18, 0, 0, 0, time.UTC)
	if err := store.InsertFundingRate(ctx, models.FundingRate{
		Symbol: "BTCUSDT", Market: "futures", Exchange: "binance",
		FundingTime: now, FundingRateDecimal: "0.001", MarkPriceDecimal: "100", NextFundingTime: &nextFundingTime,
	}); err != nil {
		t.Fatalf("insert funding rate: %v", err)
	}
	if err := store.InsertOpenInterest(ctx, models.OpenInterest{
		Time: now, Symbol: "BTCUSDT", Market: "futures", Exchange: "binance",
		OpenInterest: 10, Period: "realtime",
	}); err != nil {
		t.Fatalf("insert open interest: %v", err)
	}

	for _, table := range []string{
		"spot_klines_btcusdt_1m",
		"futures_klines_btcusdt_1m",
		"spot_orderbook_btcusdt",
		"futures_orderbook_btcusdt",
		"futures_funding_rates_btcusdt",
		"futures_open_interest_btcusdt",
	} {
		if !schemaTableExists(ctx, t, store, schema, table) {
			t.Fatalf("current symbol-keyed table %s is missing", table)
		}
		if !schemaHypertableExists(ctx, t, store, schema, table) {
			t.Fatalf("current symbol-keyed table %s is not a hypertable", table)
		}
	}
	for _, table := range []string{
		"spot_klines",
		"futures_klines",
		"spot_orderbook",
		"futures_orderbook",
		"futures_funding_rates",
		"futures_open_interest",
		"futures_klines_BTCUSDT_2026",
	} {
		if schemaTableExists(ctx, t, store, schema, table) {
			t.Fatalf("obsolete market-data table %s exists after fresh bootstrap", table)
		}
	}
	for _, function := range []string{"symbol_year_table_name", "ensure_symbol_year_hypertable"} {
		if schemaFunctionExists(ctx, t, store, schema, function) {
			t.Fatalf("obsolete market-data function %s exists after fresh bootstrap", function)
		}
	}

	t.Run("funding reads use only the canonical symbol table", func(t *testing.T) {
		legacyTables := []struct {
			name string
			time time.Time
			rate float64
		}{
			{name: "futures_funding_rates_BTCUSDT_2026", time: now.Add(-time.Minute), rate: 0.002},
			{name: "futures_funding_rates", time: now.Add(time.Minute), rate: 0.003},
		}
		for _, legacy := range legacyTables {
			if _, err := store.db.ExecContext(ctx, fmt.Sprintf(
				`CREATE TABLE %s (LIKE futures_funding_rates_btcusdt INCLUDING ALL)`,
				legacy.name,
			)); err != nil {
				t.Fatalf("create legacy funding table %s: %v", legacy.name, err)
			}
			if _, err := store.db.ExecContext(ctx, fmt.Sprintf(`
				INSERT INTO %s (time, symbol, market, exchange, funding_rate, mark_price, next_funding_time)
				VALUES ($1, 'BTCUSDT', 'futures', 'binance', $2, 100, $3)`, legacy.name),
				legacy.time,
				legacy.rate,
				time.Date(2026, 8, 24, 18, 0, 0, 0, time.UTC),
			); err != nil {
				t.Fatalf("insert legacy funding row into %s: %v", legacy.name, err)
			}
		}

		rates, err := store.QueryFundingRatesByRange(ctx, "BTCUSDT", now.Add(-time.Hour), now.Add(time.Hour))
		if err != nil {
			t.Fatalf("query canonical funding rates: %v", err)
		}
		if len(rates) != 1 {
			t.Fatalf("funding rows = %d, want only the canonical row", len(rates))
		}
		if !rates[0].FundingTime.Equal(now) || rates[0].FundingRateDecimal != "0.001" {
			t.Fatalf("unexpected canonical funding row: %+v", rates[0])
		}
	})

	t.Run("funding conflicts enrich unknown successors without changing known facts", func(t *testing.T) {
		differentKnown := nextFundingTime.Add(time.Hour)
		for _, candidate := range []*time.Time{nil, &differentKnown} {
			if err := store.InsertFundingRate(ctx, models.FundingRate{
				Symbol: "BTCUSDT", Market: "futures", Exchange: "binance",
				FundingTime: now, FundingRateDecimal: "9.9", MarkPriceDecimal: "999", NextFundingTime: candidate,
			}); err != nil {
				t.Fatalf("retry known Funding row: %v", err)
			}
		}
		unknownTime := now.Add(time.Minute)
		if err := store.InsertFundingRate(ctx, models.FundingRate{
			Symbol: "BTCUSDT", Market: "futures", Exchange: "binance",
			FundingTime: unknownTime, FundingRateDecimal: "0.002", MarkPriceDecimal: "101",
		}); err != nil {
			t.Fatalf("insert unknown-successor Funding row: %v", err)
		}
		provenSuccessor := unknownTime.Add(8 * time.Hour)
		if err := store.InsertFundingRate(ctx, models.FundingRate{
			Symbol: "BTCUSDT", Market: "futures", Exchange: "binance",
			FundingTime: unknownTime, FundingRateDecimal: "0.002", MarkPriceDecimal: "101", NextFundingTime: &provenSuccessor,
		}); err != nil {
			t.Fatalf("enrich unknown Funding successor: %v", err)
		}
		rates, err := store.QueryFundingRatesByRange(ctx, "BTCUSDT", now, unknownTime)
		if err != nil {
			t.Fatalf("query conflict Funding rows: %v", err)
		}
		if len(rates) != 2 || rates[0].NextFundingTime == nil || !rates[0].NextFundingTime.Equal(nextFundingTime) ||
			rates[0].FundingRateDecimal != "0.001" || rates[0].MarkPriceDecimal != "100" ||
			rates[1].NextFundingTime == nil || !rates[1].NextFundingTime.Equal(provenSuccessor) {
			t.Fatalf("Funding conflict facts = %#v", rates)
		}
	})

	var migrationCount int
	if err := store.db.QueryRowContext(ctx, `SELECT count(*) FROM schema_migrations`).Scan(&migrationCount); err != nil {
		t.Fatalf("count schema migrations: %v", err)
	}
	if migrationCount != 1 {
		t.Fatalf("schema_migrations rows = %d, want 1", migrationCount)
	}
}

func schemaTableExists(ctx context.Context, t *testing.T, store *TimescaleDB, schema, table string) bool {
	t.Helper()
	var exists bool
	if err := store.db.QueryRowContext(ctx, `
		SELECT EXISTS (
			SELECT 1 FROM information_schema.tables
			WHERE table_schema = $1 AND table_name = $2
		)`, schema, table).Scan(&exists); err != nil {
		t.Fatalf("check table %s: %v", table, err)
	}
	return exists
}

func schemaHypertableExists(ctx context.Context, t *testing.T, store *TimescaleDB, schema, table string) bool {
	t.Helper()
	var exists bool
	if err := store.db.QueryRowContext(ctx, `
		SELECT EXISTS (
			SELECT 1 FROM timescaledb_information.hypertables
			WHERE hypertable_schema = $1 AND hypertable_name = $2
		)`, schema, table).Scan(&exists); err != nil {
		t.Fatalf("check hypertable %s: %v", table, err)
	}
	return exists
}

func schemaFunctionExists(ctx context.Context, t *testing.T, store *TimescaleDB, schema, function string) bool {
	t.Helper()
	var exists bool
	if err := store.db.QueryRowContext(ctx, `
		SELECT EXISTS (
			SELECT 1 FROM information_schema.routines
			WHERE routine_schema = $1 AND routine_name = $2
		)`, schema, function).Scan(&exists); err != nil {
		t.Fatalf("check function %s: %v", function, err)
	}
	return exists
}

func quoteSchemaIdent(s string) string {
	return `"` + strings.ReplaceAll(s, `"`, `""`) + `"`
}
