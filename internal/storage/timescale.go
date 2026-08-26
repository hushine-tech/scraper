package storage

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
	"unicode"

	"github.com/hushine-tech/golang-lib/middleware/sqlmiddleware"
	"github.com/hushine-tech/scraper/internal/logger"
	"github.com/hushine-tech/scraper/internal/models"

	_ "github.com/lib/pq"
)

type TimescaleDB struct {
	db            *sql.DB
	sqlExec       *sqlmiddleware.Middleware
	exchange      string
	migrationsDir string
	tableLocks    sync.Map // map[string]*sync.Mutex
	writeRouter   *MarketDataWriteRouter
}

const scraperMigrationAdvisoryLockKey int64 = 0x4853484e53435250

func NewTimescaleDB(connStr string, exchange string, migrationsDir string) (*TimescaleDB, error) {
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}

	db.SetMaxOpenConns(25)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(5 * time.Minute)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := db.PingContext(ctx); err != nil {
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	return &TimescaleDB{
		db:            db,
		sqlExec:       sqlmiddleware.New(db, logger.NewSQLAdapter()),
		exchange:      exchange,
		migrationsDir: migrationsDir,
	}, nil
}

func (ts *TimescaleDB) Close() error {
	if ts.writeRouter != nil {
		return ts.writeRouter.Close()
	}
	return ts.db.Close()
}

func (ts *TimescaleDB) InitSchema(ctx context.Context) error {
	if ts.writeRouter != nil {
		return nil
	}
	if strings.TrimSpace(ts.migrationsDir) == "" {
		return fmt.Errorf("scraper migrations directory is required")
	}
	return ts.runMigrations(ctx)
}

func (ts *TimescaleDB) runMigrations(ctx context.Context) error {
	migrationFiles, err := listMigrationFiles(ts.migrationsDir)
	if err != nil {
		return err
	}

	tx, err := ts.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin scraper migrations: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	if _, err := tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock($1)`, scraperMigrationAdvisoryLockKey); err != nil {
		return fmt.Errorf("lock scraper migrations: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `
CREATE TABLE IF NOT EXISTS schema_migrations (
	filename   TEXT PRIMARY KEY,
	applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
)`); err != nil {
		return fmt.Errorf("ensure schema_migrations: %w", err)
	}

	for _, file := range migrationFiles {
		var alreadyApplied bool
		if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM schema_migrations WHERE filename = $1)`, file).Scan(&alreadyApplied); err != nil {
			return fmt.Errorf("check migration %s: %w", file, err)
		}
		if alreadyApplied {
			continue
		}

		filePath := filepath.Join(ts.migrationsDir, file)
		content, err := os.ReadFile(filePath)
		if err != nil {
			return fmt.Errorf("failed to read migration file %s: %w", file, err)
		}

		_, err = tx.ExecContext(ctx, string(content))
		if err != nil {
			return fmt.Errorf("failed to execute migration %s: %w", file, err)
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO schema_migrations (filename) VALUES ($1) ON CONFLICT (filename) DO NOTHING`, file); err != nil {
			return fmt.Errorf("record migration %s: %w", file, err)
		}
	}

	return tx.Commit()
}

func listMigrationFiles(dir string) ([]string, error) {
	files, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("failed to read migrations directory: %w", err)
	}

	migrationFiles := make([]string, 0, len(files))
	for _, f := range files {
		if !f.IsDir() && strings.HasSuffix(f.Name(), ".sql") {
			migrationFiles = append(migrationFiles, f.Name())
		}
	}
	sort.Strings(migrationFiles)

	return migrationFiles, nil
}

// buildTableName 生成表名。K线表含 interval：{market}_klines_{symbol}_{interval}；
// 其他数据类型不含 interval：{market}_{datatype}_{symbol}。
func buildTableName(market, dataType, symbol, interval string) string {
	m := normalizeMarket(market)
	normalized := normalizeSymbolForTable(symbol)
	if dataType == "klines" && interval != "" {
		return fmt.Sprintf("%s_%s_%s_%s", m, dataType, normalized, strings.ToLower(interval))
	}
	return fmt.Sprintf("%s_%s_%s", m, dataType, normalized)
}
func normalizeMarket(market string) string {
	m := strings.ToLower(strings.TrimSpace(market))
	if m != "futures" {
		return "spot"
	}
	return m
}

func normalizeSymbolForTable(symbol string) string {
	lower := strings.ToLower(strings.TrimSpace(symbol))
	var b strings.Builder
	for _, r := range lower {
		if unicode.IsLetter(r) || unicode.IsDigit(r) {
			b.WriteRune(r)
		}
	}
	if b.Len() == 0 {
		return "unknown"
	}
	return b.String()
}

func normalizeSymbolForWrite(symbol string) string {
	return strings.ToUpper(strings.TrimSpace(symbol))
}

func (ts *TimescaleDB) getTableMutex(tableName string) *sync.Mutex {
	if lock, ok := ts.tableLocks.Load(tableName); ok {
		return lock.(*sync.Mutex)
	}
	lock := &sync.Mutex{}
	actual, _ := ts.tableLocks.LoadOrStore(tableName, lock)
	return actual.(*sync.Mutex)
}

func (ts *TimescaleDB) optimisticInsertWithLazyCreate(
	ctx context.Context,
	tableName string,
	insertSQL string,
	createSQL string,
	args ...any,
) error {
	if _, err := ts.sqlExec.ExecContext(ctx, insertSQL, args...); err == nil {
		return nil
	}

	lock := ts.getTableMutex(tableName)
	lock.Lock()
	defer lock.Unlock()

	if _, err := ts.sqlExec.ExecContext(ctx, insertSQL, args...); err == nil {
		return nil
	}

	if _, err := ts.sqlExec.ExecContext(ctx, createSQL); err != nil {
		return fmt.Errorf("failed to create table %s: %w", tableName, err)
	}

	if _, err := ts.sqlExec.ExecContext(ctx, insertSQL, args...); err != nil {
		return fmt.Errorf("failed final insert into %s: %w", tableName, err)
	}
	return nil
}

func (ts *TimescaleDB) buildCreateTableSQL(tableName, market, dataType string) string {
	switch dataType {
	case "klines":
		return fmt.Sprintf(`
CREATE TABLE IF NOT EXISTS %s (
	time         TIMESTAMPTZ NOT NULL,
	symbol       TEXT NOT NULL,
	market       TEXT NOT NULL DEFAULT '%s',
	exchange     TEXT NOT NULL DEFAULT '%s',
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
SELECT create_hypertable('%s', 'time', chunk_time_interval => INTERVAL '1 month', if_not_exists => TRUE);
`, tableName, market, ts.exchange, tableName)
	case "orderbook":
		return fmt.Sprintf(`
CREATE TABLE IF NOT EXISTS %s (
	time       TIMESTAMPTZ NOT NULL,
	symbol     TEXT NOT NULL,
	market     TEXT NOT NULL DEFAULT '%s',
	exchange   TEXT NOT NULL DEFAULT '%s',
	bids       JSONB NOT NULL,
	asks       JSONB NOT NULL,
	created_at TIMESTAMPTZ DEFAULT NOW(),
	PRIMARY KEY (time, symbol)
);
SELECT create_hypertable('%s', 'time', chunk_time_interval => INTERVAL '1 month', if_not_exists => TRUE);
`, tableName, market, ts.exchange, tableName)
	case "funding_rates":
		return fmt.Sprintf(`
CREATE TABLE IF NOT EXISTS %s (
	time              TIMESTAMPTZ NOT NULL,
	symbol            TEXT NOT NULL,
	market            TEXT NOT NULL DEFAULT '%s',
	exchange          TEXT NOT NULL DEFAULT '%s',
	funding_rate      NUMERIC(38,18) NOT NULL,
	mark_price        NUMERIC(38,18) NOT NULL,
	next_funding_time TIMESTAMPTZ,
	created_at        TIMESTAMPTZ DEFAULT NOW(),
	PRIMARY KEY (time, symbol)
);
SELECT create_hypertable('%s', 'time', chunk_time_interval => INTERVAL '1 month', if_not_exists => TRUE);
`, tableName, market, ts.exchange, tableName)
	case "open_interest":
		return fmt.Sprintf(`
CREATE TABLE IF NOT EXISTS %s (
	time          TIMESTAMPTZ NOT NULL,
	symbol        TEXT NOT NULL,
	open_interest DOUBLE PRECISION NOT NULL,
	period        TEXT NOT NULL DEFAULT 'realtime',
	market        TEXT NOT NULL DEFAULT '%s',
	exchange      TEXT NOT NULL DEFAULT '%s',
	created_at    TIMESTAMPTZ DEFAULT NOW(),
	PRIMARY KEY (time, symbol, period)
);
SELECT create_hypertable('%s', 'time', chunk_time_interval => INTERVAL '1 month', if_not_exists => TRUE);
`, tableName, market, ts.exchange, tableName)
	default:
		return ""
	}
}

// InsertKline 根据 market 类型插入到对应的 klines 表
func (ts *TimescaleDB) InsertKline(ctx context.Context, klines []models.Kline) error {
	if ts.writeRouter != nil {
		return ts.writeRouter.InsertKline(ctx, klines)
	}
	if len(klines) == 0 {
		return nil
	}

	for _, k := range klines {
		normalizedSymbol := normalizeSymbolForWrite(k.Symbol)
		interval := k.Interval
		if interval == "" {
			interval = "1m"
		}
		tableName := buildTableName(k.Market, "klines", normalizedSymbol, interval)
		stmtText := fmt.Sprintf(`
		INSERT INTO %s (time, symbol, market, exchange, open_time, close_time, open, high, low, close, volume, quote_volume, num_trades)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
		ON CONFLICT (time, symbol) DO NOTHING
	`, tableName)
		createSQL := ts.buildCreateTableSQL(tableName, normalizeMarket(k.Market), "klines")
		if err := ts.optimisticInsertWithLazyCreate(
			ctx,
			tableName,
			stmtText,
			createSQL,
			k.Time,
			normalizedSymbol,
			k.Market,
			k.Exchange,
			k.OpenTime,
			k.CloseTime,
			k.Open,
			k.High,
			k.Low,
			k.Close,
			k.Volume,
			k.QuoteVolume,
			k.NumTrades,
		); err != nil {
			return fmt.Errorf("failed to insert kline into %s: %w", tableName, err)
		}
	}

	return nil
}

// InsertOrderBook 根据 market 类型插入到对应的 orderbook 表
func (ts *TimescaleDB) InsertOrderBook(ctx context.Context, ob models.OrderBook) error {
	if ts.writeRouter != nil {
		return ts.writeRouter.InsertOrderBook(ctx, ob)
	}
	normalizedSymbol := normalizeSymbolForWrite(ob.Symbol)
	tableName := buildTableName(ob.Market, "orderbook", normalizedSymbol, "")
	insertSQL := fmt.Sprintf(`
		INSERT INTO %s (time, symbol, market, exchange, bids, asks)
		VALUES ($1, $2, $3, $4, $5, $6)
		ON CONFLICT (time, symbol) DO NOTHING
	`, tableName)
	createSQL := ts.buildCreateTableSQL(tableName, normalizeMarket(ob.Market), "orderbook")
	if err := ts.optimisticInsertWithLazyCreate(
		ctx,
		tableName,
		insertSQL,
		createSQL,
		ob.Time,
		normalizedSymbol,
		ob.Market,
		ob.Exchange,
		ob.Bids,
		ob.Asks,
	); err != nil {
		return fmt.Errorf("failed to insert orderbook into %s: %w", tableName, err)
	}

	return nil
}

// InsertFundingRate 插入到当前 symbol-keyed futures funding-rate 表。
func (ts *TimescaleDB) InsertFundingRate(ctx context.Context, fr models.FundingRate) error {
	if ts.writeRouter != nil {
		return ts.writeRouter.InsertFundingRate(ctx, fr)
	}
	normalizedSymbol := normalizeSymbolForWrite(fr.Symbol)
	tableName := buildTableName("futures", "funding_rates", normalizedSymbol, "")
	insertSQL := fmt.Sprintf(`
		INSERT INTO %s (time, symbol, market, exchange, funding_rate, mark_price, next_funding_time)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
		ON CONFLICT (time, symbol) DO NOTHING
	`, tableName)
	createSQL := ts.buildCreateTableSQL(tableName, "futures", "funding_rates")
	if err := ts.optimisticInsertWithLazyCreate(
		ctx,
		tableName,
		insertSQL,
		createSQL,
		fr.FundingTime,
		normalizedSymbol,
		fr.Market,
		fr.Exchange,
		fr.FundingRateDecimal,
		fr.MarkPriceDecimal,
		fr.NextFundingTime,
	); err != nil {
		return fmt.Errorf("failed to insert funding rate into %s: %w", tableName, err)
	}
	return nil
}

func (ts *TimescaleDB) InsertOpenInterest(ctx context.Context, oi models.OpenInterest) error {
	if ts.writeRouter != nil {
		return ts.writeRouter.InsertOpenInterest(ctx, oi)
	}
	normalizedSymbol := normalizeSymbolForWrite(oi.Symbol)
	tableName := buildTableName("futures", "open_interest", normalizedSymbol, "")
	insertSQL := fmt.Sprintf(`
		INSERT INTO %s (time, symbol, open_interest, period, market, exchange)
		VALUES ($1, $2, $3, $4, $5, $6)
		ON CONFLICT (time, symbol, period) DO NOTHING
	`, tableName)
	createSQL := ts.buildCreateTableSQL(tableName, "futures", "open_interest")
	if err := ts.optimisticInsertWithLazyCreate(
		ctx,
		tableName,
		insertSQL,
		createSQL,
		oi.Time,
		normalizedSymbol,
		oi.OpenInterest,
		oi.Period,
		oi.Market,
		oi.Exchange,
	); err != nil {
		return fmt.Errorf("failed to insert open interest into %s: %w", tableName, err)
	}
	return nil
}

func (ts *TimescaleDB) InsertOpenInterests(ctx context.Context, items []models.OpenInterest) error {
	if ts.writeRouter != nil {
		return ts.writeRouter.InsertOpenInterests(ctx, items)
	}
	if len(items) == 0 {
		return nil
	}

	for _, oi := range items {
		if err := ts.InsertOpenInterest(ctx, oi); err != nil {
			return err
		}
	}
	return nil
}

func (ts *TimescaleDB) QueryFundingRatesByRange(
	ctx context.Context,
	symbol string,
	startTime time.Time,
	endTime time.Time,
) ([]models.FundingRate, error) {
	candidates := []string{buildTableName("futures", "funding_rates", symbol, "")}
	tableNames, err := ts.filterExistingTables(ctx, candidates)
	if err != nil {
		return nil, err
	}
	parts := make([]string, 0, len(tableNames))
	for _, tableName := range tableNames {
		if tableName == "" {
			continue
		}
		parts = append(parts, fmt.Sprintf(
			"SELECT time, symbol, market, exchange, funding_rate, mark_price, next_funding_time FROM %s WHERE UPPER(symbol) = UPPER($1) AND time >= $2 AND time <= $3",
			tableName,
		))
	}
	if len(parts) == 0 {
		return nil, nil
	}

	query := strings.Join(parts, " UNION ALL ") + " ORDER BY time ASC"
	rows, err := ts.db.QueryContext(ctx, query, symbol, startTime.UTC(), endTime.UTC())
	if err != nil {
		return nil, fmt.Errorf("query funding rates by range: %w", err)
	}
	defer rows.Close()

	result := make([]models.FundingRate, 0)
	for rows.Next() {
		var (
			item          models.FundingRate
			nextFundingAt sql.NullTime
		)
		if err := rows.Scan(
			&item.FundingTime,
			&item.Symbol,
			&item.Market,
			&item.Exchange,
			&item.FundingRateDecimal,
			&item.MarkPriceDecimal,
			&nextFundingAt,
		); err != nil {
			return nil, fmt.Errorf("scan funding rate row: %w", err)
		}
		if nextFundingAt.Valid {
			next := nextFundingAt.Time.UTC()
			item.NextFundingTime = &next
		}
		result = append(result, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate funding rate rows: %w", err)
	}
	return result, nil
}

func (ts *TimescaleDB) filterExistingTables(ctx context.Context, tableNames []string) ([]string, error) {
	existing := make([]string, 0, len(tableNames))
	for _, tableName := range tableNames {
		if tableName == "" {
			continue
		}
		var regclass sql.NullString
		if err := ts.db.QueryRowContext(ctx, "SELECT to_regclass($1)", tableName).Scan(&regclass); err != nil {
			return nil, fmt.Errorf("check table existence %s: %w", tableName, err)
		}
		if regclass.Valid {
			existing = append(existing, tableName)
		}
	}
	return existing, nil
}
