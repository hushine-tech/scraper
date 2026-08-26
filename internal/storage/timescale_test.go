package storage

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hushine-tech/golang-lib/middleware/sqlmiddleware"
	"github.com/hushine-tech/scraper/internal/logger"
	"github.com/hushine-tech/scraper/internal/models"
)

func TestListMigrationFilesSorted(t *testing.T) {
	dir := t.TempDir()
	files := []string{
		"0003_c.sql",
		"0001_a.sql",
		"README.md",
		"0002_b.sql",
	}
	for _, file := range files {
		if err := os.WriteFile(filepath.Join(dir, file), []byte("SELECT 1;"), 0o644); err != nil {
			t.Fatalf("write file: %v", err)
		}
	}

	got, err := listMigrationFiles(dir)
	if err != nil {
		t.Fatalf("listMigrationFiles: %v", err)
	}

	want := []string{"0001_a.sql", "0002_b.sql", "0003_c.sql"}
	if len(got) != len(want) {
		t.Fatalf("expected %d files got %d", len(want), len(got))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("unexpected order at %d: want %s got %s", i, want[i], got[i])
		}
	}
}

func TestMigrationsContainIdempotentGuards(t *testing.T) {
	dir := filepath.Join("migrations")
	files, err := listMigrationFiles(dir)
	if err != nil {
		t.Fatalf("load project migrations: %v", err)
	}
	if len(files) == 0 {
		t.Fatal("expected migration files")
	}

	for _, file := range files {
		path := filepath.Join(dir, file)
		contentBytes, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read migration %s: %v", file, err)
		}
		content := strings.ToLower(string(contentBytes))
		if strings.Contains(content, "create table") && !strings.Contains(content, "create table if not exists") {
			t.Fatalf("migration %s should use CREATE TABLE IF NOT EXISTS", file)
		}
	}
}

func TestFundingStorageUsesExactDecimalColumnsAndScansStrings(t *testing.T) {
	ts := &TimescaleDB{exchange: "binance"}
	createSQL := strings.ToLower(ts.buildCreateTableSQL("futures_funding_rates_btcusdt", "futures", "funding_rates"))
	for _, column := range []string{
		"funding_rate      numeric(38,18) not null",
		"mark_price        numeric(38,18) not null",
	} {
		if !strings.Contains(createSQL, column) {
			t.Fatalf("funding create SQL missing %q:\n%s", column, createSQL)
		}
	}
	if strings.Contains(createSQL, "next_funding_time timestamptz not null") {
		t.Fatalf("funding next time must allow unknown exchange schedules:\n%s", createSQL)
	}

	db, cleanup := newFundingQueryMockDB(t)
	defer cleanup()
	ts.db = db
	rates, err := ts.QueryFundingRatesByRange(
		context.Background(),
		"BTCUSDT",
		time.Unix(0, 0).UTC(),
		time.Unix(100, 0).UTC(),
	)
	if err != nil {
		t.Fatalf("query funding rates: %v", err)
	}
	if len(rates) != 1 {
		t.Fatalf("funding rows = %d, want 1", len(rates))
	}
	if got, want := rates[0].FundingRateDecimal, "0.000100000000000001"; got != want {
		t.Fatalf("scanned funding rate decimal = %q, want %q", got, want)
	}
	if got, want := rates[0].MarkPriceDecimal, "20000.123456789012345678"; got != want {
		t.Fatalf("scanned mark price decimal = %q, want %q", got, want)
	}
	if rates[0].NextFundingTime != nil {
		t.Fatalf("scanned next funding time = %s, want unknown", rates[0].NextFundingTime)
	}
}

func TestInsertFundingRateConflictOnlyEnrichesUnknownNextTime(t *testing.T) {
	var insertSQL string
	db, cleanup := newMockDB(t, func(query string) error {
		if strings.HasPrefix(strings.TrimSpace(strings.ToUpper(query)), "INSERT INTO") {
			insertSQL = query
		}
		return nil
	})
	defer cleanup()
	ts := &TimescaleDB{db: db, sqlExec: sqlmiddleware.New(db, logger.NewSQLAdapter()), exchange: "binance"}
	next := time.UnixMilli(2000).UTC()
	if err := ts.InsertFundingRate(context.Background(), models.FundingRate{
		Exchange: models.ExchangeBinance, Market: models.MarketFutures, Symbol: "BTCUSDT",
		FundingTime: time.UnixMilli(1000).UTC(), FundingRateDecimal: "0.1", MarkPriceDecimal: "100", NextFundingTime: &next,
	}); err != nil {
		t.Fatalf("InsertFundingRate: %v", err)
	}
	normalized := strings.Join(strings.Fields(strings.ToLower(insertSQL)), " ")
	want := "do update set next_funding_time = excluded.next_funding_time where futures_funding_rates_btcusdt.next_funding_time is null and excluded.next_funding_time is not null"
	if !strings.Contains(normalized, want) {
		t.Fatalf("Funding conflict SQL does not preserve known successors and enrich only unknown ones:\n%s", insertSQL)
	}
}

func TestLinkFundingRatePredecessorTreatsKnownSuccessorAsFound(t *testing.T) {
	driverName := fmt.Sprintf("funding_predecessor_known_%d", time.Now().UnixNano())
	sql.Register(driverName, fundingPredecessorKnownDriver{})
	db, err := sql.Open(driverName, "")
	if err != nil {
		t.Fatalf("open predecessor DB: %v", err)
	}
	defer db.Close()
	ts := &TimescaleDB{db: db, sqlExec: sqlmiddleware.New(db, logger.NewSQLAdapter()), exchange: "binance"}
	found, err := ts.linkFundingRatePredecessor(context.Background(), models.FundingRate{
		Exchange: "binance", Market: "futures", Symbol: "BTCUSDT", FundingTime: time.UnixMilli(2000).UTC(),
	})
	if err != nil {
		t.Fatalf("linkFundingRatePredecessor: %v", err)
	}
	if !found {
		t.Fatal("predecessor with an already-known immutable successor was treated as missing")
	}
}

type fundingPredecessorKnownDriver struct{}

func (fundingPredecessorKnownDriver) Open(string) (driver.Conn, error) {
	return fundingPredecessorKnownConn{}, nil
}

type fundingPredecessorKnownConn struct{}

func (fundingPredecessorKnownConn) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("Prepare is not implemented")
}
func (fundingPredecessorKnownConn) Close() error { return nil }
func (fundingPredecessorKnownConn) Begin() (driver.Tx, error) {
	return nil, errors.New("Begin is not implemented")
}
func (fundingPredecessorKnownConn) ExecContext(context.Context, string, []driver.NamedValue) (driver.Result, error) {
	return driver.RowsAffected(0), nil
}
func (fundingPredecessorKnownConn) QueryContext(context.Context, string, []driver.NamedValue) (driver.Rows, error) {
	return &fundingQueryRows{columns: []string{"found"}, values: [][]driver.Value{{true}}}, nil
}

func TestRunMigrationsUsesLedgerAndAdvisoryLock(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "0001_existing.sql"), []byte("SELECT existing"), 0o644); err != nil {
		t.Fatalf("write existing migration: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "0002_new.sql"), []byte("SELECT new"), 0o644); err != nil {
		t.Fatalf("write new migration: %v", err)
	}

	state := &runtimeMigrationState{
		applied: map[string]bool{
			"0001_existing.sql": true,
		},
	}
	db, cleanup := newRuntimeMigrationMockDB(t, state)
	defer cleanup()

	ts := &TimescaleDB{
		db:            db,
		migrationsDir: dir,
	}
	if err := ts.runMigrations(context.Background()); err != nil {
		t.Fatalf("runMigrations: %v", err)
	}

	if got := state.execCount("SELECT existing"); got != 0 {
		t.Fatalf("already applied migration executed %d times, want 0", got)
	}
	if got := state.execCount("SELECT new"); got != 1 {
		t.Fatalf("new migration executed %d times, want 1", got)
	}
	if !state.appliedMigration("0002_new.sql") {
		t.Fatalf("new migration was not recorded in schema_migrations")
	}
	if got := state.execCountPrefix("SELECT pg_advisory_xact_lock"); got != 1 {
		t.Fatalf("advisory lock executed %d times, want 1", got)
	}
}

func TestInitSchemaRejectsEmptyMigrationsDirectory(t *testing.T) {
	db, cleanup := newMockDB(t, nil)
	defer cleanup()

	ts := &TimescaleDB{
		db:      db,
		sqlExec: sqlmiddleware.New(db, logger.NewSQLAdapter()),
	}
	if err := ts.InitSchema(context.Background()); err == nil {
		t.Fatal("InitSchema with an empty migrations directory succeeded; want a configuration error")
	}
}

func TestOptimisticInsertCreatesThenRetries(t *testing.T) {
	var insertCount int32
	var createCount int32

	db, cleanup := newMockDB(t, func(query string) error {
		trimmed := strings.TrimSpace(strings.ToUpper(query))
		switch {
		case strings.HasPrefix(trimmed, "INSERT INTO"):
			switch atomic.AddInt32(&insertCount, 1) {
			case 1, 2:
				return errors.New("relation does not exist")
			default:
				return nil
			}
		case strings.HasPrefix(trimmed, "CREATE TABLE"):
			atomic.AddInt32(&createCount, 1)
			return nil
		default:
			return nil
		}
	})
	defer cleanup()

	ts := &TimescaleDB{
		db:      db,
		sqlExec: sqlmiddleware.New(db, logger.NewSQLAdapter()),
	}
	err := ts.optimisticInsertWithLazyCreate(
		context.Background(),
		"futures_klines_BTCUSDT_2026",
		"INSERT INTO futures_klines_BTCUSDT_2026 (time, symbol) VALUES ($1, $2)",
		"CREATE TABLE IF NOT EXISTS futures_klines_BTCUSDT_2026 (time TIMESTAMPTZ, symbol TEXT, PRIMARY KEY (time, symbol));",
		time.Now().UTC(),
		"BTCUSDT",
	)
	if err != nil {
		t.Fatalf("optimistic insert should succeed after create: %v", err)
	}

	if got := atomic.LoadInt32(&createCount); got != 1 {
		t.Fatalf("create should run once, got %d", got)
	}
	if got := atomic.LoadInt32(&insertCount); got != 3 {
		t.Fatalf("expected 3 inserts (try/retry/final), got %d", got)
	}
}

func TestOptimisticInsertConcurrentCreateIsSingleFlight(t *testing.T) {
	var createCount int32
	var tableReady int32

	db, cleanup := newMockDB(t, func(query string) error {
		trimmed := strings.TrimSpace(strings.ToUpper(query))
		switch {
		case strings.HasPrefix(trimmed, "INSERT INTO"):
			if atomic.LoadInt32(&tableReady) == 1 {
				return nil
			}
			return errors.New("relation does not exist")
		case strings.HasPrefix(trimmed, "CREATE TABLE"):
			atomic.AddInt32(&createCount, 1)
			atomic.StoreInt32(&tableReady, 1)
			return nil
		default:
			return nil
		}
	})
	defer cleanup()

	ts := &TimescaleDB{
		db:      db,
		sqlExec: sqlmiddleware.New(db, logger.NewSQLAdapter()),
	}

	const workers = 16
	var wg sync.WaitGroup
	errCh := make(chan error, workers)
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := ts.optimisticInsertWithLazyCreate(
				context.Background(),
				"futures_open_interest_BTCUSDT_2026",
				"INSERT INTO futures_open_interest_BTCUSDT_2026 (time, symbol, period) VALUES ($1, $2, $3)",
				"CREATE TABLE IF NOT EXISTS futures_open_interest_BTCUSDT_2026 (time TIMESTAMPTZ, symbol TEXT, period TEXT, PRIMARY KEY (time, symbol, period));",
				time.Now().UTC(),
				"BTCUSDT",
				"realtime",
			)
			errCh <- err
		}()
	}
	wg.Wait()
	close(errCh)

	for err := range errCh {
		if err != nil {
			t.Fatalf("concurrent optimistic insert should succeed: %v", err)
		}
	}
	if got := atomic.LoadInt32(&createCount); got != 1 {
		t.Fatalf("create should run once with table lock, got %d", got)
	}
}

type execBehavior func(query string) error

type mockDriver struct {
	behavior execBehavior
}

func (d *mockDriver) Open(name string) (driver.Conn, error) {
	return &mockConn{behavior: d.behavior}, nil
}

type mockConn struct {
	behavior execBehavior
}

func (c *mockConn) Prepare(string) (driver.Stmt, error) { return nil, errors.New("not implemented") }
func (c *mockConn) Close() error                        { return nil }
func (c *mockConn) Begin() (driver.Tx, error)           { return nil, errors.New("not implemented") }

func (c *mockConn) ExecContext(_ context.Context, query string, _ []driver.NamedValue) (driver.Result, error) {
	if c.behavior != nil {
		if err := c.behavior(query); err != nil {
			return nil, err
		}
	}
	return driver.RowsAffected(1), nil
}

func newMockDB(t *testing.T, behavior execBehavior) (*sql.DB, func()) {
	t.Helper()
	driverName := fmt.Sprintf("timescale_mock_%d", time.Now().UnixNano())
	sql.Register(driverName, &mockDriver{behavior: behavior})
	db, err := sql.Open(driverName, "")
	if err != nil {
		t.Fatalf("open mock db: %v", err)
	}
	return db, func() {
		_ = db.Close()
	}
}

var runtimeMigrationMockDriverSeq atomic.Uint64

type runtimeMigrationState struct {
	mu      sync.Mutex
	applied map[string]bool
	execSQL []string
}

func (s *runtimeMigrationState) appliedMigration(name string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.applied[name]
}

func (s *runtimeMigrationState) execCount(query string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	var count int
	for _, got := range s.execSQL {
		if got == query {
			count++
		}
	}
	return count
}

func (s *runtimeMigrationState) execCountPrefix(prefix string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	var count int
	for _, got := range s.execSQL {
		if strings.HasPrefix(got, prefix) {
			count++
		}
	}
	return count
}

func newRuntimeMigrationMockDB(t *testing.T, state *runtimeMigrationState) (*sql.DB, func()) {
	t.Helper()
	driverName := fmt.Sprintf("runtime_migration_mock_%d", runtimeMigrationMockDriverSeq.Add(1))
	sql.Register(driverName, runtimeMigrationDriver{state: state})
	db, err := sql.Open(driverName, "")
	if err != nil {
		t.Fatalf("open runtime migration mock db: %v", err)
	}
	return db, func() {
		_ = db.Close()
	}
}

type runtimeMigrationDriver struct {
	state *runtimeMigrationState
}

func (d runtimeMigrationDriver) Open(string) (driver.Conn, error) {
	return &runtimeMigrationConn{state: d.state}, nil
}

type runtimeMigrationConn struct {
	state *runtimeMigrationState
}

func (c *runtimeMigrationConn) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("Prepare is not implemented")
}

func (c *runtimeMigrationConn) Close() error {
	return nil
}

func (c *runtimeMigrationConn) Begin() (driver.Tx, error) {
	return runtimeMigrationTx{}, nil
}

func (c *runtimeMigrationConn) ExecContext(_ context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	normalized := strings.TrimSpace(query)
	c.state.mu.Lock()
	defer c.state.mu.Unlock()
	if c.state.applied == nil {
		c.state.applied = map[string]bool{}
	}
	c.state.execSQL = append(c.state.execSQL, normalized)
	if strings.HasPrefix(normalized, "INSERT INTO schema_migrations") && len(args) > 0 {
		c.state.applied[fmt.Sprint(args[0].Value)] = true
	}
	return driver.RowsAffected(1), nil
}

func (c *runtimeMigrationConn) QueryContext(_ context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	normalized := strings.TrimSpace(query)
	if strings.HasPrefix(normalized, "SELECT EXISTS(SELECT 1 FROM schema_migrations") {
		filename := ""
		if len(args) > 0 {
			filename = fmt.Sprint(args[0].Value)
		}
		c.state.mu.Lock()
		applied := c.state.applied[filename]
		c.state.mu.Unlock()
		return &runtimeMigrationRows{values: []driver.Value{applied}}, nil
	}
	return nil, fmt.Errorf("unexpected query: %s", query)
}

type runtimeMigrationTx struct{}

func (runtimeMigrationTx) Commit() error {
	return nil
}

func (runtimeMigrationTx) Rollback() error {
	return nil
}

type runtimeMigrationRows struct {
	values []driver.Value
	read   bool
}

var fundingQueryMockDriverSeq atomic.Uint64

func newFundingQueryMockDB(t *testing.T) (*sql.DB, func()) {
	t.Helper()
	driverName := fmt.Sprintf("funding_query_mock_%d", fundingQueryMockDriverSeq.Add(1))
	sql.Register(driverName, fundingQueryDriver{})
	db, err := sql.Open(driverName, "")
	if err != nil {
		t.Fatalf("open funding query mock db: %v", err)
	}
	return db, func() { _ = db.Close() }
}

type fundingQueryDriver struct{}

func (fundingQueryDriver) Open(string) (driver.Conn, error) { return fundingQueryConn{}, nil }

type fundingQueryConn struct{}

func (fundingQueryConn) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("Prepare is not implemented")
}
func (fundingQueryConn) Close() error { return nil }
func (fundingQueryConn) Begin() (driver.Tx, error) {
	return nil, errors.New("Begin is not implemented")
}

func (fundingQueryConn) QueryContext(_ context.Context, query string, _ []driver.NamedValue) (driver.Rows, error) {
	switch {
	case strings.Contains(query, "to_regclass"):
		return &fundingQueryRows{
			columns: []string{"to_regclass"},
			values:  [][]driver.Value{{"futures_funding_rates_btcusdt"}},
		}, nil
	case strings.Contains(query, "FROM futures_funding_rates_btcusdt"):
		return &fundingQueryRows{
			columns: []string{"time", "symbol", "market", "exchange", "funding_rate", "mark_price", "next_funding_time"},
			values: [][]driver.Value{{
				time.UnixMilli(1000).UTC(), "BTCUSDT", "futures", "binance",
				"0.000100000000000001", "20000.123456789012345678", nil,
			}},
		}, nil
	default:
		return nil, fmt.Errorf("unexpected funding query: %s", query)
	}
}

type fundingQueryRows struct {
	columns []string
	values  [][]driver.Value
	index   int
}

func (r *fundingQueryRows) Columns() []string { return r.columns }
func (r *fundingQueryRows) Close() error      { return nil }
func (r *fundingQueryRows) Next(dest []driver.Value) error {
	if r.index >= len(r.values) {
		return io.EOF
	}
	copy(dest, r.values[r.index])
	r.index++
	return nil
}

func (r *runtimeMigrationRows) Columns() []string {
	return []string{"exists"}
}

func (r *runtimeMigrationRows) Close() error {
	return nil
}

func (r *runtimeMigrationRows) Next(dest []driver.Value) error {
	if r.read {
		return io.EOF
	}
	r.read = true
	copy(dest, r.values)
	return nil
}
