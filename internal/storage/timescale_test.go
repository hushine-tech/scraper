package storage

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hushine-tech/scraper/internal/logger"
	"github.com/hushine-tech/golang-lib/middleware/sqlmiddleware"
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

func TestListMigrationFilesSkipsLegacyInitWhenSplitMigrationsExist(t *testing.T) {
	dir := t.TempDir()
	files := []string{
		"0001_a.sql",
		"0002_b.sql",
		"001_init.sql",
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

	want := []string{"0001_a.sql", "0002_b.sql"}
	if len(got) != len(want) {
		t.Fatalf("expected %d files got %d (%v)", len(want), len(got), got)
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

func TestBuildSymbolYearTableNameUsesRecordTimestamp(t *testing.T) {
	table2025 := buildSymbolYearTableName("futures", "funding_rates", "btcusdt", time.Date(2025, 12, 31, 23, 0, 0, 0, time.UTC))
	table2026 := buildSymbolYearTableName("futures", "funding_rates", "btcusdt", time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))

	if table2025 != "futures_funding_rates_BTCUSDT_2025" {
		t.Fatalf("unexpected table name: %s", table2025)
	}
	if table2026 != "futures_funding_rates_BTCUSDT_2026" {
		t.Fatalf("unexpected table name: %s", table2026)
	}
}

func TestBuildReadTableNamesCrossYearIncludesLegacyFallback(t *testing.T) {
	ts := &TimescaleDB{}
	tables := ts.buildReadTableNames(
		"futures",
		"funding_rates",
		"btcusdt",
		time.Date(2025, 12, 31, 23, 0, 0, 0, time.UTC),
		time.Date(2026, 1, 1, 1, 0, 0, 0, time.UTC),
	)

	want := []string{
		"futures_funding_rates_btcusdt",
		"futures_funding_rates_BTCUSDT_2025",
		"futures_funding_rates_BTCUSDT_2026",
		"futures_funding_rates",
	}
	if len(tables) != len(want) {
		t.Fatalf("unexpected table count: got=%d want=%d tables=%v", len(tables), len(want), tables)
	}
	for i := range want {
		if tables[i] != want[i] {
			t.Fatalf("unexpected table[%d]: got=%s want=%s", i, tables[i], want[i])
		}
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
