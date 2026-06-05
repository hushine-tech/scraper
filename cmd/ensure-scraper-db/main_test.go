package main

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"io"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
)

func TestTargetDatabasesDefaultToCurrentYearExchangeDatabases(t *testing.T) {
	t.Setenv("SCRAPER_DBS", "")
	t.Setenv("SCRAPER_EXCHANGES", "")
	t.Setenv("SCRAPER_YEARS", "")

	got, err := targetDatabases(2026)
	if err != nil {
		t.Fatalf("targetDatabases: %v", err)
	}
	want := []string{"binance_2026", "okx_2026"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("targetDatabases = %v, want %v", got, want)
	}
}

func TestTargetDatabasesAllowExplicitYearDatabases(t *testing.T) {
	t.Setenv("SCRAPER_DBS", "binance_2025, okx_2026")

	got, err := targetDatabases(2026)
	if err != nil {
		t.Fatalf("targetDatabases: %v", err)
	}
	want := []string{"binance_2025", "okx_2026"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("targetDatabases = %v, want %v", got, want)
	}
}

func TestTargetDatabasesRejectFixedExchangeDatabases(t *testing.T) {
	t.Setenv("SCRAPER_DBS", "binance,okx")

	_, err := targetDatabases(2026)
	if err == nil || !strings.Contains(err.Error(), "fixed exchange database") {
		t.Fatalf("targetDatabases error = %v, want fixed exchange database rejection", err)
	}
}

func TestTargetDatabasesUseConfiguredExchangesAndYears(t *testing.T) {
	t.Setenv("SCRAPER_DBS", "")
	t.Setenv("SCRAPER_EXCHANGES", "binance")
	t.Setenv("SCRAPER_YEARS", "2025, 2026")

	got, err := targetDatabases(2026)
	if err != nil {
		t.Fatalf("targetDatabases: %v", err)
	}
	want := []string{"binance_2025", "binance_2026"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("targetDatabases = %v, want %v", got, want)
	}
}

func TestApplyMigrationsToDBRecordsAndSkipsAppliedFiles(t *testing.T) {
	state := &migrationTestState{
		applied: map[string]bool{
			"0001_existing.sql": true,
		},
	}
	db := openMigrationTestDB(t, state)
	defer db.Close()

	migs := []migration{
		{name: "0001_existing.sql", sql: "SELECT existing"},
		{name: "0002_new.sql", sql: "SELECT new"},
	}

	if err := applyMigrationsToDB(db, "binance_2026", migs); err != nil {
		t.Fatalf("applyMigrationsToDB: %v", err)
	}

	if got := state.execCount("SELECT existing"); got != 0 {
		t.Fatalf("already applied migration executed %d times, want 0", got)
	}
	if got := state.execCount("SELECT new"); got != 1 {
		t.Fatalf("new migration executed %d times, want 1", got)
	}
	if !state.applied["0002_new.sql"] {
		t.Fatalf("new migration was not recorded in schema_migrations")
	}
}

var migrationTestDriverSeq atomic.Uint64

type migrationTestState struct {
	mu      sync.Mutex
	applied map[string]bool
	execSQL []string
}

func (s *migrationTestState) execCount(query string) int {
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

func openMigrationTestDB(t *testing.T, state *migrationTestState) *sql.DB {
	t.Helper()
	driverName := fmt.Sprintf("migration-test-%d", migrationTestDriverSeq.Add(1))
	sql.Register(driverName, migrationTestDriver{state: state})
	db, err := sql.Open(driverName, "")
	if err != nil {
		t.Fatalf("open test db: %v", err)
	}
	return db
}

type migrationTestDriver struct {
	state *migrationTestState
}

func (d migrationTestDriver) Open(string) (driver.Conn, error) {
	return &migrationTestConn{state: d.state}, nil
}

type migrationTestConn struct {
	state *migrationTestState
}

func (c *migrationTestConn) Prepare(string) (driver.Stmt, error) {
	return nil, fmt.Errorf("Prepare is not implemented")
}

func (c *migrationTestConn) Close() error {
	return nil
}

func (c *migrationTestConn) Begin() (driver.Tx, error) {
	return migrationTestTx{}, nil
}

func (c *migrationTestConn) Ping(context.Context) error {
	return nil
}

func (c *migrationTestConn) ExecContext(_ context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	normalized := strings.TrimSpace(query)
	c.state.mu.Lock()
	defer c.state.mu.Unlock()
	if strings.HasPrefix(normalized, "CREATE TABLE IF NOT EXISTS schema_migrations") {
		if c.state.applied == nil {
			c.state.applied = map[string]bool{}
		}
		return driver.RowsAffected(0), nil
	}
	if strings.HasPrefix(normalized, "INSERT INTO schema_migrations") {
		if len(args) > 0 {
			c.state.applied[fmt.Sprint(args[0].Value)] = true
		}
		return driver.RowsAffected(1), nil
	}
	c.state.execSQL = append(c.state.execSQL, normalized)
	return driver.RowsAffected(1), nil
}

func (c *migrationTestConn) QueryContext(_ context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	normalized := strings.TrimSpace(query)
	if strings.HasPrefix(normalized, "SELECT EXISTS(SELECT 1 FROM schema_migrations") {
		filename := ""
		if len(args) > 0 {
			filename = fmt.Sprint(args[0].Value)
		}
		c.state.mu.Lock()
		applied := c.state.applied[filename]
		c.state.mu.Unlock()
		return &migrationTestRows{values: []driver.Value{applied}}, nil
	}
	return nil, fmt.Errorf("unexpected query: %s", query)
}

type migrationTestTx struct{}

func (migrationTestTx) Commit() error {
	return nil
}

func (migrationTestTx) Rollback() error {
	return nil
}

type migrationTestRows struct {
	values []driver.Value
	read   bool
}

func (r *migrationTestRows) Columns() []string {
	return []string{"exists"}
}

func (r *migrationTestRows) Close() error {
	return nil
}

func (r *migrationTestRows) Next(dest []driver.Value) error {
	if r.read {
		return io.EOF
	}
	r.read = true
	copy(dest, r.values)
	return nil
}
