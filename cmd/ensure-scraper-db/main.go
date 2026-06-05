// ensure-scraper-db connects to PostgreSQL, creates the scraper's exchange-year
// databases (for example "binance_2026" and/or "okx_2026") if missing, and
// applies each one's migrations from internal/storage/migrations/.
//
// Scraper's main binary also auto-migrates at boot via storage.InitSchema, so
// this CLI is mainly a deploy-time convenience for operators who want DBs
// ready before services come online. Running both is safe — migrations use
// IF NOT EXISTS / ON CONFLICT and are idempotent.
//
// Usage:
//
//	go run ./cmd/ensure-scraper-db
//	PGHOST=192.168.88.10 PGUSER=postgres PGPASSWORD=postgres go run ./cmd/ensure-scraper-db
//
// By default the current-year binance/okx databases are created. To target
// specific year databases, set SCRAPER_DBS to a comma-separated list, e.g.:
//
//	SCRAPER_DBS=binance_2026 go run ./cmd/ensure-scraper-db
//
// Or set SCRAPER_EXCHANGES and SCRAPER_YEARS, e.g.:
//
//	SCRAPER_EXCHANGES=binance SCRAPER_YEARS=2025,2026 go run ./cmd/ensure-scraper-db
package main

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	_ "github.com/lib/pq"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "ensure-scraper-db: %v\n", err)
		os.Exit(1)
	}
	fmt.Println("ensure-scraper-db: OK (all target databases + migrations)")
}

func run() error {
	host := getenv("PGHOST", "192.168.88.10")
	port := getenv("PGPORT", "5432")
	user := getenv("PGUSER", "postgres")
	pass := getenv("PGPASSWORD", "postgres")
	dbnameAdmin := getenv("PGDATABASE_ADMIN", "postgres")

	targets, err := targetDatabases(time.Now().UTC().Year())
	if err != nil {
		return err
	}

	root, err := findModuleRoot()
	if err != nil {
		return err
	}
	migDir := filepath.Join(root, "internal", "storage", "migrations")
	migrationSQL, err := loadMigrations(migDir)
	if err != nil {
		return err
	}

	adminDSN := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=disable",
		host, port, user, pass, dbnameAdmin)

	for _, dbname := range targets {
		if err := ensureDatabase(adminDSN, dbname); err != nil {
			return fmt.Errorf("%s: %w", dbname, err)
		}
		dsn := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=disable",
			host, port, user, pass, dbname)
		if err := applyMigrations(dsn, dbname, migrationSQL); err != nil {
			return fmt.Errorf("%s: %w", dbname, err)
		}
	}
	return nil
}

func ensureDatabase(adminDSN, dbname string) error {
	admin, err := sql.Open("postgres", adminDSN)
	if err != nil {
		return fmt.Errorf("open admin: %w", err)
	}
	defer admin.Close()
	if err := admin.Ping(); err != nil {
		return fmt.Errorf("ping postgres: %w", err)
	}

	var exists bool
	if err := admin.QueryRow(
		`SELECT EXISTS(SELECT 1 FROM pg_database WHERE datname = $1)`, dbname,
	).Scan(&exists); err != nil {
		return fmt.Errorf("check database: %w", err)
	}
	if exists {
		fmt.Printf("database %s already exists\n", dbname)
		return nil
	}
	// CREATE DATABASE does not accept parameters — validate the identifier
	// so a malformed SCRAPER_DBS entry cannot inject SQL.
	if !isValidIdent(dbname) {
		return fmt.Errorf("refusing to create db with suspicious name %q", dbname)
	}
	if _, err := admin.Exec(fmt.Sprintf(`CREATE DATABASE %s`, dbname)); err != nil {
		return fmt.Errorf("CREATE DATABASE %s: %w", dbname, err)
	}
	fmt.Printf("created database: %s\n", dbname)
	return nil
}

type migration struct {
	name string
	sql  string
}

func loadMigrations(migDir string) ([]migration, error) {
	entries, err := os.ReadDir(migDir)
	if err != nil {
		return nil, fmt.Errorf("read migrations dir %s: %w", migDir, err)
	}
	var names []string
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".sql") {
			continue
		}
		names = append(names, e.Name())
	}
	sort.Strings(names)

	out := make([]migration, 0, len(names))
	for _, name := range names {
		body, err := os.ReadFile(filepath.Join(migDir, name))
		if err != nil {
			return nil, fmt.Errorf("read %s: %w", name, err)
		}
		sqlText := strings.TrimSpace(string(body))
		if sqlText == "" {
			continue
		}
		out = append(out, migration{name: name, sql: sqlText})
	}
	return out, nil
}

func applyMigrations(dsn, dbname string, migs []migration) error {
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return fmt.Errorf("open %s: %w", dbname, err)
	}
	defer db.Close()
	return applyMigrationsToDB(db, dbname, migs)
}

func applyMigrationsToDB(db *sql.DB, dbname string, migs []migration) error {
	if err := db.Ping(); err != nil {
		return fmt.Errorf("ping %s: %w", dbname, err)
	}
	if _, err := db.Exec(`
CREATE TABLE IF NOT EXISTS schema_migrations (
    filename   TEXT PRIMARY KEY,
    applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
)`); err != nil {
		return fmt.Errorf("ensure schema_migrations on %s: %w", dbname, err)
	}

	for _, m := range migs {
		tx, err := db.Begin()
		if err != nil {
			return fmt.Errorf("begin %s on %s: %w", m.name, dbname, err)
		}
		var alreadyApplied bool
		if err := tx.QueryRow(`SELECT EXISTS(SELECT 1 FROM schema_migrations WHERE filename = $1)`, m.name).Scan(&alreadyApplied); err != nil {
			_ = tx.Rollback()
			return fmt.Errorf("check migration %s on %s: %w", m.name, dbname, err)
		}
		if alreadyApplied {
			_ = tx.Rollback()
			fmt.Printf("[%s] skipped: %s\n", dbname, m.name)
			continue
		}
		if _, err := tx.Exec(m.sql); err != nil {
			_ = tx.Rollback()
			return fmt.Errorf("exec %s on %s: %w", m.name, dbname, err)
		}
		if _, err := tx.Exec(`INSERT INTO schema_migrations (filename) VALUES ($1)`, m.name); err != nil {
			_ = tx.Rollback()
			return fmt.Errorf("record migration %s on %s: %w", m.name, dbname, err)
		}
		if err := tx.Commit(); err != nil {
			return fmt.Errorf("commit migration %s on %s: %w", m.name, dbname, err)
		}
		fmt.Printf("[%s] applied: %s\n", dbname, m.name)
	}
	return nil
}

func getenv(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func targetDatabases(currentYear int) ([]string, error) {
	if raw := strings.TrimSpace(os.Getenv("SCRAPER_DBS")); raw != "" {
		parts := splitCSV(raw)
		targets := make([]string, 0, len(parts))
		for _, dbname := range parts {
			if err := validateExchangeYearDatabase(dbname); err != nil {
				return nil, err
			}
			targets = append(targets, dbname)
		}
		return targets, nil
	}

	exchanges := splitCSV(getenv("SCRAPER_EXCHANGES", "binance,okx"))
	years, err := targetYears(getenv("SCRAPER_YEARS", strconv.Itoa(currentYear)))
	if err != nil {
		return nil, err
	}
	targets := make([]string, 0, len(exchanges)*len(years))
	for _, exchange := range exchanges {
		if exchange != "binance" && exchange != "okx" {
			return nil, fmt.Errorf("unsupported scraper exchange %q", exchange)
		}
		for _, year := range years {
			targets = append(targets, fmt.Sprintf("%s_%d", exchange, year))
		}
	}
	if len(targets) == 0 {
		return nil, fmt.Errorf("no scraper target databases configured")
	}
	return targets, nil
}

func splitCSV(raw string) []string {
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		p := strings.TrimSpace(part)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}

func targetYears(raw string) ([]int, error) {
	parts := splitCSV(raw)
	years := make([]int, 0, len(parts))
	for _, part := range parts {
		year, err := strconv.Atoi(part)
		if err != nil || year < 1970 || year > 9999 {
			return nil, fmt.Errorf("invalid scraper year %q", part)
		}
		years = append(years, year)
	}
	if len(years) == 0 {
		return nil, fmt.Errorf("no scraper years configured")
	}
	return years, nil
}

func validateExchangeYearDatabase(dbname string) error {
	if dbname == "binance" || dbname == "okx" {
		return fmt.Errorf("fixed exchange database %q is no longer supported; use %s_<year>", dbname, dbname)
	}
	for _, exchange := range []string{"binance", "okx"} {
		prefix := exchange + "_"
		if strings.HasPrefix(dbname, prefix) {
			yearPart := strings.TrimPrefix(dbname, prefix)
			year, err := strconv.Atoi(yearPart)
			if err != nil || len(yearPart) != 4 || year < 1970 {
				return fmt.Errorf("invalid exchange-year database %q", dbname)
			}
			return nil
		}
	}
	return fmt.Errorf("unsupported scraper database %q; expected {exchange}_{year}", dbname)
}

func findModuleRoot() (string, error) {
	dir, err := os.Getwd()
	if err != nil {
		return "", err
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir, nil
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return "", fmt.Errorf("go.mod not found from cwd")
		}
		dir = parent
	}
}

func isValidIdent(s string) bool {
	if s == "" || len(s) > 63 {
		return false
	}
	for i, r := range s {
		alpha := (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z')
		digit := r >= '0' && r <= '9'
		if i == 0 && !alpha && r != '_' {
			return false
		}
		if !alpha && !digit && r != '_' {
			return false
		}
	}
	return true
}
