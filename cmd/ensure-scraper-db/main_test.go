package main

import (
	"os"
	"path/filepath"
	"reflect"
	"strings"
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

func TestLoadMigrationsSkipsLegacyInitWhenSplitMigrationsExist(t *testing.T) {
	dir := t.TempDir()
	files := map[string]string{
		"0001_enable_timescaledb.sql": "SELECT 1;",
		"0002_create_klines.sql":      "SELECT 2;",
		"001_init.sql":                "SELECT 999;",
	}
	for name, body := range files {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(body), 0o644); err != nil {
			t.Fatalf("write migration %s: %v", name, err)
		}
	}

	got, err := loadMigrations(dir)
	if err != nil {
		t.Fatalf("loadMigrations: %v", err)
	}
	want := []string{"0001_enable_timescaledb.sql", "0002_create_klines.sql"}
	if len(got) != len(want) {
		t.Fatalf("loadMigrations count = %d (%v), want %d", len(got), got, len(want))
	}
	for i, name := range want {
		if got[i].name != name {
			t.Fatalf("migration[%d] = %q, want %q", i, got[i].name, name)
		}
	}
}
