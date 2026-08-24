package migrations_test

import (
	"os"
	"reflect"
	"sort"
	"strings"
	"testing"
)

func TestCurrentMarketDataMigrationSetIsBaselineOnly(t *testing.T) {
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	for _, entry := range entries {
		if !entry.IsDir() && strings.HasSuffix(entry.Name(), ".sql") {
			got = append(got, entry.Name())
		}
	}
	sort.Strings(got)
	want := []string{"0001_current_schema_baseline.sql"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("market-data migration set = %v, want %v", got, want)
	}
}

func TestCurrentBaselineInstallsOnlyTimescaleForLazySymbolTables(t *testing.T) {
	raw, err := os.ReadFile("0001_current_schema_baseline.sql")
	if err != nil {
		t.Fatalf("read current schema baseline: %v", err)
	}
	sql := strings.ToLower(string(raw))
	for _, forbidden := range []string{
		"drop table",
		"drop column",
		"rename column",
		"account_id",
		"create table",
		"symbol_year_table_name",
		"ensure_symbol_year_hypertable",
		"spot_klines",
		"futures_klines",
		"spot_orderbook",
		"futures_orderbook",
		"futures_funding_rates",
		"futures_open_interest",
	} {
		if strings.Contains(sql, forbidden) {
			t.Fatalf("current baseline contains obsolete fixed-schema operation/name %q", forbidden)
		}
	}
	if !strings.Contains(sql, "create extension if not exists timescaledb") {
		t.Fatal("current baseline must install TimescaleDB before lazy symbol-table creation")
	}
}
