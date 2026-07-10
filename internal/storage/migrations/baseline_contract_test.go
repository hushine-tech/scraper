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

func TestCurrentBaselineContainsAllMarketDataContracts(t *testing.T) {
	raw, err := os.ReadFile("0001_current_schema_baseline.sql")
	if err != nil {
		t.Fatalf("read current schema baseline: %v", err)
	}
	sql := strings.ToLower(string(raw))
	for _, forbidden := range []string{"drop table", "drop column", "rename column", "account_id"} {
		if strings.Contains(sql, forbidden) {
			t.Fatalf("current baseline contains historical operation/name %q", forbidden)
		}
	}
	for _, required := range []string{
		"create extension if not exists timescaledb",
		"create table if not exists spot_klines",
		"create table if not exists futures_klines",
		"create table if not exists spot_orderbook",
		"create table if not exists futures_orderbook",
		"create table if not exists futures_funding_rates",
		"create table if not exists futures_open_interest",
		"create or replace function symbol_year_table_name",
		"create or replace function ensure_symbol_year_hypertable",
		"chunk_time_interval => interval '1 month'",
		"create_hypertable",
	} {
		if !strings.Contains(sql, required) {
			t.Fatalf("current baseline missing market-data contract %q", required)
		}
	}
}
