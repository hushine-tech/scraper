package backfill

import (
	"testing"
	"time"

	"github.com/hushine-tech/scraper/internal/models"
)

func TestDatabaseForYear_AppendsSuffixWhenTemplateMissing(t *testing.T) {
	if got := databaseForYear("binance", "binance", 2026); got != "binance_2026" {
		t.Fatalf("databaseForYear() = %q, want %q", got, "binance_2026")
	}
}

func TestDatabaseForYear_ReplacesTemplateWhenPresent(t *testing.T) {
	if got := databaseForYear("binance_{year}", "binance", 2025); got != "binance_2025" {
		t.Fatalf("databaseForYear() = %q, want %q", got, "binance_2025")
	}
}

func TestDatabaseForYear_ReplacesExchangeAndYearTemplate(t *testing.T) {
	if got := databaseForYear("{exchange}_{year}", "binance", 2026); got != "binance_2026" {
		t.Fatalf("databaseForYear() = %q, want %q", got, "binance_2026")
	}
}

func TestSplitByYear_SplitsCrossYearWindow(t *testing.T) {
	start := time.Date(2025, 12, 31, 23, 59, 0, 0, time.UTC)
	end := time.Date(2026, 1, 1, 0, 2, 0, 0, time.UTC)

	windows := splitByYear(start, end)
	if len(windows) != 2 {
		t.Fatalf("splitByYear() len = %d, want 2", len(windows))
	}
	if windows[0].Year() != 2025 || windows[1].Year() != 2026 {
		t.Fatalf("splitByYear() years = [%d,%d], want [2025,2026]", windows[0].Year(), windows[1].Year())
	}
	if !windows[0].End.Equal(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)) {
		t.Fatalf("first window end = %s", windows[0].End)
	}
}

func TestExpectedBarCountUsesEndExclusiveWindow(t *testing.T) {
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	end := time.Date(2026, 1, 1, 0, 2, 0, 0, time.UTC)

	got, err := expectedBarCount(start, end, "1m")
	if err != nil {
		t.Fatalf("expectedBarCount: %v", err)
	}
	if got != 2 {
		t.Fatalf("expectedBarCount() = %d, want 2 for [start,end)", got)
	}

	got, err = expectedBarCount(start, start, "1m")
	if err != nil {
		t.Fatalf("expectedBarCount zero-length: %v", err)
	}
	if got != 0 {
		t.Fatalf("expectedBarCount zero-length = %d, want 0", got)
	}
}

func TestCoveredEndFromMaxOpenTimeReturnsExclusiveEnd(t *testing.T) {
	maxOpen := time.Date(2026, 1, 1, 0, 1, 0, 0, time.UTC)

	got, err := coveredEndFromMaxOpenTime(maxOpen, "1m")
	if err != nil {
		t.Fatalf("coveredEndFromMaxOpenTime: %v", err)
	}
	want := time.Date(2026, 1, 1, 0, 2, 0, 0, time.UTC)
	if !got.Equal(want) {
		t.Fatalf("covered end = %s, want %s", got, want)
	}
}

func TestAppendCoverageSegmentsMergesAdjacentPaginationSegments(t *testing.T) {
	first := CoverageSegment{
		Exchange: "binance",
		Market:   "futures",
		Symbol:   "BTCUSDT",
		Interval: "1m",
		Year:     2026,
		StartAt:  time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		EndAt:    time.Date(2026, 1, 1, 0, 2, 0, 0, time.UTC),
		RowCount: 2,
		Source:   "historical_backfill",
	}
	second := CoverageSegment{
		Exchange: "binance",
		Market:   "futures",
		Symbol:   "BTCUSDT",
		Interval: "1m",
		Year:     2026,
		StartAt:  time.Date(2026, 1, 1, 0, 2, 0, 0, time.UTC),
		EndAt:    time.Date(2026, 1, 1, 0, 4, 0, 0, time.UTC),
		RowCount: 2,
		Source:   "historical_backfill",
	}

	got := appendCoverageSegments(nil, first)
	got = appendCoverageSegments(got, second)
	if len(got) != 1 {
		t.Fatalf("segment count = %d, want 1: %#v", len(got), got)
	}
	if !got[0].StartAt.Equal(first.StartAt) || !got[0].EndAt.Equal(second.EndAt) || got[0].RowCount != 4 {
		t.Fatalf("merged segment = %#v", got[0])
	}
}

func TestFilterKlinesForWindowDropsEndBoundaryRows(t *testing.T) {
	window := timeWindow{
		Start: time.Date(2026, 12, 31, 23, 59, 0, 0, time.UTC),
		End:   time.Date(2027, 1, 1, 0, 0, 0, 0, time.UTC),
	}
	rows := []models.Kline{
		{OpenTime: window.Start.Add(-time.Minute)},
		{OpenTime: window.Start},
		{OpenTime: window.End},
	}

	got := filterKlinesForWindow(rows, window)
	if len(got) != 1 {
		t.Fatalf("filtered rows = %d, want 1: %#v", len(got), got)
	}
	if !got[0].OpenTime.Equal(window.Start) {
		t.Fatalf("kept open_time = %s, want %s", got[0].OpenTime, window.Start)
	}
}
