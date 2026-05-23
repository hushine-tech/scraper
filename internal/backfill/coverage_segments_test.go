package backfill

import (
	"testing"
	"time"

	"github.com/hushine-tech/scraper/internal/models"
)

func TestBuildCoverageSegmentsSplitsGaps(t *testing.T) {
	rows := []models.Kline{
		klineAt("2026-01-01T00:00:00Z"),
		klineAt("2026-01-01T00:01:00Z"),
		klineAt("2026-01-01T00:03:00Z"),
	}

	got, err := BuildCoverageSegments("binance", "futures", "BTCUSDT", "1m", rows)
	if err != nil {
		t.Fatalf("BuildCoverageSegments: %v", err)
	}

	want := []CoverageSegment{
		{
			Exchange: "binance", Market: "futures", Symbol: "BTCUSDT", Interval: "1m",
			Year: 2026, StartAt: mustTime("2026-01-01T00:00:00Z"), EndAt: mustTime("2026-01-01T00:02:00Z"),
			RowCount: 2, Source: "historical_backfill",
		},
		{
			Exchange: "binance", Market: "futures", Symbol: "BTCUSDT", Interval: "1m",
			Year: 2026, StartAt: mustTime("2026-01-01T00:03:00Z"), EndAt: mustTime("2026-01-01T00:04:00Z"),
			RowCount: 1, Source: "historical_backfill",
		},
	}
	assertCoverageSegments(t, got, want)
}

func TestBuildCoverageSegmentsSplitsYearBoundary(t *testing.T) {
	rows := []models.Kline{
		klineAt("2026-12-31T23:59:00Z"),
		klineAt("2027-01-01T00:00:00Z"),
	}

	got, err := BuildCoverageSegments("binance", "spot", "ETHUSDT", "1m", rows)
	if err != nil {
		t.Fatalf("BuildCoverageSegments: %v", err)
	}

	want := []CoverageSegment{
		{
			Exchange: "binance", Market: "spot", Symbol: "ETHUSDT", Interval: "1m",
			Year: 2026, StartAt: mustTime("2026-12-31T23:59:00Z"), EndAt: mustTime("2027-01-01T00:00:00Z"),
			RowCount: 1, Source: "historical_backfill",
		},
		{
			Exchange: "binance", Market: "spot", Symbol: "ETHUSDT", Interval: "1m",
			Year: 2027, StartAt: mustTime("2027-01-01T00:00:00Z"), EndAt: mustTime("2027-01-01T00:01:00Z"),
			RowCount: 1, Source: "historical_backfill",
		},
	}
	assertCoverageSegments(t, got, want)
}

func TestBuildCoverageSegmentsDeduplicatesOutOfOrderRows(t *testing.T) {
	rows := []models.Kline{
		klineAt("2026-01-01T00:01:00Z"),
		klineAt("2026-01-01T00:00:00Z"),
		klineAt("2026-01-01T00:01:00Z"),
	}

	got, err := BuildCoverageSegments("binance", "futures", "BTCUSDT", "1m", rows)
	if err != nil {
		t.Fatalf("BuildCoverageSegments: %v", err)
	}

	want := []CoverageSegment{
		{
			Exchange: "binance", Market: "futures", Symbol: "BTCUSDT", Interval: "1m",
			Year: 2026, StartAt: mustTime("2026-01-01T00:00:00Z"), EndAt: mustTime("2026-01-01T00:02:00Z"),
			RowCount: 2, Source: "historical_backfill",
		},
	}
	assertCoverageSegments(t, got, want)
}

func klineAt(raw string) models.Kline {
	t := mustTime(raw)
	return models.Kline{OpenTime: t}
}

func mustTime(raw string) time.Time {
	t, err := time.Parse(time.RFC3339, raw)
	if err != nil {
		panic(err)
	}
	return t.UTC()
}

func assertCoverageSegments(t *testing.T, got, want []CoverageSegment) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("segment count = %d, want %d: %#v", len(got), len(want), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("segment[%d] = %#v, want %#v", i, got[i], want[i])
		}
	}
}
