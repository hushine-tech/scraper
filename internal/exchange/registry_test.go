package exchange

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/hushine-tech/scraper/internal/storage"
)

type historicalFundingTestAdapter struct {
	name       string
	backfiller HistoricalFundingBackfiller
}

func (a historicalFundingTestAdapter) Name() string { return a.name }
func (historicalFundingTestAdapter) APIs() []API    { return nil }
func (historicalFundingTestAdapter) Build(RuntimeConfig, *storage.TimescaleDB) []Scraper {
	return nil
}
func (a historicalFundingTestAdapter) HistoricalFundingBackfiller() HistoricalFundingBackfiller {
	return a.backfiller
}

type historicalFundingTestBackfiller struct{ got HistoricalFundingRequest }

func (b *historicalFundingTestBackfiller) BackfillFundingHistory(_ context.Context, req HistoricalFundingRequest, _ HistoricalFundingStore) ([]HistoricalFundingCoverageSegment, error) {
	b.got = req
	return []HistoricalFundingCoverageSegment{{Year: 2026, StartAt: req.StartAt, EndAt: req.EndAt, RowCount: 0, Source: "test"}}, nil
}

func TestRegistryHistoricalFundingBindsCanonicalExchangeWithoutBinanceBranch(t *testing.T) {
	backfiller := &historicalFundingTestBackfiller{}
	registry := NewRegistry()
	registry.Register("paperx", func() ExchangeAdapter {
		return historicalFundingTestAdapter{name: "paperx", backfiller: backfiller}
	})
	bound, err := registry.HistoricalFunding("paperx")
	if err != nil {
		t.Fatalf("HistoricalFunding: %v", err)
	}
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	end := start.Add(time.Hour)
	if _, err := bound.BackfillFundingHistory(context.Background(), HistoricalFundingRequest{
		Exchange: "spoofed", Market: "futures", Symbol: "btcusdt", StartAt: start, EndAt: end,
	}, nil); err != nil {
		t.Fatalf("BackfillFundingHistory: %v", err)
	}
	if backfiller.got.Exchange != "paperx" {
		t.Fatalf("bound exchange = %q, want paperx", backfiller.got.Exchange)
	}
}

func TestRegistryHistoricalFundingMissingCapabilityIsTypedFailClosed(t *testing.T) {
	registry := NewRegistry()
	registry.Register("okx", func() ExchangeAdapter { return historicalFundingTestAdapter{name: "okx"} })
	_, err := registry.HistoricalFunding("okx")
	if !errors.Is(err, ErrHistoricalFundingUnsupported) {
		t.Fatalf("HistoricalFunding error = %v, want ErrHistoricalFundingUnsupported", err)
	}
}
