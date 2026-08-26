package exchange

import (
	"context"
	"time"

	"github.com/hushine-tech/scraper/internal/models"
	"github.com/hushine-tech/scraper/internal/storage"
)

type APIType string

const (
	APITypeREST APIType = "rest"
	APITypeWS   APIType = "ws"
)

type API struct {
	Name     string
	Type     APIType
	Endpoint string
}

type Exchange interface {
	Name() string
	APIs() []API
}

type Scraper interface {
	Run(ctx context.Context)
	Stop()
}

// FundingMarketDataCollector is the optional Futures Funding capability an
// exchange adapter exposes to the common Registry.
type FundingMarketDataCollector interface {
	CollectFundingMarketData(RuntimeConfig, *storage.TimescaleDB) (Scraper, error)
}

type HistoricalFundingRequest struct {
	Exchange string
	Market   string
	Symbol   string
	StartAt  time.Time
	EndAt    time.Time
}

type HistoricalFundingCoverageSegment struct {
	Year     int
	StartAt  time.Time
	EndAt    time.Time
	RowCount int64
	Source   string
}

type HistoricalFundingStore interface {
	InsertFundingRate(context.Context, models.FundingRate) error
	LinkFundingRatePredecessor(context.Context, models.FundingRate) error
}

type HistoricalFundingBackfiller interface {
	BackfillFundingHistory(context.Context, HistoricalFundingRequest, HistoricalFundingStore) ([]HistoricalFundingCoverageSegment, error)
}

type HistoricalFundingBackfillerProvider interface {
	HistoricalFundingBackfiller() HistoricalFundingBackfiller
}
