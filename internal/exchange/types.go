package exchange

import (
	"context"

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
