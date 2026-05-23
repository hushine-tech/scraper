package okx

import (
	"github.com/hushine-tech/scraper/internal/exchange"
	"github.com/hushine-tech/scraper/internal/storage"
)

type Exchange struct{}

func New() exchange.ExchangeAdapter { return &Exchange{} }

func (e *Exchange) Name() string { return "okx" }

func (e *Exchange) APIs() []exchange.API {
	return []exchange.API{
		{Name: "placeholder_rest", Type: exchange.APITypeREST, Endpoint: "https://www.okx.com"},
	}
}

func (e *Exchange) Build(cfg exchange.RuntimeConfig, store *storage.TimescaleDB) []exchange.Scraper {
	_ = cfg
	_ = store
	// Intentionally left as placeholder for future OKX implementation.
	return nil
}
