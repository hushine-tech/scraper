package exchange

import (
	"fmt"
	"strings"

	"github.com/hushine-tech/scraper/internal/config"
	"github.com/hushine-tech/scraper/internal/marketdata"
	"github.com/hushine-tech/scraper/internal/storage"
)

type RuntimeConfig struct {
	Mode           string
	ExchangeName   string
	SpotSymbols    []string
	FuturesSymbols []string
	KlineIntervals []string // 抓取的 K 线周期列表，默认 ["1m"]
	Forward        config.ForwardConfig
	Reverse        config.ReverseConfig
	Publisher      marketdata.Publisher
}

type ExchangeAdapter interface {
	Exchange
	Build(RuntimeConfig, *storage.TimescaleDB) []Scraper
}

type Factory func() ExchangeAdapter

type Registry struct {
	factories map[string]Factory
}

func NewRegistry() *Registry {
	return &Registry{factories: make(map[string]Factory)}
}

func (r *Registry) Register(name string, factory Factory) {
	r.factories[strings.ToLower(strings.TrimSpace(name))] = factory
}

func (r *Registry) Build(name string, cfg RuntimeConfig, store *storage.TimescaleDB) ([]Scraper, error) {
	f, ok := r.factories[strings.ToLower(strings.TrimSpace(name))]
	if !ok {
		return nil, fmt.Errorf("exchange %q not registered", name)
	}
	adapter := f()
	scrapers := adapter.Build(cfg, store)
	if !fundingEnabled(cfg) || len(cfg.FuturesSymbols) == 0 {
		return scrapers, nil
	}
	collector, ok := adapter.(FundingMarketDataCollector)
	if !ok {
		return nil, fmt.Errorf("exchange %q does not support funding market data collection", name)
	}
	fundingScraper, err := collector.CollectFundingMarketData(cfg, store)
	if err != nil {
		return nil, err
	}
	return append(scrapers, fundingScraper), nil
}

func fundingEnabled(cfg RuntimeConfig) bool {
	reverse := strings.EqualFold(strings.TrimSpace(cfg.Mode), "reverse")
	return (!reverse && cfg.Forward.FundingRate) || (reverse && cfg.Reverse.FundingRate)
}
