package exchange

import "context"

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

type MarketData interface {
	Kline(symbol string, market string, reverse bool) []Scraper
	OrderBook(symbol string, market string, reverse bool) []Scraper
	FundingRate(symbols []string, reverse bool) Scraper
	OpenInterest(symbols []string, reverse bool) Scraper
}
