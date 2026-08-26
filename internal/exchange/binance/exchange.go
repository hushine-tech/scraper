package binance

import (
	"fmt"
	"strings"
	"time"

	"github.com/hushine-tech/scraper/internal/config"
	"github.com/hushine-tech/scraper/internal/exchange"
	bscraper "github.com/hushine-tech/scraper/internal/exchange/binance/scraper"
	"github.com/hushine-tech/scraper/internal/storage"
)

type Exchange struct{}

func New() exchange.ExchangeAdapter {
	return &Exchange{}
}

func (e *Exchange) Name() string { return "binance" }

func (e *Exchange) APIs() []exchange.API {
	return []exchange.API{
		{Name: "spot_kline_ws", Type: exchange.APITypeWS, Endpoint: "wss://stream.binance.com:9443/ws"},
		{Name: "spot_orderbook_ws", Type: exchange.APITypeWS, Endpoint: "wss://stream.binance.com:9443/ws"},
		{Name: "futures_open_interest_ws", Type: exchange.APITypeWS, Endpoint: "wss://fstream.binance.com/ws/btcusdt_openinterest"},
		{Name: "futures_open_interest_rest", Type: exchange.APITypeREST, Endpoint: "https://fapi.binance.com/fapi/v1/openInterest"},
		{Name: "futures_open_interest_hist_rest", Type: exchange.APITypeREST, Endpoint: "https://fapi.binance.com/fapi/v1/futures/data/openInterestHist"},
		{Name: "futures_funding_rate_rest", Type: exchange.APITypeREST, Endpoint: "https://fapi.binance.com/fapi/v1/fundingRate"},
		{Name: "futures_premium_index_rest", Type: exchange.APITypeREST, Endpoint: "https://fapi.binance.com/fapi/v1/premiumIndex"},
	}
}

func (e *Exchange) Build(cfg exchange.RuntimeConfig, store *storage.TimescaleDB) []exchange.Scraper {
	scrapers := []exchange.Scraper{}
	mode := strings.ToLower(strings.TrimSpace(cfg.Mode))
	reverse := mode == "reverse"

	intervals := cfg.KlineIntervals
	if len(intervals) == 0 {
		intervals = []string{"1m"}
	}

	for _, symbol := range cfg.SpotSymbols {
		if cfg.Forward.SpotKline || (reverse && cfg.Reverse.SpotKline) {
			for _, interval := range intervals {
				s := bscraper.NewKlineScraperWithInterval(symbol, "spot", cfg.ExchangeName, interval, store, cfg.Publisher)
				if reverse {
					s.SetReverse(parseReverseRange(cfg.Reverse))
				}
				scrapers = append(scrapers, s)
			}
		}
		if cfg.Forward.SpotOrderBook && !reverse {
			scrapers = append(scrapers, bscraper.NewOrderbookScraper(symbol, "spot", cfg.ExchangeName, store))
		}
	}

	for _, symbol := range cfg.FuturesSymbols {
		if cfg.Forward.FuturesKline || (reverse && cfg.Reverse.FuturesKline) {
			for _, interval := range intervals {
				s := bscraper.NewKlineScraperWithInterval(symbol, "futures", cfg.ExchangeName, interval, store, cfg.Publisher)
				if reverse {
					s.SetReverse(parseReverseRange(cfg.Reverse))
				}
				scrapers = append(scrapers, s)
			}
		}
		if cfg.Forward.FuturesOrderBook && !reverse {
			scrapers = append(scrapers, bscraper.NewOrderbookScraper(symbol, "futures", cfg.ExchangeName, store))
		}
	}

	if (cfg.Forward.FuturesOpenInterest && !reverse) || (reverse && cfg.Reverse.FuturesOpenInterest) {
		s := bscraper.NewOpenInterestScraper(cfg.FuturesSymbols, cfg.ExchangeName, store)
		if reverse {
			s.SetReverse(parseReverseRange(cfg.Reverse))
		}
		scrapers = append(scrapers, s)
	}

	return scrapers
}

func (e *Exchange) CollectFundingMarketData(cfg exchange.RuntimeConfig, store *storage.TimescaleDB) (exchange.Scraper, error) {
	if len(cfg.FuturesSymbols) == 0 {
		return nil, fmt.Errorf("Binance Funding market data requires Futures symbols")
	}
	collector := NewFundingMarketDataCollector(cfg.FuturesSymbols, store)
	if strings.EqualFold(strings.TrimSpace(cfg.Mode), "reverse") {
		collector.SetReverse(parseReverseRange(cfg.Reverse))
	}
	return collector, nil
}

func parseReverseRange(cfg config.ReverseConfig) (time.Time, time.Time) {
	start := time.Now().UTC().AddDate(0, -1, 0)
	end := time.Now().UTC()
	if t, err := time.Parse(time.RFC3339, cfg.StartTime); err == nil {
		start = t.UTC()
	}
	if t, err := time.Parse(time.RFC3339, cfg.EndTime); err == nil {
		end = t.UTC()
	}
	return start, end
}
