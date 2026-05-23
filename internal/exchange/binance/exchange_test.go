package binance

import (
	"testing"

	"github.com/hushine-tech/scraper/internal/config"
	"github.com/hushine-tech/scraper/internal/exchange"
)

func TestBuildIncludesOpenInterestForwardAndReverse(t *testing.T) {
	ex := &Exchange{}
	fwdScrapers := ex.Build(exchange.RuntimeConfig{
		Mode:         "forward",
		ExchangeName: "binance",
		SpotSymbols:  []string{"btcusdt"},
		FuturesSymbols: []string{"btcusdt"},
		Forward: config.ForwardConfig{
			SpotKline:           true,
			FuturesKline:        true,
			SpotOrderBook:       true,
			FuturesOrderBook:    true,
			FundingRate:         true,
			FuturesOpenInterest: true,
		},
	}, nil)
	if len(fwdScrapers) != 6 {
		t.Fatalf("expected 6 scrapers in forward mode, got %d", len(fwdScrapers))
	}

	revScrapers := ex.Build(exchange.RuntimeConfig{
		Mode:         "reverse",
		ExchangeName: "binance",
		SpotSymbols:  []string{"btcusdt"},
		FuturesSymbols: []string{"btcusdt"},
		Reverse: config.ReverseConfig{
			SpotKline:           true,
			FuturesKline:        true,
			FundingRate:         true,
			FuturesOpenInterest: true,
		},
	}, nil)
	if len(revScrapers) != 4 {
		t.Fatalf("expected 4 scrapers in reverse mode, got %d", len(revScrapers))
	}
}
