package binance

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/hushine-tech/golang-lib/middleware/httpclient"
	elog "github.com/hushine-tech/golang-lib/pkg/log"
	"github.com/hushine-tech/scraper/internal/config"
	"github.com/hushine-tech/scraper/internal/exchange"
	"github.com/hushine-tech/scraper/internal/exchange/okx"
	"github.com/hushine-tech/scraper/internal/models"
)

type fundingTestDoer struct {
	do func(*http.Request) (*http.Response, error)
}

func (d fundingTestDoer) Do(req *http.Request) (*http.Response, error) { return d.do(req) }

type fundingNoopExtAPILogger struct{}

func (fundingNoopExtAPILogger) ExtAPI(context.Context, elog.ExtAPILogEntry) {}

func TestHistoricalFundingUsesExchangeTimesAndExactDecimals(t *testing.T) {
	collector := &fundingMarketDataCollector{
		httpClient: httpclient.New(fundingTestDoer{do: func(req *http.Request) (*http.Response, error) {
			if got, want := req.URL.Path, "/fapi/v1/fundingRate"; got != want {
				t.Fatalf("funding history path = %q, want %q", got, want)
			}
			if got, want := req.URL.Query().Get("symbol"), "BTCUSDT"; got != want {
				t.Fatalf("funding history symbol = %q, want %q", got, want)
			}
			return &http.Response{
				StatusCode: http.StatusOK,
				Header:     make(http.Header),
				Body: io.NopCloser(strings.NewReader(`[
					{"symbol":"BTCUSDT","fundingRate":"0.000100000000000001","markPrice":"20000.123456789012345678","fundingTime":1000},
					{"symbol":"BTCUSDT","fundingRate":"-0.000200000000000002","markPrice":"20001.000000000000000001","fundingTime":13000}
				]`)),
			}, nil
		}}, fundingNoopExtAPILogger{}, "funding_test"),
	}

	rates, err := collector.fetchHistoricalFundingRates(context.Background(), "btcusdt", time.UnixMilli(0), time.UnixMilli(20000))
	if err != nil {
		t.Fatalf("fetch historical funding: %v", err)
	}
	if len(rates) != 2 {
		t.Fatalf("historical funding rows = %d, want 2", len(rates))
	}
	if got, want := rates[0].FundingRateDecimal, "0.000100000000000001"; got != want {
		t.Fatalf("funding rate decimal = %q, want %q", got, want)
	}
	if got, want := rates[0].MarkPriceDecimal, "20000.123456789012345678"; got != want {
		t.Fatalf("mark price decimal = %q, want %q", got, want)
	}
	if got, want := rates[0].FundingTime, time.UnixMilli(1000).UTC(); !got.Equal(want) {
		t.Fatalf("funding time = %s, want %s", got, want)
	}
	if rates[0].NextFundingTime == nil || !rates[0].NextFundingTime.Equal(time.UnixMilli(13000).UTC()) {
		t.Fatalf("first next funding time = %v, want exchange row at %s", rates[0].NextFundingTime, time.UnixMilli(13000).UTC())
	}
	if rates[1].NextFundingTime != nil {
		t.Fatalf("last historical next funding time = %s, want unknown", rates[1].NextFundingTime)
	}
}

func TestCurrentFundingUsesExchangeNextFundingTime(t *testing.T) {
	collector := &fundingMarketDataCollector{
		httpClient: httpclient.New(fundingTestDoer{do: func(req *http.Request) (*http.Response, error) {
			if got, want := req.URL.Path, "/fapi/v1/premiumIndex"; got != want {
				t.Fatalf("premium index path = %q, want %q", got, want)
			}
			return &http.Response{
				StatusCode: http.StatusOK,
				Header:     make(http.Header),
				Body:       io.NopCloser(strings.NewReader(`{"symbol":"BTCUSDT","markPrice":"20000.123456789012345678","lastFundingRate":"0.000100000000000001","nextFundingTime":17000,"time":15000}`)),
			}, nil
		}}, fundingNoopExtAPILogger{}, "funding_test"),
	}

	rate, err := collector.fetchCurrentFundingRate(context.Background(), "btcusdt")
	if err != nil {
		t.Fatalf("fetch current funding: %v", err)
	}
	if got, want := rate.NextFundingTime, time.UnixMilli(17000).UTC(); got == nil || !got.Equal(want) {
		t.Fatalf("next funding time = %v, want %s", got, want)
	}
	if got, want := rate.FundingRateDecimal, "0.000100000000000001"; got != want {
		t.Fatalf("current funding rate decimal = %q, want %q", got, want)
	}
	if got, want := rate.MarkPriceDecimal, "20000.123456789012345678"; got != want {
		t.Fatalf("current mark price decimal = %q, want %q", got, want)
	}
}

func TestRegistryKeepsOKXFundingFailClosed(t *testing.T) {
	registry := exchange.NewRegistry()
	registry.Register("binance", New)
	registry.Register("okx", okx.New)

	_, err := registry.Build("okx", exchange.RuntimeConfig{
		Mode:           "forward",
		ExchangeName:   "okx",
		FuturesSymbols: []string{"BTCUSDT"},
		Forward: config.ForwardConfig{
			FundingRate: true,
		},
	}, nil)
	if err == nil || !strings.Contains(err.Error(), "funding market data") {
		t.Fatalf("OKX funding build error = %v, want unsupported funding market data", err)
	}
}

func TestFundingRateUsesCanonicalRouteTypes(t *testing.T) {
	rate := models.FundingRate{Exchange: models.ExchangeBinance, Market: models.MarketFutures}
	if rate.Exchange != models.ExchangeBinance || rate.Market != models.MarketFutures {
		t.Fatalf("funding route = %+v, want Binance Futures", rate)
	}
}
