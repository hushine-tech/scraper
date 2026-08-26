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
	"github.com/hushine-tech/scraper/internal/storage"
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

func TestHistoricalFundingCarriesNextTimeAcrossHTTPPages(t *testing.T) {
	var calls int
	collector := &fundingMarketDataCollector{
		httpClient: httpclient.New(fundingTestDoer{do: func(req *http.Request) (*http.Response, error) {
			calls++
			if got, want := req.URL.Path, "/fapi/v1/fundingRate"; got != want {
				t.Fatalf("funding history path = %q, want %q", got, want)
			}
			var body string
			switch calls {
			case 1:
				if got, want := req.URL.Query().Get("startTime"), "1"; got != want {
					t.Fatalf("first page start time = %q, want %q", got, want)
				}
				body = `[
					{"symbol":"BTCUSDT","fundingRate":"0.0001","markPrice":"100","fundingTime":1000},
					{"symbol":"BTCUSDT","fundingRate":"0.0002","markPrice":"101","fundingTime":2000}
				]`
			case 2:
				if got, want := req.URL.Query().Get("startTime"), "2001"; got != want {
					t.Fatalf("second page start time = %q, want %q", got, want)
				}
				body = `[{"symbol":"BTCUSDT","fundingRate":"0.0003","markPrice":"102","fundingTime":3000}]`
			default:
				t.Fatalf("unexpected Funding history request %d", calls)
			}
			return &http.Response{
				StatusCode: http.StatusOK,
				Header:     make(http.Header),
				Body:       io.NopCloser(strings.NewReader(body)),
			}, nil
		}}, fundingNoopExtAPILogger{}, "funding_test"),
		done: make(chan struct{}),
	}
	capture := &fundingRateCaptureStore{}
	collector.storage = storage.NewRoutedTimescaleDB(func(context.Context, string, int) (storage.MarketDataStore, error) {
		return capture, nil
	})

	if err := collector.backfillSymbol(
		context.Background(),
		"BTCUSDT",
		time.UnixMilli(1).UTC(),
		time.UnixMilli(3001).UTC(),
	); err != nil {
		t.Fatalf("backfill Funding pages: %v", err)
	}
	if calls != 2 {
		t.Fatalf("Funding HTTP pages = %d, want 2", calls)
	}
	if len(capture.rates) != 3 {
		t.Fatalf("stored Funding rows = %d, want 3", len(capture.rates))
	}
	if got, want := capture.rates[1].NextFundingTime, time.UnixMilli(3000).UTC(); got == nil || !got.Equal(want) {
		t.Fatalf("final row of first page next time = %v, want %s", got, want)
	}
	if capture.rates[2].NextFundingTime != nil {
		t.Fatalf("overall final Funding row next time = %s, want unknown", capture.rates[2].NextFundingTime)
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

func TestRegistryRejectsFundingWithoutFuturesSymbols(t *testing.T) {
	for _, tc := range []struct {
		name    string
		factory exchange.Factory
		wantErr string
	}{
		{name: "binance", factory: New, wantErr: "Futures symbols"},
		{name: "okx", factory: okx.New, wantErr: "funding market data"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			registry := exchange.NewRegistry()
			registry.Register(tc.name, tc.factory)
			_, err := registry.Build(tc.name, exchange.RuntimeConfig{
				Mode:         "forward",
				ExchangeName: tc.name,
				Forward: config.ForwardConfig{
					FundingRate: true,
				},
			}, nil)
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("empty-symbol Funding build error = %v, want %q", err, tc.wantErr)
			}
		})
	}
}

type fundingRateCaptureStore struct {
	rates []models.FundingRate
}

func (s *fundingRateCaptureStore) InsertKline(context.Context, []models.Kline) error { return nil }

func (s *fundingRateCaptureStore) InsertOrderBook(context.Context, models.OrderBook) error {
	return nil
}

func (s *fundingRateCaptureStore) InsertFundingRate(_ context.Context, rate models.FundingRate) error {
	s.rates = append(s.rates, rate)
	return nil
}

func (s *fundingRateCaptureStore) InsertOpenInterest(context.Context, models.OpenInterest) error {
	return nil
}

func (s *fundingRateCaptureStore) InsertOpenInterests(context.Context, []models.OpenInterest) error {
	return nil
}

func TestFundingRateUsesCanonicalRouteTypes(t *testing.T) {
	rate := models.FundingRate{Exchange: models.ExchangeBinance, Market: models.MarketFutures}
	if rate.Exchange != models.ExchangeBinance || rate.Market != models.MarketFutures {
		t.Fatalf("funding route = %+v, want Binance Futures", rate)
	}
}
