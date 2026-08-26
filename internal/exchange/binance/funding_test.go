package binance

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"reflect"
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
	rates              []models.FundingRate
	insertErr          error
	predecessor        *models.FundingRate
	predecessorErr     error
	predecessorQueries []models.FundingRate
	adjacencyLinks     []fundingAdjacencyLink
	adjacencyErr       error
}

type fundingAdjacencyLink struct {
	predecessor models.FundingRate
	successor   models.FundingRate
}

func (s *fundingRateCaptureStore) InsertKline(context.Context, []models.Kline) error { return nil }

func (s *fundingRateCaptureStore) InsertOrderBook(context.Context, models.OrderBook) error {
	return nil
}

func (s *fundingRateCaptureStore) InsertFundingRate(_ context.Context, rate models.FundingRate) error {
	if s.insertErr != nil {
		return s.insertErr
	}
	s.rates = append(s.rates, rate)
	return nil
}

func (s *fundingRateCaptureStore) FindFundingRatePredecessor(_ context.Context, successor models.FundingRate) (*models.FundingRate, error) {
	s.predecessorQueries = append(s.predecessorQueries, successor)
	if s.predecessorErr != nil {
		return nil, s.predecessorErr
	}
	if s.predecessor == nil {
		return nil, nil
	}
	candidate := *s.predecessor
	return &candidate, nil
}

func (s *fundingRateCaptureStore) LinkFundingRateAdjacency(_ context.Context, predecessor, successor models.FundingRate) error {
	if s.adjacencyErr != nil {
		return s.adjacencyErr
	}
	s.adjacencyLinks = append(s.adjacencyLinks, fundingAdjacencyLink{predecessor: predecessor, successor: successor})
	if s.predecessor != nil && s.predecessor.FundingTime.Equal(predecessor.FundingTime) && s.predecessor.NextFundingTime == nil {
		next := successor.FundingTime
		s.predecessor.NextFundingTime = &next
	}
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

func TestBackfillFundingHistoryReportsExactPerYearCoverageAfterAllPagesSucceed(t *testing.T) {
	start := time.Date(2026, 12, 31, 23, 0, 0, 0, time.UTC)
	boundary := time.Date(2027, 1, 1, 0, 0, 0, 0, time.UTC)
	end := boundary.Add(time.Hour)
	var calls int
	collector := &fundingMarketDataCollector{
		httpClient: httpclient.New(fundingTestDoer{do: func(req *http.Request) (*http.Response, error) {
			calls++
			var body string
			switch calls {
			case 1:
				if got := req.URL.Query().Get("startTime"); got != "1798758000000" {
					t.Fatalf("2026 startTime = %q, want %d", got, start.UnixMilli())
				}
				if got := req.URL.Query().Get("endTime"); got != "1798761600000" {
					t.Fatalf("2026 endTime = %q, want %d", got, boundary.UnixMilli())
				}
				body = `[{"symbol":"BTCUSDT","fundingRate":"0.000100000000000001","markPrice":"20000.123456789012345678","fundingTime":1798761599999}]`
			case 2:
				if got := req.URL.Query().Get("startTime"); got != "1798761600000" {
					t.Fatalf("2027 startTime = %q, want %d", got, boundary.UnixMilli())
				}
				body = `[{"symbol":"BTCUSDT","fundingRate":"-0.000200000000000002","markPrice":"20001.000000000000000001","fundingTime":1798765199999}]`
			default:
				t.Fatalf("unexpected Funding page %d", calls)
			}
			return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(body))}, nil
		}}, fundingNoopExtAPILogger{}, "funding_test"),
	}
	store := &fundingRateCaptureStore{}
	segments, err := collector.BackfillFundingHistory(context.Background(), exchange.HistoricalFundingRequest{
		Exchange: "binance", Market: "futures", Symbol: "BTCUSDT", StartAt: start, EndAt: end,
	}, store)
	if err != nil {
		t.Fatalf("BackfillFundingHistory: %v", err)
	}
	if len(segments) != 2 || segments[0].Year != 2026 || segments[0].RowCount != 1 || !segments[0].StartAt.Equal(start) || !segments[0].EndAt.Equal(boundary) ||
		segments[1].Year != 2027 || segments[1].RowCount != 1 || !segments[1].StartAt.Equal(boundary) || !segments[1].EndAt.Equal(end) {
		t.Fatalf("Funding coverage segments = %#v", segments)
	}
	if len(store.rates) != 2 {
		t.Fatalf("stored Funding rates = %d, want 2", len(store.rates))
	}
	if store.rates[0].FundingRateDecimal != "0.000100000000000001" || store.rates[0].MarkPriceDecimal != "20000.123456789012345678" ||
		store.rates[0].NextFundingTime == nil || !store.rates[0].NextFundingTime.Equal(store.rates[1].FundingTime) {
		t.Fatalf("first exact/cross-year Funding row = %#v", store.rates[0])
	}
	if store.rates[1].NextFundingTime != nil {
		t.Fatalf("final Funding row invented next time: %#v", store.rates[1])
	}
}

func TestBackfillFundingHistoryExplicitlyCoversZeroRows(t *testing.T) {
	collector := &fundingMarketDataCollector{
		httpClient: httpclient.New(fundingTestDoer{do: func(*http.Request) (*http.Response, error) {
			return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(`[]`))}, nil
		}}, fundingNoopExtAPILogger{}, "funding_test"),
	}
	start := time.Date(2026, 5, 1, 1, 2, 3, 0, time.UTC)
	end := start.Add(17 * time.Minute)
	segments, err := collector.BackfillFundingHistory(context.Background(), exchange.HistoricalFundingRequest{
		Exchange: "binance", Market: "futures", Symbol: "BTCUSDT", StartAt: start, EndAt: end,
	}, &fundingRateCaptureStore{})
	if err != nil {
		t.Fatalf("BackfillFundingHistory: %v", err)
	}
	if len(segments) != 1 || segments[0].RowCount != 0 || !segments[0].StartAt.Equal(start) || !segments[0].EndAt.Equal(end) {
		t.Fatalf("zero-row Funding coverage = %#v, want explicit requested window", segments)
	}
}

func TestBackfillFundingHistoryFailureReturnsNoCoverage(t *testing.T) {
	start := time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC)
	end := start.Add(time.Hour)
	for _, tc := range []struct {
		name            string
		httpDo          func(*http.Request) (*http.Response, error)
		storeFail       bool
		wantPartialRows int
	}{
		{name: "partial page failure", httpDo: func() func(*http.Request) (*http.Response, error) {
			calls := 0
			return func(*http.Request) (*http.Response, error) {
				calls++
				if calls == 2 {
					return nil, errors.New("second page transport failed")
				}
				return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(`[
					{"symbol":"BTCUSDT","fundingRate":"0.1","markPrice":"100","fundingTime":1777593600000},
					{"symbol":"BTCUSDT","fundingRate":"0.2","markPrice":"101","fundingTime":1777593601000}
				]`))}, nil
			}
		}(), wantPartialRows: 1},
		{name: "storage failure", httpDo: func(*http.Request) (*http.Response, error) {
			return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(`[{"symbol":"BTCUSDT","fundingRate":"0.1","markPrice":"100","fundingTime":1777597199999}]`))}, nil
		}, storeFail: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			collector := &fundingMarketDataCollector{httpClient: httpclient.New(fundingTestDoer{do: tc.httpDo}, fundingNoopExtAPILogger{}, "funding_test")}
			store := &fundingRateCaptureStore{}
			if tc.storeFail {
				store.insertErr = errors.New("storage failed")
			}
			segments, err := collector.BackfillFundingHistory(context.Background(), exchange.HistoricalFundingRequest{
				Exchange: "binance", Market: "futures", Symbol: "BTCUSDT", StartAt: start, EndAt: end,
			}, store)
			if err == nil || len(segments) != 0 {
				t.Fatalf("BackfillFundingHistory = segments %#v err %v, want error and no coverage", segments, err)
			}
			if len(store.rates) != tc.wantPartialRows {
				t.Fatalf("partially stored Funding rows = %d, want %d while still reporting no coverage", len(store.rates), tc.wantPartialRows)
			}
		})
	}
}

func TestBackfillFundingHistoryOverlapEnrichesStoredRowFromObservedPair(t *testing.T) {
	collector := &fundingMarketDataCollector{httpClient: httpclient.New(fundingTestDoer{do: func(*http.Request) (*http.Response, error) {
		return fundingHTTPResponse(`[
			{"symbol":"BTCUSDT","fundingRate":"0.1","markPrice":"100","fundingTime":1000},
			{"symbol":"BTCUSDT","fundingRate":"0.2","markPrice":"101","fundingTime":2000}
		]`), nil
	}}, fundingNoopExtAPILogger{}, "funding_test")}
	store := &fundingRateCaptureStore{}
	segments, err := collector.BackfillFundingHistory(context.Background(), fundingHistoryRequest(1000, 2001), store)
	if err != nil || len(segments) != 1 {
		t.Fatalf("overlap Funding backfill = segments %#v err %v", segments, err)
	}
	if len(store.rates) != 2 || store.rates[0].FundingTime.UnixMilli() != 1000 || store.rates[0].NextFundingTime == nil || store.rates[0].NextFundingTime.UnixMilli() != 2000 {
		t.Fatalf("overlap Funding facts = %#v, want observed 1000 -> 2000 pair", store.rates)
	}
}

func TestBackfillFundingHistoryLinksAdjacentRequestOnlyAfterBinanceProofAndIsIdempotent(t *testing.T) {
	requests := make([]string, 0, 3)
	collector := &fundingMarketDataCollector{httpClient: httpclient.New(fundingTestDoer{do: func(req *http.Request) (*http.Response, error) {
		start := req.URL.Query().Get("startTime")
		requests = append(requests, start)
		switch start {
		case "2000":
			return fundingHTTPResponse(`[{"symbol":"BTCUSDT","fundingRate":"0.2","markPrice":"101","fundingTime":2000}]`), nil
		case "1000":
			return fundingHTTPResponse(`[
				{"symbol":"BTCUSDT","fundingRate":"0.1","markPrice":"100","fundingTime":1000},
				{"symbol":"BTCUSDT","fundingRate":"0.2","markPrice":"101","fundingTime":2000}
			]`), nil
		default:
			t.Fatalf("unexpected Funding request startTime=%s", start)
			return nil, nil
		}
	}}, fundingNoopExtAPILogger{}, "funding_test")}
	store := &fundingRateCaptureStore{predecessor: fundingFact(1000, "0.1", "100")}
	for attempt := 0; attempt < 2; attempt++ {
		segments, err := collector.BackfillFundingHistory(context.Background(), fundingHistoryRequest(2000, 2001), store)
		if err != nil || len(segments) != 1 {
			t.Fatalf("adjacent Funding attempt %d = segments %#v err %v", attempt+1, segments, err)
		}
	}
	if got, want := requests, []string{"2000", "1000", "2000"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("adjacency proof requests = %v, want %v", got, want)
	}
	if len(store.adjacencyLinks) != 1 {
		t.Fatalf("exact adjacency links = %#v, want one idempotent link", store.adjacencyLinks)
	}
	link := store.adjacencyLinks[0]
	if link.predecessor.FundingTime.UnixMilli() != 1000 || link.successor.FundingTime.UnixMilli() != 2000 ||
		link.predecessor.Symbol != "BTCUSDT" || link.successor.Symbol != "BTCUSDT" ||
		link.predecessor.Exchange != models.ExchangeBinance || link.successor.Exchange != models.ExchangeBinance ||
		link.predecessor.Market != models.MarketFutures || link.successor.Market != models.MarketFutures {
		t.Fatalf("exact adjacency identity = %#v", link)
	}
}

func TestBackfillFundingHistoryProofComparesPaddedDecimalsByExactValue(t *testing.T) {
	collector := &fundingMarketDataCollector{httpClient: httpclient.New(fundingTestDoer{do: func(req *http.Request) (*http.Response, error) {
		if req.URL.Query().Get("startTime") == "1000" {
			return fundingHTTPResponse(`[
				{"symbol":"BTCUSDT","fundingRate":"0.1","markPrice":"100","fundingTime":1000},
				{"symbol":"BTCUSDT","fundingRate":"0.2","markPrice":"101","fundingTime":2000}
			]`), nil
		}
		return fundingHTTPResponse(`[{"symbol":"BTCUSDT","fundingRate":"0.2","markPrice":"101","fundingTime":2000}]`), nil
	}}, fundingNoopExtAPILogger{}, "funding_test")}
	store := &fundingRateCaptureStore{predecessor: fundingFact(1000, "0.100000000000000000", "100.000000000000000000")}
	segments, err := collector.BackfillFundingHistory(context.Background(), fundingHistoryRequest(2000, 2001), store)
	if err != nil || len(segments) != 1 {
		t.Fatalf("padded-decimal Funding proof = segments %#v err %v", segments, err)
	}
	if len(store.adjacencyLinks) != 1 {
		t.Fatalf("padded database decimals rejected an exact Binance fact: %#v", store.adjacencyLinks)
	}
}

func TestBackfillFundingHistoryProofRejectsMalformedDecimals(t *testing.T) {
	collector := &fundingMarketDataCollector{httpClient: httpclient.New(fundingTestDoer{do: func(req *http.Request) (*http.Response, error) {
		if req.URL.Query().Get("startTime") == "1000" {
			return fundingHTTPResponse(`[
				{"symbol":"BTCUSDT","fundingRate":"not-a-decimal","markPrice":"also-bad","fundingTime":1000},
				{"symbol":"BTCUSDT","fundingRate":"0.2","markPrice":"101","fundingTime":2000}
			]`), nil
		}
		return fundingHTTPResponse(`[{"symbol":"BTCUSDT","fundingRate":"0.2","markPrice":"101","fundingTime":2000}]`), nil
	}}, fundingNoopExtAPILogger{}, "funding_test")}
	store := &fundingRateCaptureStore{predecessor: fundingFact(1000, "not-a-decimal", "also-bad")}
	segments, err := collector.BackfillFundingHistory(context.Background(), fundingHistoryRequest(2000, 2001), store)
	if err == nil || !strings.Contains(err.Error(), "invalid Funding rate decimal") || len(segments) != 0 {
		t.Fatalf("malformed-decimal Funding proof = segments %#v err %v, want fail-closed", segments, err)
	}
	if len(store.adjacencyLinks) != 0 {
		t.Fatalf("malformed Funding decimals proved adjacency: %#v", store.adjacencyLinks)
	}
}

func TestBackfillFundingHistoryDoesNotLinkUnprovenCandidate(t *testing.T) {
	for _, tc := range []struct {
		name      string
		candidate *models.FundingRate
		proofBody string
	}{
		{
			name:      "intervening exchange fact",
			candidate: fundingFact(1000, "0.1", "100"),
			proofBody: `[
				{"symbol":"BTCUSDT","fundingRate":"0.1","markPrice":"100","fundingTime":1000},
				{"symbol":"BTCUSDT","fundingRate":"0.3","markPrice":"103","fundingTime":3000},
				{"symbol":"BTCUSDT","fundingRate":"0.5","markPrice":"105","fundingTime":5000}
			]`,
		},
		{
			name:      "stored candidate facts mismatch exchange proof",
			candidate: fundingFact(1000, "0.1", "100"),
			proofBody: `[
				{"symbol":"BTCUSDT","fundingRate":"9.9","markPrice":"999","fundingTime":1000},
				{"symbol":"BTCUSDT","fundingRate":"0.2","markPrice":"101","fundingTime":2000}
			]`,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			successorMS := int64(5000)
			if tc.name == "stored candidate facts mismatch exchange proof" {
				successorMS = 2000
			}
			collector := &fundingMarketDataCollector{httpClient: httpclient.New(fundingTestDoer{do: func(req *http.Request) (*http.Response, error) {
				if req.URL.Query().Get("startTime") == "1000" {
					return fundingHTTPResponse(tc.proofBody), nil
				}
				return fundingHTTPResponse(fmt.Sprintf(`[{"symbol":"BTCUSDT","fundingRate":"0.2","markPrice":"101","fundingTime":%d}]`, successorMS)), nil
			}}, fundingNoopExtAPILogger{}, "funding_test")}
			store := &fundingRateCaptureStore{predecessor: tc.candidate}
			segments, err := collector.BackfillFundingHistory(context.Background(), fundingHistoryRequest(successorMS, successorMS+1), store)
			if err != nil || len(segments) != 1 {
				t.Fatalf("unproven Funding candidate = segments %#v err %v", segments, err)
			}
			if len(store.adjacencyLinks) != 0 {
				t.Fatalf("unproven Funding candidate was linked: %#v", store.adjacencyLinks)
			}
		})
	}
}

func TestBackfillFundingHistoryPreservesKnownImmutableSuccessor(t *testing.T) {
	candidate := fundingFact(1000, "0.1", "100")
	known := time.UnixMilli(3000).UTC()
	candidate.NextFundingTime = &known
	collector := &fundingMarketDataCollector{httpClient: httpclient.New(fundingTestDoer{do: func(req *http.Request) (*http.Response, error) {
		if req.URL.Query().Get("startTime") != "2000" {
			t.Fatalf("known successor triggered an unnecessary proof query: %s", req.URL.RawQuery)
		}
		return fundingHTTPResponse(`[{"symbol":"BTCUSDT","fundingRate":"0.2","markPrice":"101","fundingTime":2000}]`), nil
	}}, fundingNoopExtAPILogger{}, "funding_test")}
	store := &fundingRateCaptureStore{predecessor: candidate}
	segments, err := collector.BackfillFundingHistory(context.Background(), fundingHistoryRequest(2000, 2001), store)
	if err != nil || len(segments) != 1 {
		t.Fatalf("known-successor Funding backfill = segments %#v err %v", segments, err)
	}
	if len(store.adjacencyLinks) != 0 || store.predecessor.NextFundingTime == nil || store.predecessor.NextFundingTime.UnixMilli() != 3000 {
		t.Fatalf("known Funding successor changed: candidate=%#v links=%#v", store.predecessor, store.adjacencyLinks)
	}
}

func TestBackfillFundingHistoryAdjacencyProofFailuresReportNoCoverage(t *testing.T) {
	for _, tc := range []struct {
		name           string
		predecessorErr error
		adjacencyErr   error
		proofErr       error
		want           string
	}{
		{name: "candidate query", predecessorErr: errors.New("candidate query failed"), want: "candidate query failed"},
		{name: "Binance proof API", proofErr: errors.New("proof transport failed"), want: "proof transport failed"},
		{name: "exact link", adjacencyErr: errors.New("exact link failed"), want: "exact link failed"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			collector := &fundingMarketDataCollector{httpClient: httpclient.New(fundingTestDoer{do: func(req *http.Request) (*http.Response, error) {
				if req.URL.Query().Get("startTime") == "1000" && tc.proofErr != nil {
					return nil, tc.proofErr
				}
				if req.URL.Query().Get("startTime") == "1000" {
					return fundingHTTPResponse(`[
						{"symbol":"BTCUSDT","fundingRate":"0.1","markPrice":"100","fundingTime":1000},
						{"symbol":"BTCUSDT","fundingRate":"0.2","markPrice":"101","fundingTime":2000}
					]`), nil
				}
				return fundingHTTPResponse(`[{"symbol":"BTCUSDT","fundingRate":"0.2","markPrice":"101","fundingTime":2000}]`), nil
			}}, fundingNoopExtAPILogger{}, "funding_test")}
			store := &fundingRateCaptureStore{
				predecessor: fundingFact(1000, "0.1", "100"), predecessorErr: tc.predecessorErr, adjacencyErr: tc.adjacencyErr,
			}
			segments, err := collector.BackfillFundingHistory(context.Background(), fundingHistoryRequest(2000, 2001), store)
			if err == nil || !strings.Contains(err.Error(), tc.want) || len(segments) != 0 {
				t.Fatalf("adjacency %s failure = segments %#v err %v, want no coverage and %q", tc.name, segments, err, tc.want)
			}
		})
	}
}

func TestBackfillFundingHistoryDoesNotLinkLatestStoredRowAcrossDisjointGap(t *testing.T) {
	collector := &fundingMarketDataCollector{httpClient: httpclient.New(fundingTestDoer{do: func(req *http.Request) (*http.Response, error) {
		if req.URL.Query().Get("startTime") == "1000" {
			return fundingHTTPResponse(`[
				{"symbol":"BTCUSDT","fundingRate":"0.1","markPrice":"100","fundingTime":1000},
				{"symbol":"BTCUSDT","fundingRate":"0.3","markPrice":"103","fundingTime":3000},
				{"symbol":"BTCUSDT","fundingRate":"0.5","markPrice":"105","fundingTime":5000}
			]`), nil
		}
		return fundingHTTPResponse(`[{"symbol":"BTCUSDT","fundingRate":"0.5","markPrice":"105","fundingTime":5000}]`), nil
	}}, fundingNoopExtAPILogger{}, "funding_test")}
	store := &fundingRateCaptureStore{predecessor: fundingFact(1000, "0.1", "100")}
	segments, err := collector.BackfillFundingHistory(context.Background(), exchange.HistoricalFundingRequest{
		Exchange: "binance", Market: "futures", Symbol: "BTCUSDT",
		StartAt: time.UnixMilli(5000).UTC(), EndAt: time.UnixMilli(5001).UTC(),
	}, store)
	if err != nil || len(segments) != 1 {
		t.Fatalf("disjoint Funding backfill = segments %#v err %v", segments, err)
	}
	if len(store.adjacencyLinks) != 0 {
		t.Fatalf("disjoint Funding backfill linked an unproven predecessor: %#v", store.adjacencyLinks)
	}
}

func fundingHistoryRequest(startMS, endMS int64) exchange.HistoricalFundingRequest {
	return exchange.HistoricalFundingRequest{
		Exchange: "binance", Market: "futures", Symbol: "BTCUSDT",
		StartAt: time.UnixMilli(startMS).UTC(), EndAt: time.UnixMilli(endMS).UTC(),
	}
}

func fundingFact(atMS int64, rate, mark string) *models.FundingRate {
	return &models.FundingRate{
		Exchange: models.ExchangeBinance, Market: models.MarketFutures, Symbol: "BTCUSDT",
		FundingTime: time.UnixMilli(atMS).UTC(), FundingRateDecimal: rate, MarkPriceDecimal: mark,
	}
}

func fundingHTTPResponse(body string) *http.Response {
	return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(body))}
}
