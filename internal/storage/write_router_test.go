package storage

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/hushine-tech/scraper/internal/models"
)

func TestDatabaseNameForYearRejectsFixedExchangeDB(t *testing.T) {
	if got, err := DatabaseNameForYear("{exchange}_{year}", "binance", 2026); err != nil || got != "binance_2026" {
		t.Fatalf("DatabaseNameForYear template = %q/%v, want binance_2026", got, err)
	}
	if _, err := DatabaseNameForYear("binance", "binance", 2026); err == nil || !strings.Contains(err.Error(), "fixed exchange database") {
		t.Fatalf("DatabaseNameForYear fixed err = %v, want fixed exchange database rejection", err)
	}
}

func TestMarketDataWriteRouterSplitsKlinesByOpenTimeYear(t *testing.T) {
	factory := newCaptureFactory()
	router := NewMarketDataWriteRouter(factory.open)
	rows := []models.Kline{
		{
			Exchange: "binance",
			Market:   "futures",
			Symbol:   "BTCUSDT",
			Interval: "1m",
			OpenTime: time.Date(2025, 12, 31, 23, 59, 0, 0, time.UTC),
		},
		{
			Exchange: "binance",
			Market:   "futures",
			Symbol:   "BTCUSDT",
			Interval: "1m",
			OpenTime: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		},
	}

	if err := router.InsertKline(context.Background(), rows); err != nil {
		t.Fatalf("InsertKline: %v", err)
	}

	if got := len(factory.stores["binance_2025"].klines); got != 1 {
		t.Fatalf("binance_2025 klines = %d, want 1", got)
	}
	if got := len(factory.stores["binance_2026"].klines); got != 1 {
		t.Fatalf("binance_2026 klines = %d, want 1", got)
	}
}

func TestMarketDataWriteRouterRoutesEventTimestampKinds(t *testing.T) {
	factory := newCaptureFactory()
	router := NewMarketDataWriteRouter(factory.open)
	ctx := context.Background()

	if err := router.InsertFundingRate(ctx, models.FundingRate{
		Exchange:    "binance",
		Market:      "futures",
		Symbol:      "BTCUSDT",
		FundingTime: time.Date(2027, 1, 1, 0, 0, 0, 0, time.UTC),
	}); err != nil {
		t.Fatalf("InsertFundingRate: %v", err)
	}
	if err := router.InsertOrderBook(ctx, models.OrderBook{
		Exchange: "okx",
		Market:   "spot",
		Symbol:   "ETHUSDT",
		Time:     time.Date(2028, 5, 1, 0, 0, 0, 0, time.UTC),
	}); err != nil {
		t.Fatalf("InsertOrderBook: %v", err)
	}
	if err := router.InsertOpenInterest(ctx, models.OpenInterest{
		Exchange: "binance",
		Market:   "futures",
		Symbol:   "BTCUSDT",
		Time:     time.Date(2029, 6, 1, 0, 0, 0, 0, time.UTC),
	}); err != nil {
		t.Fatalf("InsertOpenInterest: %v", err)
	}

	if got := len(factory.stores["binance_2027"].funding); got != 1 {
		t.Fatalf("binance_2027 funding = %d, want 1", got)
	}
	if got := len(factory.stores["okx_2028"].orderbooks); got != 1 {
		t.Fatalf("okx_2028 orderbooks = %d, want 1", got)
	}
	if got := len(factory.stores["binance_2029"].openInterest); got != 1 {
		t.Fatalf("binance_2029 open interest = %d, want 1", got)
	}
}

func TestMarketDataWriteRouterLinksFundingPredecessorInSuccessorYearThenPriorYear(t *testing.T) {
	for _, tc := range []struct {
		name            string
		sameYearFound   bool
		wantPriorSearch bool
	}{
		{name: "same successor year", sameYearFound: true},
		{name: "prior year fallback", wantPriorSearch: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			factory := newCaptureFactory()
			same, _ := factory.open(context.Background(), "binance", 2027)
			same.(*captureStore).predecessorFound = tc.sameYearFound
			prior, _ := factory.open(context.Background(), "binance", 2026)
			prior.(*captureStore).predecessorFound = tc.wantPriorSearch
			router := NewMarketDataWriteRouter(factory.open)
			successor := models.FundingRate{
				Exchange: "binance", Market: "futures", Symbol: "BTCUSDT",
				FundingTime: time.Date(2027, 1, 1, 0, 0, 0, 0, time.UTC),
			}
			if err := router.LinkFundingRatePredecessor(context.Background(), successor); err != nil {
				t.Fatalf("LinkFundingRatePredecessor: %v", err)
			}
			if got := len(same.(*captureStore).predecessorLinks); got != 1 {
				t.Fatalf("same-year predecessor searches = %d, want 1", got)
			}
			if got := len(prior.(*captureStore).predecessorLinks); (got == 1) != tc.wantPriorSearch {
				t.Fatalf("prior-year predecessor searches = %d, want searched=%v", got, tc.wantPriorSearch)
			}
		})
	}
}

func TestMarketDataWriteRouterStopsPredecessorFallbackOnStorageFailure(t *testing.T) {
	factory := newCaptureFactory()
	same, _ := factory.open(context.Background(), "binance", 2027)
	same.(*captureStore).predecessorErr = errors.New("same-year predecessor query failed")
	prior, _ := factory.open(context.Background(), "binance", 2026)
	err := NewMarketDataWriteRouter(factory.open).LinkFundingRatePredecessor(context.Background(), models.FundingRate{
		Exchange: "binance", Market: "futures", Symbol: "BTCUSDT", FundingTime: time.Date(2027, 1, 1, 0, 0, 0, 0, time.UTC),
	})
	if err == nil || !strings.Contains(err.Error(), "same-year predecessor query failed") {
		t.Fatalf("LinkFundingRatePredecessor error = %v", err)
	}
	if len(prior.(*captureStore).predecessorLinks) != 0 {
		t.Fatal("prior-year predecessor search ran after same-year storage failure")
	}
}

func TestMarketDataWriteRouterAcquiresLeaseBeforeWrite(t *testing.T) {
	factory := newCaptureFactory()
	leases := &captureLeaseManager{}
	router := NewMarketDataWriteRouter(factory.open)
	router.SetWriterLeaseManager(leases)
	ctx := context.Background()

	if err := router.InsertFundingRate(ctx, models.FundingRate{
		Exchange:    "binance",
		Market:      "futures",
		Symbol:      "BTCUSDT",
		FundingTime: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
	}); err != nil {
		t.Fatalf("InsertFundingRate: %v", err)
	}

	if len(leases.domains) != 1 {
		t.Fatalf("lease domains = %d, want 1", len(leases.domains))
	}
	got := leases.domains[0]
	if got.Exchange != "binance" || got.Market != "futures" || got.Kind != "funding_rate" || got.Symbol != "BTCUSDT" || got.Year != 2026 {
		t.Fatalf("lease domain = %+v", got)
	}
	if leases.collectorIDs[0] != "binance:futures:funding_rate:BTCUSDT:-:2026" {
		t.Fatalf("collector_id = %q", leases.collectorIDs[0])
	}
}

func TestMarketDataWriteRouterStopsWriteWhenLeaseDenied(t *testing.T) {
	factory := newCaptureFactory()
	router := NewMarketDataWriteRouter(factory.open)
	router.SetWriterLeaseManager(&captureLeaseManager{err: errors.New("lease denied")})

	err := router.InsertOrderBook(context.Background(), models.OrderBook{
		Exchange: "binance",
		Market:   "spot",
		Symbol:   "ETHUSDT",
		Time:     time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC),
	})
	if err == nil || !strings.Contains(err.Error(), "lease denied") {
		t.Fatalf("InsertOrderBook err = %v, want lease denied", err)
	}
	if len(factory.stores) != 0 {
		t.Fatalf("stores opened after denied lease = %d, want 0", len(factory.stores))
	}
}

type captureFactory struct {
	stores map[string]*captureStore
}

func newCaptureFactory() *captureFactory {
	return &captureFactory{stores: map[string]*captureStore{}}
}

func (f *captureFactory) open(_ context.Context, exchange string, year int) (MarketDataStore, error) {
	key := fmt.Sprintf("%s_%d", exchange, year)
	store := f.stores[key]
	if store == nil {
		store = &captureStore{}
		f.stores[key] = store
	}
	return store, nil
}

type captureStore struct {
	klines           [][]models.Kline
	orderbooks       []models.OrderBook
	funding          []models.FundingRate
	openInterest     []models.OpenInterest
	predecessorLinks []models.FundingRate
	predecessorFound bool
	predecessorErr   error
}

func (s *captureStore) InsertKline(_ context.Context, klines []models.Kline) error {
	s.klines = append(s.klines, append([]models.Kline(nil), klines...))
	return nil
}

func (s *captureStore) InsertOrderBook(_ context.Context, ob models.OrderBook) error {
	s.orderbooks = append(s.orderbooks, ob)
	return nil
}

func (s *captureStore) InsertFundingRate(_ context.Context, fr models.FundingRate) error {
	s.funding = append(s.funding, fr)
	return nil
}

func (s *captureStore) linkFundingRatePredecessor(_ context.Context, successor models.FundingRate) (bool, error) {
	s.predecessorLinks = append(s.predecessorLinks, successor)
	return s.predecessorFound, s.predecessorErr
}

func (s *captureStore) InsertOpenInterest(_ context.Context, oi models.OpenInterest) error {
	s.openInterest = append(s.openInterest, oi)
	return nil
}

func (s *captureStore) InsertOpenInterests(_ context.Context, items []models.OpenInterest) error {
	s.openInterest = append(s.openInterest, items...)
	return nil
}

type captureLeaseManager struct {
	domains      []WriterLeaseDomain
	collectorIDs []string
	err          error
}

func (m *captureLeaseManager) Acquire(_ context.Context, domain WriterLeaseDomain, collectorID string) (WriterLease, error) {
	if m.err != nil {
		return WriterLease{}, m.err
	}
	m.domains = append(m.domains, domain)
	m.collectorIDs = append(m.collectorIDs, collectorID)
	return WriterLease{
		LeaseID:           "lease-" + collectorID,
		ScraperInstanceID: "scraper-1",
		CollectorID:       collectorID,
		Status:            "active",
		ExpiresAt:         time.Now().Add(time.Minute),
	}, nil
}
