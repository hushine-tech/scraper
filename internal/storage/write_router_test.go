package storage

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/hushine-tech/scraper/internal/models"
	"github.com/lib/pq"
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

func TestMarketDataWriteRouterFindsFundingPredecessorInSuccessorYearThenPriorYear(t *testing.T) {
	for _, tc := range []struct {
		name           string
		successor      time.Time
		sameCandidate  *models.FundingRate
		priorCandidate *models.FundingRate
		want           time.Time
	}{
		{
			name:          "same successor year",
			successor:     time.Date(2027, 2, 1, 0, 0, 0, 0, time.UTC),
			sameCandidate: &models.FundingRate{Exchange: "binance", Market: "futures", Symbol: "BTCUSDT", FundingTime: time.Date(2027, 1, 31, 23, 0, 0, 0, time.UTC)},
			want:          time.Date(2027, 1, 31, 23, 0, 0, 0, time.UTC),
		},
		{
			name:           "prior year boundary",
			successor:      time.Date(2027, 1, 1, 0, 0, 0, 0, time.UTC),
			priorCandidate: &models.FundingRate{Exchange: "binance", Market: "futures", Symbol: "BTCUSDT", FundingTime: time.Date(2026, 12, 31, 23, 0, 0, 0, time.UTC)},
			want:           time.Date(2026, 12, 31, 23, 0, 0, 0, time.UTC),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			factory := newCaptureFactory()
			same, _ := factory.open(context.Background(), "binance", 2027)
			same.(*captureStore).predecessor = tc.sameCandidate
			prior, _ := factory.open(context.Background(), "binance", 2026)
			prior.(*captureStore).predecessor = tc.priorCandidate
			router := NewMarketDataWriteRouter(factory.open)
			successor := models.FundingRate{
				Exchange: "binance", Market: "futures", Symbol: "BTCUSDT",
				FundingTime: tc.successor,
			}
			candidate, err := router.FindFundingRatePredecessor(context.Background(), successor)
			if err != nil {
				t.Fatalf("FindFundingRatePredecessor: %v", err)
			}
			if candidate == nil || !candidate.FundingTime.Equal(tc.want) {
				t.Fatalf("Funding predecessor = %#v, want time %s", candidate, tc.want)
			}
			if got := len(same.(*captureStore).predecessorQueries); got != 1 {
				t.Fatalf("same-year predecessor searches = %d, want 1", got)
			}
			wantPriorSearch := tc.sameCandidate == nil
			if got := len(prior.(*captureStore).predecessorQueries); (got == 1) != wantPriorSearch {
				t.Fatalf("prior-year predecessor searches = %d, want searched=%v", got, wantPriorSearch)
			}
		})
	}
}

func TestMarketDataWriteRouterStopsPredecessorFallbackOnStorageFailure(t *testing.T) {
	factory := newCaptureFactory()
	same, _ := factory.open(context.Background(), "binance", 2027)
	same.(*captureStore).predecessorErr = errors.New("same-year predecessor query failed")
	prior, _ := factory.open(context.Background(), "binance", 2026)
	_, err := NewMarketDataWriteRouter(factory.open).FindFundingRatePredecessor(context.Background(), models.FundingRate{
		Exchange: "binance", Market: "futures", Symbol: "BTCUSDT", FundingTime: time.Date(2027, 1, 1, 0, 0, 0, 0, time.UTC),
	})
	if err == nil || !strings.Contains(err.Error(), "same-year predecessor query failed") {
		t.Fatalf("FindFundingRatePredecessor error = %v", err)
	}
	if len(prior.(*captureStore).predecessorQueries) != 0 {
		t.Fatal("prior-year predecessor search ran after same-year storage failure")
	}
}

func TestMarketDataWriteRouterTreatsMissingPriorYearDatabaseAsNoPredecessor(t *testing.T) {
	factory := newCaptureFactory()
	factory.errs["binance_2026"] = fmt.Errorf("open prior year: %w", &pq.Error{Code: "3D000"})
	candidate, err := NewMarketDataWriteRouter(factory.open).FindFundingRatePredecessor(context.Background(), models.FundingRate{
		Exchange: "binance", Market: "futures", Symbol: "BTCUSDT", FundingTime: time.Date(2027, 1, 1, 0, 0, 0, 0, time.UTC),
	})
	if err != nil || candidate != nil {
		t.Fatalf("first-ever Funding predecessor = %#v err %v, want nil without missing-database failure", candidate, err)
	}
}

func TestMarketDataWriteRouterLinksExactFundingAdjacencyInPredecessorYear(t *testing.T) {
	factory := newCaptureFactory()
	router := NewMarketDataWriteRouter(factory.open)
	predecessor := models.FundingRate{
		Exchange: "binance", Market: "futures", Symbol: "BTCUSDT",
		FundingTime: time.Date(2026, 12, 31, 23, 0, 0, 0, time.UTC), FundingRateDecimal: "0.1", MarkPriceDecimal: "100",
	}
	successor := models.FundingRate{
		Exchange: "binance", Market: "futures", Symbol: "BTCUSDT",
		FundingTime: time.Date(2027, 1, 1, 0, 0, 0, 0, time.UTC), FundingRateDecimal: "0.2", MarkPriceDecimal: "101",
	}
	if err := router.LinkFundingRateAdjacency(context.Background(), predecessor, successor); err != nil {
		t.Fatalf("LinkFundingRateAdjacency: %v", err)
	}
	prior := factory.stores["binance_2026"]
	if prior == nil || len(prior.adjacencyLinks) != 1 || !prior.adjacencyLinks[0].predecessor.FundingTime.Equal(predecessor.FundingTime) ||
		!prior.adjacencyLinks[0].successor.FundingTime.Equal(successor.FundingTime) {
		t.Fatalf("prior-year exact Funding adjacency = %#v", prior)
	}
	if factory.stores["binance_2027"] != nil {
		t.Fatal("exact Funding adjacency was routed by successor year instead of predecessor year")
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
	errs   map[string]error
}

func newCaptureFactory() *captureFactory {
	return &captureFactory{stores: map[string]*captureStore{}, errs: map[string]error{}}
}

func (f *captureFactory) open(_ context.Context, exchange string, year int) (MarketDataStore, error) {
	key := fmt.Sprintf("%s_%d", exchange, year)
	if err := f.errs[key]; err != nil {
		return nil, err
	}
	store := f.stores[key]
	if store == nil {
		store = &captureStore{}
		f.stores[key] = store
	}
	return store, nil
}

type captureStore struct {
	klines             [][]models.Kline
	orderbooks         []models.OrderBook
	funding            []models.FundingRate
	openInterest       []models.OpenInterest
	predecessor        *models.FundingRate
	predecessorQueries []models.FundingRate
	predecessorErr     error
	adjacencyLinks     []captureFundingAdjacencyLink
	adjacencyErr       error
}

type captureFundingAdjacencyLink struct {
	predecessor models.FundingRate
	successor   models.FundingRate
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

func (s *captureStore) findFundingRatePredecessor(_ context.Context, successor models.FundingRate) (*models.FundingRate, error) {
	s.predecessorQueries = append(s.predecessorQueries, successor)
	return s.predecessor, s.predecessorErr
}

func (s *captureStore) linkFundingRateAdjacency(_ context.Context, predecessor, successor models.FundingRate) error {
	s.adjacencyLinks = append(s.adjacencyLinks, captureFundingAdjacencyLink{predecessor: predecessor, successor: successor})
	return s.adjacencyErr
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
