package storage

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/hushine-tech/scraper/internal/logger"
	"github.com/hushine-tech/scraper/internal/models"
)

type MarketDataStore interface {
	InsertKline(ctx context.Context, klines []models.Kline) error
	InsertOrderBook(ctx context.Context, ob models.OrderBook) error
	InsertFundingRate(ctx context.Context, fr models.FundingRate) error
	InsertOpenInterest(ctx context.Context, oi models.OpenInterest) error
	InsertOpenInterests(ctx context.Context, items []models.OpenInterest) error
}

type YearStoreFactory func(ctx context.Context, exchange string, year int) (MarketDataStore, error)

type WriterLeaseDomain struct {
	Exchange string
	Market   string
	Kind     string
	Symbol   string
	Interval string
	Year     int
}

type WriterLease struct {
	LeaseID           string
	OwnerInstanceID   string
	ScraperInstanceID string
	CollectorID       string
	Status            string
	ExpiresAt         time.Time
}

type WriterLeaseManager interface {
	Acquire(ctx context.Context, domain WriterLeaseDomain, collectorID string) (WriterLease, error)
}

type MarketDataWriteRouter struct {
	mu           sync.Mutex
	factory      YearStoreFactory
	stores       map[string]MarketDataStore
	leaseManager WriterLeaseManager
}

func NewRoutedTimescaleDB(factory YearStoreFactory) *TimescaleDB {
	return &TimescaleDB{
		writeRouter: NewMarketDataWriteRouter(factory),
	}
}

func NewMarketDataWriteRouter(factory YearStoreFactory) *MarketDataWriteRouter {
	return &MarketDataWriteRouter{
		factory: factory,
		stores:  map[string]MarketDataStore{},
	}
}

func (ts *TimescaleDB) SetWriterLeaseManager(manager WriterLeaseManager) {
	if ts == nil || ts.writeRouter == nil {
		return
	}
	ts.writeRouter.SetWriterLeaseManager(manager)
}

func (r *MarketDataWriteRouter) SetWriterLeaseManager(manager WriterLeaseManager) {
	if r == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.leaseManager = manager
}

func DatabaseNameForYear(template, exchange string, year int) (string, error) {
	exchange = strings.ToLower(strings.TrimSpace(exchange))
	template = strings.TrimSpace(template)
	if exchange == "" {
		return "", fmt.Errorf("exchange is required")
	}
	if year < 1970 {
		return "", fmt.Errorf("year %d is invalid", year)
	}
	if template == "" {
		return fmt.Sprintf("%s_%d", exchange, year), nil
	}
	if template == exchange {
		return "", fmt.Errorf("fixed exchange database %q is not a supported write target", template)
	}
	if strings.Contains(template, "{exchange}") {
		template = strings.ReplaceAll(template, "{exchange}", exchange)
	}
	if strings.Contains(template, "{year}") {
		return strings.ReplaceAll(template, "{year}", fmt.Sprintf("%d", year)), nil
	}
	return fmt.Sprintf("%s_%d", template, year), nil
}

func (r *MarketDataWriteRouter) Close() error {
	if r == nil {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	var firstErr error
	for key, store := range r.stores {
		if closer, ok := store.(interface{ Close() error }); ok {
			if err := closer.Close(); err != nil && firstErr == nil {
				firstErr = fmt.Errorf("close %s: %w", key, err)
			}
		}
		delete(r.stores, key)
	}
	return firstErr
}

func (r *MarketDataWriteRouter) InsertKline(ctx context.Context, klines []models.Kline) error {
	groups := map[WriterLeaseDomain][]models.Kline{}
	for _, k := range klines {
		ts := k.OpenTime
		if ts.IsZero() {
			ts = k.Time
		}
		domain := domainForKline(k, ts)
		groups[domain] = append(groups[domain], k)
	}
	for domain, batch := range groups {
		lease, err := r.acquireLease(ctx, domain)
		if err != nil {
			return err
		}
		store, err := r.storeFor(ctx, domain.routeKey())
		if err != nil {
			return err
		}
		if err := store.InsertKline(ctx, batch); err != nil {
			return err
		}
		logWrite(domain, lease, len(batch))
	}
	return nil
}

func (r *MarketDataWriteRouter) InsertOrderBook(ctx context.Context, ob models.OrderBook) error {
	domain := domainForOrderBook(ob)
	lease, err := r.acquireLease(ctx, domain)
	if err != nil {
		return err
	}
	store, err := r.storeFor(ctx, domain.routeKey())
	if err != nil {
		return err
	}
	if err := store.InsertOrderBook(ctx, ob); err != nil {
		return err
	}
	logWrite(domain, lease, 1)
	return nil
}

func (r *MarketDataWriteRouter) InsertFundingRate(ctx context.Context, fr models.FundingRate) error {
	domain := domainForFundingRate(fr)
	lease, err := r.acquireLease(ctx, domain)
	if err != nil {
		return err
	}
	store, err := r.storeFor(ctx, domain.routeKey())
	if err != nil {
		return err
	}
	if err := store.InsertFundingRate(ctx, fr); err != nil {
		return err
	}
	logWrite(domain, lease, 1)
	return nil
}

func (r *MarketDataWriteRouter) InsertOpenInterest(ctx context.Context, oi models.OpenInterest) error {
	domain := domainForOpenInterest(oi)
	lease, err := r.acquireLease(ctx, domain)
	if err != nil {
		return err
	}
	store, err := r.storeFor(ctx, domain.routeKey())
	if err != nil {
		return err
	}
	if err := store.InsertOpenInterest(ctx, oi); err != nil {
		return err
	}
	logWrite(domain, lease, 1)
	return nil
}

func (r *MarketDataWriteRouter) InsertOpenInterests(ctx context.Context, items []models.OpenInterest) error {
	groups := map[WriterLeaseDomain][]models.OpenInterest{}
	for _, item := range items {
		domain := domainForOpenInterest(item)
		groups[domain] = append(groups[domain], item)
	}
	for domain, batch := range groups {
		lease, err := r.acquireLease(ctx, domain)
		if err != nil {
			return err
		}
		store, err := r.storeFor(ctx, domain.routeKey())
		if err != nil {
			return err
		}
		if err := store.InsertOpenInterests(ctx, batch); err != nil {
			return err
		}
		logWrite(domain, lease, len(batch))
	}
	return nil
}

func (r *MarketDataWriteRouter) acquireLease(ctx context.Context, domain WriterLeaseDomain) (WriterLease, error) {
	domain = normalizeWriterLeaseDomain(domain)
	manager := r.writerLeaseManager()
	if manager == nil {
		return WriterLease{}, nil
	}
	collectorID := domain.CollectorID()
	lease, err := manager.Acquire(ctx, domain, collectorID)
	if err != nil {
		return WriterLease{}, fmt.Errorf("acquire writer lease %s: %w", collectorID, err)
	}
	return lease, nil
}

func (r *MarketDataWriteRouter) writerLeaseManager() WriterLeaseManager {
	if r == nil {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.leaseManager
}

func (r *MarketDataWriteRouter) storeFor(ctx context.Context, key routeKey) (MarketDataStore, error) {
	if r == nil || r.factory == nil {
		return nil, fmt.Errorf("market-data write router factory is not configured")
	}
	if key.exchange == "" {
		return nil, fmt.Errorf("exchange is required for market-data write routing")
	}
	r.mu.Lock()
	if store := r.stores[key.String()]; store != nil {
		r.mu.Unlock()
		return store, nil
	}
	r.mu.Unlock()

	store, err := r.factory(ctx, key.exchange, key.year)
	if err != nil {
		return nil, err
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	if existing := r.stores[key.String()]; existing != nil {
		if closer, ok := store.(interface{ Close() error }); ok {
			_ = closer.Close()
		}
		return existing, nil
	}
	r.stores[key.String()] = store
	return store, nil
}

type routeKey struct {
	exchange string
	year     int
}

func routeKeyFor(exchange string, ts time.Time) routeKey {
	if ts.IsZero() {
		ts = time.Now().UTC()
	}
	return routeKey{
		exchange: strings.ToLower(strings.TrimSpace(exchange)),
		year:     ts.UTC().Year(),
	}
}

func (k routeKey) String() string {
	return fmt.Sprintf("%s_%d", k.exchange, k.year)
}

func domainForKline(k models.Kline, ts time.Time) WriterLeaseDomain {
	return normalizeWriterLeaseDomain(WriterLeaseDomain{
		Exchange: k.Exchange,
		Market:   k.Market,
		Kind:     "kline",
		Symbol:   k.Symbol,
		Interval: k.Interval,
		Year:     yearFrom(ts),
	})
}

func domainForOrderBook(ob models.OrderBook) WriterLeaseDomain {
	return normalizeWriterLeaseDomain(WriterLeaseDomain{
		Exchange: ob.Exchange,
		Market:   ob.Market,
		Kind:     "orderbook",
		Symbol:   ob.Symbol,
		Year:     yearFrom(ob.Time),
	})
}

func domainForFundingRate(fr models.FundingRate) WriterLeaseDomain {
	return normalizeWriterLeaseDomain(WriterLeaseDomain{
		Exchange: string(fr.Exchange),
		Market:   string(fr.Market),
		Kind:     "funding_rate",
		Symbol:   fr.Symbol,
		Year:     yearFrom(fr.FundingTime),
	})
}

func domainForOpenInterest(oi models.OpenInterest) WriterLeaseDomain {
	return normalizeWriterLeaseDomain(WriterLeaseDomain{
		Exchange: oi.Exchange,
		Market:   oi.Market,
		Kind:     "open_interest",
		Symbol:   oi.Symbol,
		Year:     yearFrom(oi.Time),
	})
}

func normalizeWriterLeaseDomain(domain WriterLeaseDomain) WriterLeaseDomain {
	domain.Exchange = strings.ToLower(strings.TrimSpace(domain.Exchange))
	domain.Market = strings.ToLower(strings.TrimSpace(domain.Market))
	domain.Kind = strings.ToLower(strings.TrimSpace(domain.Kind))
	domain.Symbol = strings.ToUpper(strings.TrimSpace(domain.Symbol))
	domain.Interval = strings.TrimSpace(domain.Interval)
	if domain.Year < 1970 {
		domain.Year = time.Now().UTC().Year()
	}
	return domain
}

func yearFrom(ts time.Time) int {
	if ts.IsZero() {
		ts = time.Now().UTC()
	}
	return ts.UTC().Year()
}

func (d WriterLeaseDomain) CollectorID() string {
	interval := strings.TrimSpace(d.Interval)
	if interval == "" {
		interval = "-"
	}
	return strings.Join([]string{
		strings.ToLower(strings.TrimSpace(d.Exchange)),
		strings.ToLower(strings.TrimSpace(d.Market)),
		strings.ToLower(strings.TrimSpace(d.Kind)),
		strings.ToUpper(strings.TrimSpace(d.Symbol)),
		interval,
		fmt.Sprintf("%d", d.Year),
	}, ":")
}

func (d WriterLeaseDomain) routeKey() routeKey {
	return routeKey{
		exchange: strings.ToLower(strings.TrimSpace(d.Exchange)),
		year:     d.Year,
	}
}

func logWrite(domain WriterLeaseDomain, lease WriterLease, rows int) {
	fields := map[string]any{
		"exchange":            domain.Exchange,
		"market":              domain.Market,
		"kind":                domain.Kind,
		"symbol":              domain.Symbol,
		"interval":            domain.Interval,
		"year":                domain.Year,
		"rows":                rows,
		"lease_id":            lease.LeaseID,
		"scraper_instance_id": lease.ScraperInstanceID,
		"collector_id":        lease.CollectorID,
	}
	logger.NewScraperLifecycleHelper().Event("INFO", "market_data_write_committed", fields)
}
