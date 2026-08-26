package controlplane

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/hushine-tech/scraper/internal/backfill"
	"github.com/hushine-tech/scraper/internal/config"
	"github.com/hushine-tech/scraper/internal/exchange"
	"github.com/hushine-tech/scraper/internal/logger"
)

var (
	verifyKlineCoverage = backfill.VerifyKlineCoverage
	runKlineBackfill    = backfill.RunKlineRequest
)

type HistoricalRuntimeConfig struct {
	Client            Client
	Registry          *exchange.Registry
	Databases         map[string]config.DatabaseConfig
	FundingStores     map[string]exchange.HistoricalFundingStore
	MigrationsDir     string
	ReconcileInterval time.Duration
}

type HistoricalRuntime struct {
	client            Client
	registry          *exchange.Registry
	databases         map[string]config.DatabaseConfig
	fundingStores     map[string]exchange.HistoricalFundingStore
	migrationsDir     string
	reconcileInterval time.Duration

	mu        sync.Mutex
	inFlight  map[int64]context.CancelFunc
	runCancel context.CancelFunc
}

func NewHistoricalRuntime(cfg HistoricalRuntimeConfig) *HistoricalRuntime {
	dbs := make(map[string]config.DatabaseConfig, len(cfg.Databases))
	for exchange, db := range cfg.Databases {
		normalized := strings.ToLower(strings.TrimSpace(exchange))
		if normalized == "" {
			continue
		}
		dbs[normalized] = db
	}
	fundingStores := make(map[string]exchange.HistoricalFundingStore, len(cfg.FundingStores))
	for name, store := range cfg.FundingStores {
		name = strings.ToLower(strings.TrimSpace(name))
		if name != "" && store != nil {
			fundingStores[name] = store
		}
	}
	reconcileInterval := cfg.ReconcileInterval
	if reconcileInterval <= 0 {
		reconcileInterval = 5 * time.Second
	}
	return &HistoricalRuntime{
		client:            cfg.Client,
		registry:          cfg.Registry,
		databases:         dbs,
		fundingStores:     fundingStores,
		migrationsDir:     cfg.MigrationsDir,
		reconcileInterval: reconcileInterval,
		inFlight:          make(map[int64]context.CancelFunc),
	}
}

func (r *HistoricalRuntime) Run(ctx context.Context) {
	if r == nil || r.client == nil {
		return
	}
	runCtx, cancel := context.WithCancel(ctx)
	r.mu.Lock()
	r.runCancel = cancel
	r.mu.Unlock()
	defer cancel()

	if err := r.ReconcileOnce(runCtx); err != nil {
		logger.NewSystemHelper().Error("market_data_history_reconcile_failed", map[string]any{
			"error": err.Error(),
		})
	}

	ticker := time.NewTicker(r.reconcileInterval)
	defer ticker.Stop()
	for {
		select {
		case <-runCtx.Done():
			r.stopAll()
			return
		case <-ticker.C:
			if err := r.ReconcileOnce(runCtx); err != nil {
				logger.NewSystemHelper().Error("market_data_history_reconcile_failed", map[string]any{
					"error": err.Error(),
				})
			}
		}
	}
}

func (r *HistoricalRuntime) Stop() {
	if r == nil {
		return
	}
	r.mu.Lock()
	cancel := r.runCancel
	r.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	r.stopAll()
}

func (r *HistoricalRuntime) ReconcileOnce(ctx context.Context) error {
	requests, err := r.client.ListHistoricalRequests(ctx, false)
	if err != nil {
		return err
	}
	seen := make(map[int64]struct{}, len(requests))
	for _, req := range requests {
		if req.RequestID <= 0 {
			continue
		}
		seen[req.RequestID] = struct{}{}
		if req.RequestedStartAt == nil || req.RequestedEndAt == nil {
			_ = r.client.ReportHistoricalState(ctx, HistoricalRequestStateReport{
				RequestID: req.RequestID,
				Status:    "error",
				LastError: "historical request is missing requested time range",
			})
			continue
		}
		if r.isInFlight(req.RequestID) {
			continue
		}
		switch req.Key.Kind {
		case "kline":
			dbCfg, ok := r.databases[req.Key.Exchange]
			if !ok {
				_ = r.client.ReportHistoricalState(ctx, HistoricalRequestStateReport{
					RequestID: req.RequestID,
					Status:    "error",
					LastError: fmt.Sprintf("historical exchange %q is not configured in scraper", req.Key.Exchange),
				})
				continue
			}
			childCtx, cancel := context.WithCancel(ctx)
			r.track(req.RequestID, cancel)
			go r.runRequest(childCtx, req, dbCfg)
		case "funding_rate":
			if req.Key.Market != "futures" || req.Key.Interval != "" {
				_ = r.client.ReportHistoricalState(ctx, HistoricalRequestStateReport{
					RequestID: req.RequestID,
					Status:    "error",
					LastError: "historical Funding requires market=futures and empty interval",
				})
				continue
			}
			if r.registry == nil {
				_ = r.client.ReportHistoricalState(ctx, HistoricalRequestStateReport{RequestID: req.RequestID, Status: "error", LastError: "historical Funding registry is not configured"})
				continue
			}
			backfiller, err := r.registry.HistoricalFunding(req.Key.Exchange)
			if err != nil {
				_ = r.client.ReportHistoricalState(ctx, HistoricalRequestStateReport{RequestID: req.RequestID, Status: "error", LastError: err.Error()})
				continue
			}
			store := r.fundingStores[req.Key.Exchange]
			if store == nil {
				_ = r.client.ReportHistoricalState(ctx, HistoricalRequestStateReport{RequestID: req.RequestID, Status: "error", LastError: fmt.Sprintf("historical Funding store for exchange %q is not configured", req.Key.Exchange)})
				continue
			}
			childCtx, cancel := context.WithCancel(ctx)
			r.track(req.RequestID, cancel)
			go r.runFundingRequest(childCtx, req, backfiller, store)
		default:
			_ = r.client.ReportHistoricalState(ctx, HistoricalRequestStateReport{
				RequestID: req.RequestID,
				Status:    "error",
				LastError: fmt.Sprintf("historical kind %q is not supported yet", req.Key.Kind),
			})
		}
	}
	r.cancelMissing(seen)
	return nil
}

func (r *HistoricalRuntime) runFundingRequest(
	ctx context.Context,
	req HistoricalRequest,
	backfiller exchange.HistoricalFundingBackfiller,
	store exchange.HistoricalFundingStore,
) {
	defer r.untrack(req.RequestID)
	r.reportHistorical(ctx, HistoricalRequestStateReport{RequestID: req.RequestID, Status: "running"})
	segments, err := backfiller.BackfillFundingHistory(ctx, exchange.HistoricalFundingRequest{
		Exchange: req.Key.Exchange,
		Market:   req.Key.Market,
		Symbol:   req.Key.Symbol,
		StartAt:  req.RequestedStartAt.UTC(),
		EndAt:    req.RequestedEndAt.UTC(),
	}, store)
	if err != nil {
		r.reportHistorical(ctx, HistoricalRequestStateReport{RequestID: req.RequestID, Status: "error", LastError: err.Error()})
		return
	}
	if len(segments) == 0 {
		r.reportHistorical(ctx, HistoricalRequestStateReport{RequestID: req.RequestID, Status: "error", LastError: "historical Funding returned no explicit coverage segments"})
		return
	}
	reports := make([]CoverageSegmentReport, 0, len(segments))
	for _, segment := range segments {
		reports = append(reports, CoverageSegmentReport{
			Key:      req.Key,
			Year:     int32(segment.Year),
			StartAt:  segment.StartAt,
			EndAt:    segment.EndAt,
			RowCount: segment.RowCount,
			Source:   segment.Source,
		})
	}
	if err := r.client.ReportCoverageSegments(ctx, reports); err != nil {
		r.reportHistorical(ctx, HistoricalRequestStateReport{RequestID: req.RequestID, Status: "error", LastError: fmt.Sprintf("coverage report failed: %v", err)})
		return
	}
	start := req.RequestedStartAt.UTC()
	end := req.RequestedEndAt.UTC()
	r.reportHistorical(ctx, HistoricalRequestStateReport{
		RequestID: req.RequestID, Status: "ready", CoveredStartAt: &start, CoveredEndAt: &end,
	})
}

func (r *HistoricalRuntime) runRequest(ctx context.Context, req HistoricalRequest, dbCfg config.DatabaseConfig) {
	defer r.untrack(req.RequestID)

	request := backfill.KlineRequest{
		Exchange: req.Key.Exchange,
		Market:   req.Key.Market,
		Symbol:   req.Key.Symbol,
		Interval: req.Key.Interval,
		StartAt:  req.RequestedStartAt.UTC(),
		EndAt:    req.RequestedEndAt.UTC(),
	}

	cov, err := verifyKlineCoverage(ctx, dbCfg, request)
	if err != nil {
		r.reportHistorical(ctx, HistoricalRequestStateReport{
			RequestID: req.RequestID,
			Status:    "error",
			LastError: err.Error(),
		})
		return
	}
	if cov.Ready {
		r.reportHistorical(ctx, HistoricalRequestStateReport{
			RequestID:      req.RequestID,
			Status:         "ready",
			CoveredStartAt: cov.CoveredStartAt,
			CoveredEndAt:   cov.CoveredEndAt,
			LastError:      "",
		})
		return
	}

	r.reportHistorical(ctx, HistoricalRequestStateReport{
		RequestID:      req.RequestID,
		Status:         "running",
		CoveredStartAt: cov.CoveredStartAt,
		CoveredEndAt:   cov.CoveredEndAt,
		LastError:      "",
	})

	segments, err := runKlineBackfill(ctx, dbCfg, r.migrationsDir, request)
	if err != nil {
		r.reportHistorical(ctx, HistoricalRequestStateReport{
			RequestID:      req.RequestID,
			Status:         "error",
			CoveredStartAt: cov.CoveredStartAt,
			CoveredEndAt:   cov.CoveredEndAt,
			LastError:      err.Error(),
		})
		return
	}
	if len(segments) > 0 {
		if err := r.client.ReportCoverageSegments(ctx, coverageReportsFromSegments(req.Key, segments)); err != nil {
			r.reportHistorical(ctx, HistoricalRequestStateReport{
				RequestID:      req.RequestID,
				Status:         "error",
				CoveredStartAt: cov.CoveredStartAt,
				CoveredEndAt:   cov.CoveredEndAt,
				LastError:      fmt.Sprintf("coverage report failed: %v", err),
			})
			return
		}
	}

	r.reportHistorical(ctx, HistoricalRequestStateReport{
		RequestID:      req.RequestID,
		Status:         "verifying",
		CoveredStartAt: cov.CoveredStartAt,
		CoveredEndAt:   cov.CoveredEndAt,
	})

	cov, err = verifyKlineCoverage(ctx, dbCfg, request)
	if err != nil {
		r.reportHistorical(ctx, HistoricalRequestStateReport{
			RequestID: req.RequestID,
			Status:    "error",
			LastError: err.Error(),
		})
		return
	}
	status := "ready"
	lastError := ""
	if !cov.Ready {
		status = "error"
		lastError = "coverage incomplete after backfill"
	}
	r.reportHistorical(ctx, HistoricalRequestStateReport{
		RequestID:      req.RequestID,
		Status:         status,
		CoveredStartAt: cov.CoveredStartAt,
		CoveredEndAt:   cov.CoveredEndAt,
		LastError:      lastError,
	})
}

func coverageReportsFromSegments(key StreamKey, segments []backfill.CoverageSegment) []CoverageSegmentReport {
	reports := make([]CoverageSegmentReport, 0, len(segments))
	for _, segment := range segments {
		reportKey := key
		if reportKey.Exchange == "" {
			reportKey.Exchange = segment.Exchange
		}
		if reportKey.Market == "" {
			reportKey.Market = segment.Market
		}
		if reportKey.Symbol == "" {
			reportKey.Symbol = segment.Symbol
		}
		if reportKey.Interval == "" {
			reportKey.Interval = segment.Interval
		}
		reports = append(reports, CoverageSegmentReport{
			Key:      reportKey,
			Year:     int32(segment.Year),
			StartAt:  segment.StartAt,
			EndAt:    segment.EndAt,
			RowCount: segment.RowCount,
			Source:   segment.Source,
		})
	}
	return reports
}

func (r *HistoricalRuntime) reportHistorical(ctx context.Context, report HistoricalRequestStateReport) {
	if err := r.client.ReportHistoricalState(ctx, report); err != nil {
		logger.NewSystemHelper().Error("market_data_history_state_report_failed", map[string]any{
			"request_id": report.RequestID,
			"status":     report.Status,
			"error":      err.Error(),
		})
	}
}

func (r *HistoricalRuntime) isInFlight(requestID int64) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	_, ok := r.inFlight[requestID]
	return ok
}

func (r *HistoricalRuntime) track(requestID int64, cancel context.CancelFunc) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.inFlight[requestID] = cancel
}

func (r *HistoricalRuntime) untrack(requestID int64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.inFlight, requestID)
}

func (r *HistoricalRuntime) cancelMissing(seen map[int64]struct{}) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for requestID, cancel := range r.inFlight {
		if _, ok := seen[requestID]; ok {
			continue
		}
		if cancel != nil {
			cancel()
		}
		delete(r.inFlight, requestID)
	}
}

func (r *HistoricalRuntime) stopAll() {
	r.mu.Lock()
	defer r.mu.Unlock()
	for requestID, cancel := range r.inFlight {
		if cancel != nil {
			cancel()
		}
		delete(r.inFlight, requestID)
	}
}
