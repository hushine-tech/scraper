package controlplane

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/hushine-tech/scraper/internal/backfill"
	"github.com/hushine-tech/scraper/internal/config"
	"github.com/hushine-tech/scraper/internal/marketdata"
	"github.com/hushine-tech/scraper/internal/models"
	basescraper "github.com/hushine-tech/scraper/internal/scraper"
)

type fakeClient struct {
	mu              sync.Mutex
	streams         []Stream
	history         []HistoricalRequest
	reports         []StreamStateReport
	historyReports  []HistoricalRequestStateReport
	coverageReports []CoverageSegmentReport
	coverageErr     error
	closed          bool
}

func (c *fakeClient) ListStreams(_ context.Context) ([]Stream, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]Stream, len(c.streams))
	copy(out, c.streams)
	return out, nil
}

func (c *fakeClient) ListHistoricalRequests(_ context.Context, _ bool) ([]HistoricalRequest, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]HistoricalRequest, len(c.history))
	copy(out, c.history)
	return out, nil
}

func (c *fakeClient) ReportState(_ context.Context, report StreamStateReport) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.reports = append(c.reports, report)
	return nil
}

func (c *fakeClient) ReportHistoricalState(_ context.Context, report HistoricalRequestStateReport) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.historyReports = append(c.historyReports, report)
	return nil
}

func (c *fakeClient) ReportCoverageSegments(_ context.Context, reports []CoverageSegmentReport) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.coverageReports = append(c.coverageReports, reports...)
	return c.coverageErr
}

func (c *fakeClient) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.closed = true
	return nil
}

func (c *fakeClient) setStreams(streams ...Stream) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.streams = append([]Stream(nil), streams...)
}

func (c *fakeClient) lastReport(t *testing.T) StreamStateReport {
	t.Helper()
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.reports) == 0 {
		t.Fatal("expected at least one report")
	}
	return c.reports[len(c.reports)-1]
}

func (c *fakeClient) lastHistoryReport(t *testing.T) HistoricalRequestStateReport {
	t.Helper()
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.historyReports) == 0 {
		t.Fatal("expected at least one history report")
	}
	return c.historyReports[len(c.historyReports)-1]
}

func (c *fakeClient) coverageReportCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.coverageReports)
}

func (c *fakeClient) firstCoverageReport(t *testing.T) CoverageSegmentReport {
	t.Helper()
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.coverageReports) == 0 {
		t.Fatal("expected at least one coverage report")
	}
	return c.coverageReports[0]
}

type fakeCollector struct {
	mu        sync.Mutex
	observer  basescraper.KlineObserver
	publisher marketdata.Publisher
	started   chan struct{}
	stopCount int
}

func newFakeCollector(publisher marketdata.Publisher) *fakeCollector {
	return &fakeCollector{
		publisher: publisher,
		started:   make(chan struct{}),
	}
}

func (c *fakeCollector) Run(ctx context.Context) {
	select {
	case <-c.started:
	default:
		close(c.started)
	}
	<-ctx.Done()
}

func (c *fakeCollector) Stop() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.stopCount++
}

func (c *fakeCollector) SetObserver(observer basescraper.KlineObserver) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.observer = observer
}

func (c *fakeCollector) emitStored(closeTime time.Time) {
	c.mu.Lock()
	observer := c.observer
	c.mu.Unlock()
	if observer == nil {
		return
	}
	observer.OnStored(models.Kline{CloseTime: closeTime.UTC()})
}

func (c *fakeCollector) stopCalls() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.stopCount
}

type fakeFactory struct {
	mu         sync.Mutex
	calls      int
	collectors map[int64]*fakeCollector
	err        error
}

func newFakeFactory() *fakeFactory {
	return &fakeFactory{
		collectors: make(map[int64]*fakeCollector),
	}
}

func (f *fakeFactory) build(stream Stream, publisher marketdata.Publisher) (ManagedCollector, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	if f.err != nil {
		return nil, f.err
	}
	collector := newFakeCollector(publisher)
	f.collectors[stream.StreamID] = collector
	return collector, nil
}

func (f *fakeFactory) callsCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

func (f *fakeFactory) collector(streamID int64) *fakeCollector {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.collectors[streamID]
}

func TestHistoricalRuntimeReportsCoverageSegmentsBeforeReady(t *testing.T) {
	client := &fakeClient{}
	runtime := NewHistoricalRuntime(HistoricalRuntimeConfig{Client: client})

	start := mustRuntimeTime("2026-01-01T00:00:00Z")
	end := mustRuntimeTime("2026-01-01T00:02:00Z")
	req := historicalRequestForRuntime(start, end)

	restore := stubHistoricalBackfill(t,
		func(_ context.Context, _ config.DatabaseConfig, _ backfill.KlineRequest) (backfill.Coverage, error) {
			return backfill.Coverage{Ready: false}, nil
		},
		func(_ context.Context, _ config.DatabaseConfig, _ string, _ backfill.KlineRequest) ([]backfill.CoverageSegment, error) {
			return []backfill.CoverageSegment{{
				Exchange: "binance",
				Market:   "futures",
				Symbol:   "BTCUSDT",
				Interval: "1m",
				Year:     2026,
				StartAt:  start,
				EndAt:    end,
				RowCount: 2,
				Source:   "historical_backfill",
			}}, nil
		},
		func(_ context.Context, _ config.DatabaseConfig, _ backfill.KlineRequest) (backfill.Coverage, error) {
			return backfill.Coverage{Ready: true, CoveredStartAt: &start, CoveredEndAt: &end}, nil
		},
	)
	defer restore()

	runtime.runRequest(context.Background(), req, config.DatabaseConfig{})

	if got := client.coverageReportCount(); got != 1 {
		t.Fatalf("expected one coverage report, got %d", got)
	}
	report := client.firstCoverageReport(t)
	if report.Key != req.Key {
		t.Fatalf("coverage key = %#v, want %#v", report.Key, req.Key)
	}
	if report.Year != 2026 || !report.StartAt.Equal(start) || !report.EndAt.Equal(end) || report.RowCount != 2 || report.Source != "historical_backfill" {
		t.Fatalf("unexpected coverage report: %#v", report)
	}
	if got := client.lastHistoryReport(t).Status; got != "ready" {
		t.Fatalf("expected final ready status, got %q", got)
	}
}

func TestHistoricalRuntimeStopsBeforeReadyWhenCoverageReportFails(t *testing.T) {
	client := &fakeClient{coverageErr: errors.New("control-plane unavailable")}
	runtime := NewHistoricalRuntime(HistoricalRuntimeConfig{Client: client})

	start := mustRuntimeTime("2026-01-01T00:00:00Z")
	end := mustRuntimeTime("2026-01-01T00:01:00Z")
	req := historicalRequestForRuntime(start, end)

	restore := stubHistoricalBackfill(t,
		func(_ context.Context, _ config.DatabaseConfig, _ backfill.KlineRequest) (backfill.Coverage, error) {
			return backfill.Coverage{Ready: false}, nil
		},
		func(_ context.Context, _ config.DatabaseConfig, _ string, _ backfill.KlineRequest) ([]backfill.CoverageSegment, error) {
			return []backfill.CoverageSegment{{
				Exchange: "binance",
				Market:   "futures",
				Symbol:   "BTCUSDT",
				Interval: "1m",
				Year:     2026,
				StartAt:  start,
				EndAt:    end,
				RowCount: 1,
				Source:   "historical_backfill",
			}}, nil
		},
	)
	defer restore()

	runtime.runRequest(context.Background(), req, config.DatabaseConfig{})

	last := client.lastHistoryReport(t)
	if last.Status != "error" {
		t.Fatalf("expected final error status, got %q", last.Status)
	}
	if !strings.Contains(last.LastError, "coverage report failed") {
		t.Fatalf("expected coverage report failure error, got %q", last.LastError)
	}
	if strings.Contains(last.LastError, "coverage incomplete after backfill") {
		t.Fatalf("expected to stop before verify/ready path, got %q", last.LastError)
	}
}

func historicalRequestForRuntime(start, end time.Time) HistoricalRequest {
	return HistoricalRequest{
		RequestID:        42,
		Key:              StreamKey{Exchange: "binance", Market: "futures", Kind: "kline", Symbol: "BTCUSDT", Interval: "1m"},
		RequestedStartAt: &start,
		RequestedEndAt:   &end,
	}
}

func stubHistoricalBackfill(
	t *testing.T,
	firstVerify func(context.Context, config.DatabaseConfig, backfill.KlineRequest) (backfill.Coverage, error),
	run func(context.Context, config.DatabaseConfig, string, backfill.KlineRequest) ([]backfill.CoverageSegment, error),
	remainingVerify ...func(context.Context, config.DatabaseConfig, backfill.KlineRequest) (backfill.Coverage, error),
) func() {
	t.Helper()
	oldVerify := verifyKlineCoverage
	oldRun := runKlineBackfill
	verifyCalls := 0
	verifyKlineCoverage = func(ctx context.Context, dbCfg config.DatabaseConfig, req backfill.KlineRequest) (backfill.Coverage, error) {
		verifyCalls++
		if verifyCalls == 1 {
			return firstVerify(ctx, dbCfg, req)
		}
		if len(remainingVerify) > 0 {
			return remainingVerify[0](ctx, dbCfg, req)
		}
		t.Fatalf("unexpected verify call %d", verifyCalls)
		return backfill.Coverage{}, nil
	}
	runKlineBackfill = run
	return func() {
		verifyKlineCoverage = oldVerify
		runKlineBackfill = oldRun
	}
}

func mustRuntimeTime(raw string) time.Time {
	t, err := time.Parse(time.RFC3339, raw)
	if err != nil {
		panic(err)
	}
	return t.UTC()
}

func TestRuntimeReconcileStartKeepAndStopWithGrace(t *testing.T) {
	client := &fakeClient{}
	factory := newFakeFactory()
	now := time.Unix(1_711_929_600, 0).UTC()

	runtime := NewRuntime(RuntimeConfig{
		Client:              client,
		Factories:           map[string]CollectorFactory{"binance": factory.build},
		ReconcileInterval:   time.Second,
		DrainingGracePeriod: time.Minute,
		Now: func() time.Time {
			return now
		},
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	defer runtime.Stop()

	stream := Stream{
		StreamID:     1,
		DesiredState: DesiredStateRunning,
		Key: StreamKey{
			Exchange: "binance",
			Market:   "futures",
			Kind:     "kline",
			Symbol:   "BTCUSDT",
			Interval: "1m",
		},
		EffectiveLiveDelivery: true,
	}
	client.setStreams(stream)

	if err := runtime.ReconcileOnce(ctx); err != nil {
		t.Fatalf("ReconcileOnce start: %v", err)
	}
	collector := factory.collector(stream.StreamID)
	if collector == nil {
		t.Fatal("expected collector to be created")
	}
	select {
	case <-collector.started:
	case <-time.After(time.Second):
		t.Fatal("collector did not start")
	}
	if got := client.lastReport(t).ActualState; got != ActualStateStarting {
		t.Fatalf("expected starting report, got %q", got)
	}

	if err := runtime.ReconcileOnce(ctx); err != nil {
		t.Fatalf("ReconcileOnce keep: %v", err)
	}
	if got := factory.callsCount(); got != 1 {
		t.Fatalf("expected single collector creation, got %d", got)
	}

	stream.DesiredState = ActualStateStopped
	client.setStreams(stream)
	if err := runtime.ReconcileOnce(ctx); err != nil {
		t.Fatalf("ReconcileOnce draining: %v", err)
	}
	if got := client.lastReport(t).ActualState; got != ActualStateDraining {
		t.Fatalf("expected draining report, got %q", got)
	}
	if collector.stopCalls() != 0 {
		t.Fatalf("expected collector to stay alive during grace, got stopCount=%d", collector.stopCalls())
	}

	now = now.Add(61 * time.Second)
	if err := runtime.ReconcileOnce(ctx); err != nil {
		t.Fatalf("ReconcileOnce stop: %v", err)
	}
	if got := client.lastReport(t).ActualState; got != ActualStateStopped {
		t.Fatalf("expected stopped report, got %q", got)
	}
	if collector.stopCalls() != 1 {
		t.Fatalf("expected collector stop after grace, got stopCount=%d", collector.stopCalls())
	}
}

func TestRuntimeKeepsStreamRunningWhileLeaseIsActive(t *testing.T) {
	client := &fakeClient{}
	factory := newFakeFactory()
	now := time.Unix(1_711_929_600, 0).UTC()

	runtime := NewRuntime(RuntimeConfig{
		Client:              client,
		Factories:           map[string]CollectorFactory{"binance": factory.build},
		DrainingGracePeriod: time.Minute,
		Now: func() time.Time {
			return now
		},
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	defer runtime.Stop()

	stream := Stream{
		StreamID:     2,
		DesiredState: DesiredStateRunning,
		Key: StreamKey{
			Exchange: "binance",
			Market:   "spot",
			Kind:     "kline",
			Symbol:   "ETHUSDT",
			Interval: "1m",
		},
	}
	client.setStreams(stream)
	if err := runtime.ReconcileOnce(ctx); err != nil {
		t.Fatalf("start reconcile: %v", err)
	}

	collector := factory.collector(stream.StreamID)
	if collector == nil {
		t.Fatal("expected collector")
	}
	select {
	case <-collector.started:
	case <-time.After(time.Second):
		t.Fatal("collector did not start")
	}

	collector.emitStored(now.Add(time.Minute))
	if got := client.lastReport(t).ActualState; got != ActualStateRunning {
		t.Fatalf("expected running report after stored kline, got %q", got)
	}

	stream.DesiredState = ActualStateStopped
	stream.ActiveLeaseCount = 1
	client.setStreams(stream)
	if err := runtime.ReconcileOnce(ctx); err != nil {
		t.Fatalf("lease-protected reconcile: %v", err)
	}
	if got := client.lastReport(t).ActualState; got != ActualStateRunning {
		t.Fatalf("expected running report while lease active, got %q", got)
	}
	if collector.stopCalls() != 0 {
		t.Fatalf("expected no stop while lease active, got %d", collector.stopCalls())
	}
}

func TestRuntimeReportsFactoryErrors(t *testing.T) {
	client := &fakeClient{}
	factory := newFakeFactory()
	factory.err = errors.New("unsupported market")

	runtime := NewRuntime(RuntimeConfig{
		Client:    client,
		Factories: map[string]CollectorFactory{"binance": factory.build},
	})
	defer runtime.Stop()

	client.setStreams(Stream{
		StreamID:     3,
		DesiredState: DesiredStateRunning,
		Key: StreamKey{
			Exchange: "binance",
			Market:   "futures",
			Kind:     "kline",
			Symbol:   "BNBUSDT",
			Interval: "1m",
		},
	})

	if err := runtime.ReconcileOnce(context.Background()); err != nil {
		t.Fatalf("ReconcileOnce error path: %v", err)
	}
	report := client.lastReport(t)
	if report.ActualState != ActualStateError {
		t.Fatalf("expected error report, got %q", report.ActualState)
	}
	if report.LastError != "unsupported market" {
		t.Fatalf("unexpected error text: %q", report.LastError)
	}
}

func TestRuntimeTogglesLiveDeliveryWithoutRestart(t *testing.T) {
	client := &fakeClient{}
	factory := newFakeFactory()

	runtime := NewRuntime(RuntimeConfig{
		Client:    client,
		Factories: map[string]CollectorFactory{"binance": factory.build},
		Publisher: &gatedNoopPublisher{},
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	defer runtime.Stop()

	stream := Stream{
		StreamID:     4,
		DesiredState: DesiredStateRunning,
		Key: StreamKey{
			Exchange: "binance",
			Market:   "futures",
			Kind:     "kline",
			Symbol:   "SOLUSDT",
			Interval: "1m",
		},
		EffectiveLiveDelivery: false,
	}
	client.setStreams(stream)
	if err := runtime.ReconcileOnce(ctx); err != nil {
		t.Fatalf("ReconcileOnce initial: %v", err)
	}

	collector := factory.collector(stream.StreamID)
	if collector == nil {
		t.Fatal("expected collector")
	}
	select {
	case <-collector.started:
	case <-time.After(time.Second):
		t.Fatal("collector did not start")
	}

	gate, ok := collector.publisher.(*marketdata.GatedPublisher)
	if !ok {
		t.Fatalf("expected gated publisher, got %T", collector.publisher)
	}
	if gate.Enabled() {
		t.Fatal("expected live delivery to start disabled")
	}

	stream.EffectiveLiveDelivery = true
	client.setStreams(stream)
	if err := runtime.ReconcileOnce(ctx); err != nil {
		t.Fatalf("ReconcileOnce toggle: %v", err)
	}
	if got := factory.callsCount(); got != 1 {
		t.Fatalf("expected no collector restart, got %d builds", got)
	}
	if !gate.Enabled() {
		t.Fatal("expected live delivery gate to flip on")
	}
	if collector.stopCalls() != 0 {
		t.Fatalf("expected collector to stay running, got stopCount=%d", collector.stopCalls())
	}
}

type gatedNoopPublisher struct{}

func (g *gatedNoopPublisher) PublishKlines(_ context.Context, _ []models.Kline) error { return nil }
func (g *gatedNoopPublisher) Close() error                                            { return nil }
