package controlplane

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/hushine-tech/scraper/internal/logger"
	"github.com/hushine-tech/scraper/internal/marketdata"
	"github.com/hushine-tech/scraper/internal/models"
	basescraper "github.com/hushine-tech/scraper/internal/scraper"
)

const (
	DesiredStateRunning = "running"

	ActualStatePending  = "pending"
	ActualStateStarting = "starting"
	ActualStateRunning  = "running"
	ActualStateDraining = "draining"
	ActualStateStopped  = "stopped"
	ActualStateError    = "error"
)

type StreamKey struct {
	Exchange string
	Market   string
	Kind     string
	Symbol   string
	Interval string
}

type Stream struct {
	StreamID              int64
	Key                   StreamKey
	DesiredState          string
	ActualState           string
	EffectiveLiveDelivery bool
	ActiveLeaseCount      int
	LastDataAt            *time.Time
	LastError             string
}

type StreamStateReport struct {
	StreamID    int64
	ActualState string
	LastDataAt  *time.Time
	LastError   string
}

type ManagedCollector interface {
	Run(ctx context.Context)
	Stop()
	SetObserver(observer basescraper.KlineObserver)
}

type CollectorFactory func(stream Stream, publisher marketdata.Publisher) (ManagedCollector, error)

type RuntimeConfig struct {
	Client              Client
	Factories           map[string]CollectorFactory
	Publisher           marketdata.Publisher
	ReconcileInterval   time.Duration
	DrainingGracePeriod time.Duration
	ReportTimeout       time.Duration
	Now                 func() time.Time
}

type Runtime struct {
	client              Client
	factories           map[string]CollectorFactory
	publisher           marketdata.Publisher
	reconcileInterval   time.Duration
	drainingGracePeriod time.Duration
	reportTimeout       time.Duration
	now                 func() time.Time

	mu        sync.Mutex
	managed   map[int64]*managedStream
	runCancel context.CancelFunc

	stopOnce     sync.Once
	shutdownOnce sync.Once
}

type managedStream struct {
	stream        Stream
	collector     ManagedCollector
	cancel        context.CancelFunc
	publisher     *marketdata.GatedPublisher
	state         string
	lastDataAt    *time.Time
	lastError     string
	drainDeadline time.Time
	version       int64
}

type streamObserver struct {
	runtime *Runtime
	stream  int64
	version int64
}

func NewRuntime(cfg RuntimeConfig) *Runtime {
	factories := make(map[string]CollectorFactory, len(cfg.Factories))
	for exchange, factory := range cfg.Factories {
		exchange = strings.ToLower(strings.TrimSpace(exchange))
		if exchange == "" || factory == nil {
			continue
		}
		factories[exchange] = factory
	}
	reconcileInterval := cfg.ReconcileInterval
	if reconcileInterval <= 0 {
		reconcileInterval = 5 * time.Second
	}
	grace := cfg.DrainingGracePeriod
	if grace <= 0 {
		grace = 60 * time.Second
	}
	reportTimeout := cfg.ReportTimeout
	if reportTimeout <= 0 {
		reportTimeout = 5 * time.Second
	}
	nowFn := cfg.Now
	if nowFn == nil {
		nowFn = time.Now
	}

	return &Runtime{
		client:              cfg.Client,
		factories:           factories,
		publisher:           cfg.Publisher,
		reconcileInterval:   reconcileInterval,
		drainingGracePeriod: grace,
		reportTimeout:       reportTimeout,
		now:                 nowFn,
		managed:             make(map[int64]*managedStream),
	}
}

func (r *Runtime) Run(ctx context.Context) {
	if r == nil {
		return
	}
	runCtx, cancel := context.WithCancel(ctx)
	r.mu.Lock()
	r.runCancel = cancel
	r.mu.Unlock()
	defer cancel()
	defer r.shutdown()

	if err := r.ReconcileOnce(runCtx); err != nil {
		logger.NewSystemHelper().Error("market_data_control_plane_reconcile_failed", map[string]any{
			"error": err.Error(),
		})
	}

	ticker := time.NewTicker(r.reconcileInterval)
	defer ticker.Stop()

	for {
		select {
		case <-runCtx.Done():
			return
		case <-ticker.C:
			if err := r.ReconcileOnce(runCtx); err != nil {
				logger.NewSystemHelper().Error("market_data_control_plane_reconcile_failed", map[string]any{
					"error": err.Error(),
				})
			}
		}
	}
}

func (r *Runtime) Stop() {
	if r == nil {
		return
	}
	r.stopOnce.Do(func() {
		r.mu.Lock()
		cancel := r.runCancel
		r.mu.Unlock()
		if cancel != nil {
			cancel()
		}
		r.shutdown()
	})
}

func (r *Runtime) ReconcileOnce(ctx context.Context) error {
	if r == nil || r.client == nil {
		return fmt.Errorf("control-plane runtime client is required")
	}

	streams, err := r.client.ListStreams(ctx)
	if err != nil {
		return err
	}

	now := r.now().UTC()
	seen := make(map[int64]struct{}, len(streams))
	for _, raw := range streams {
		stream := normalizeStream(raw)
		if stream.StreamID <= 0 || stream.Key.Kind != "kline" {
			continue
		}

		factory, ok := r.factories[stream.Key.Exchange]
		if !ok {
			continue
		}

		seen[stream.StreamID] = struct{}{}
		r.reconcileStream(ctx, now, stream, factory)
	}

	r.stopMissingStreams(seen)
	return nil
}

func (r *Runtime) reconcileStream(ctx context.Context, now time.Time, stream Stream, factory CollectorFactory) {
	var (
		report    StreamStateReport
		collector ManagedCollector
		cancel    context.CancelFunc
		version   int64
	)

	r.mu.Lock()
	handle := r.managed[stream.StreamID]
	if handle == nil {
		handle = &managedStream{}
		r.managed[stream.StreamID] = handle
	}
	handle.stream = stream

	liveEnabled := stream.EffectiveLiveDelivery && r.publisher != nil
	if handle.publisher != nil {
		handle.publisher.SetEnabled(liveEnabled)
	}

	warning := ""
	if stream.EffectiveLiveDelivery && r.publisher == nil {
		warning = "effective_live_delivery requested but market-data kafka publisher is unavailable"
	}

	if shouldRun(stream) {
		handle.drainDeadline = time.Time{}
		if handle.collector != nil {
			if handle.state == "" || handle.state == ActualStateStopped || handle.state == ActualStateError {
				if handle.lastDataAt != nil {
					handle.state = ActualStateRunning
				} else {
					handle.state = ActualStateStarting
				}
			}
			report = snapshotReport(handle, warning)
			r.mu.Unlock()
			r.report(ctx, report)
			return
		}

		publisher := marketdata.NewGatedPublisher(r.publisher, liveEnabled)
		collector, err := factory(stream, publisher)
		if err != nil {
			handle.publisher = nil
			handle.collector = nil
			handle.cancel = nil
			handle.state = ActualStateError
			handle.lastError = err.Error()
			report = snapshotReport(handle, handle.lastError)
			r.mu.Unlock()
			r.report(ctx, report)
			return
		}

		handle.version++
		version = handle.version
		childCtx, childCancel := context.WithCancel(ctx)
		collector.SetObserver(&streamObserver{runtime: r, stream: stream.StreamID, version: version})
		handle.collector = collector
		handle.cancel = childCancel
		handle.publisher = publisher
		handle.state = ActualStateStarting
		handle.lastError = warning
		report = snapshotReport(handle, warning)
		r.mu.Unlock()

		go r.runCollector(childCtx, stream.StreamID, version, collector)
		r.report(ctx, report)
		return
	}

	if handle.collector == nil {
		handle.state = ActualStateStopped
		handle.lastError = ""
		handle.drainDeadline = time.Time{}
		report = snapshotReport(handle, "")
		r.mu.Unlock()
		r.report(ctx, report)
		return
	}

	if handle.drainDeadline.IsZero() {
		handle.drainDeadline = now.Add(r.drainingGracePeriod)
	}
	if now.Before(handle.drainDeadline) {
		handle.state = ActualStateDraining
		handle.lastError = ""
		report = snapshotReport(handle, "")
		r.mu.Unlock()
		r.report(ctx, report)
		return
	}

	collector = handle.collector
	cancel = handle.cancel
	handle.version++
	handle.collector = nil
	handle.cancel = nil
	handle.publisher = nil
	handle.state = ActualStateStopped
	handle.lastError = ""
	handle.drainDeadline = time.Time{}
	report = snapshotReport(handle, "")
	r.mu.Unlock()

	if cancel != nil {
		cancel()
	}
	if collector != nil {
		collector.Stop()
	}
	r.report(ctx, report)
}

func (r *Runtime) stopMissingStreams(seen map[int64]struct{}) {
	type toStop struct {
		collector ManagedCollector
		cancel    context.CancelFunc
	}

	var stops []toStop

	r.mu.Lock()
	for streamID, handle := range r.managed {
		if _, ok := seen[streamID]; ok {
			continue
		}
		if handle.cancel != nil || handle.collector != nil {
			handle.version++
			stops = append(stops, toStop{collector: handle.collector, cancel: handle.cancel})
		}
		delete(r.managed, streamID)
	}
	r.mu.Unlock()

	for _, stop := range stops {
		if stop.cancel != nil {
			stop.cancel()
		}
		if stop.collector != nil {
			stop.collector.Stop()
		}
	}
}

func (r *Runtime) runCollector(ctx context.Context, streamID, version int64, collector ManagedCollector) {
	collector.Run(ctx)

	if ctx.Err() != nil {
		return
	}

	var report StreamStateReport

	r.mu.Lock()
	handle := r.managed[streamID]
	if handle == nil || handle.version != version || handle.collector == nil {
		r.mu.Unlock()
		return
	}
	handle.collector = nil
	handle.cancel = nil
	handle.publisher = nil
	handle.state = ActualStateError
	handle.lastError = "collector exited unexpectedly"
	report = snapshotReport(handle, handle.lastError)
	r.mu.Unlock()

	r.report(context.Background(), report)
}

func (r *Runtime) recordStored(streamID, version int64, at time.Time) {
	var report StreamStateReport

	r.mu.Lock()
	handle := r.managed[streamID]
	if handle == nil || handle.version != version {
		r.mu.Unlock()
		return
	}
	ts := at.UTC()
	handle.lastDataAt = &ts
	handle.state = ActualStateRunning
	handle.lastError = ""
	warning := ""
	if handle.stream.EffectiveLiveDelivery && r.publisher == nil {
		warning = "effective_live_delivery requested but market-data kafka publisher is unavailable"
		handle.lastError = warning
	}
	report = snapshotReport(handle, warning)
	r.mu.Unlock()

	r.report(context.Background(), report)
}

func (r *Runtime) report(parent context.Context, report StreamStateReport) {
	if r == nil || r.client == nil || report.StreamID <= 0 || report.ActualState == "" {
		return
	}

	ctx := parent
	if ctx == nil {
		ctx = context.Background()
	}
	if r.reportTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, r.reportTimeout)
		defer cancel()
	}

	if err := r.client.ReportState(ctx, report); err != nil {
		logger.NewSystemHelper().Error("market_data_stream_state_report_failed", map[string]any{
			"stream_id":    report.StreamID,
			"actual_state": report.ActualState,
			"error":        err.Error(),
		})
	}
}

func (r *Runtime) shutdown() {
	if r == nil {
		return
	}
	r.shutdownOnce.Do(func() {
		type toStop struct {
			collector ManagedCollector
			cancel    context.CancelFunc
		}

		var stops []toStop

		r.mu.Lock()
		for streamID, handle := range r.managed {
			if handle.cancel != nil || handle.collector != nil {
				stops = append(stops, toStop{collector: handle.collector, cancel: handle.cancel})
			}
			delete(r.managed, streamID)
		}
		r.mu.Unlock()

		for _, stop := range stops {
			if stop.cancel != nil {
				stop.cancel()
			}
			if stop.collector != nil {
				stop.collector.Stop()
			}
		}

		if r.client != nil {
			if err := r.client.Close(); err != nil {
				logger.NewSystemHelper().Warning("market_data_control_plane_close_failed", map[string]any{
					"error": err.Error(),
				})
			}
		}
	})
}

func (o *streamObserver) OnStored(kline models.Kline) {
	o.runtime.recordStored(o.stream, o.version, kline.CloseTime)
}

func normalizeStream(stream Stream) Stream {
	stream.DesiredState = strings.ToLower(strings.TrimSpace(stream.DesiredState))
	stream.ActualState = strings.ToLower(strings.TrimSpace(stream.ActualState))
	stream.LastError = strings.TrimSpace(stream.LastError)
	stream.Key.Exchange = strings.ToLower(strings.TrimSpace(stream.Key.Exchange))
	stream.Key.Market = strings.ToLower(strings.TrimSpace(stream.Key.Market))
	stream.Key.Kind = strings.ToLower(strings.TrimSpace(stream.Key.Kind))
	stream.Key.Symbol = strings.ToUpper(strings.TrimSpace(stream.Key.Symbol))
	stream.Key.Interval = strings.TrimSpace(stream.Key.Interval)
	if stream.LastDataAt != nil {
		t := stream.LastDataAt.UTC()
		stream.LastDataAt = &t
	}
	return stream
}

func shouldRun(stream Stream) bool {
	return stream.DesiredState == DesiredStateRunning || stream.ActiveLeaseCount > 0
}

func snapshotReport(handle *managedStream, lastErr string) StreamStateReport {
	return StreamStateReport{
		StreamID:    handle.stream.StreamID,
		ActualState: handle.state,
		LastDataAt:  cloneTime(handle.lastDataAt),
		LastError:   strings.TrimSpace(lastErr),
	}
}

func cloneTime(ts *time.Time) *time.Time {
	if ts == nil {
		return nil
	}
	value := ts.UTC()
	return &value
}
