package marketdata

import (
	"context"
	"sync/atomic"

	"github.com/hushine-tech/scraper/internal/models"
)

// GatedPublisher lets the runtime keep DB ingestion always-on while toggling
// live Kafka publication per stream via effective_live_delivery.
type GatedPublisher struct {
	base    Publisher
	enabled atomic.Bool
}

func NewGatedPublisher(base Publisher, enabled bool) *GatedPublisher {
	p := &GatedPublisher{base: base}
	p.enabled.Store(enabled)
	return p
}

func (p *GatedPublisher) SetEnabled(enabled bool) {
	if p == nil {
		return
	}
	p.enabled.Store(enabled)
}

func (p *GatedPublisher) Enabled() bool {
	if p == nil {
		return false
	}
	return p.enabled.Load()
}

func (p *GatedPublisher) PublishKlines(ctx context.Context, klines []models.Kline) error {
	if p == nil || p.base == nil || !p.enabled.Load() {
		return nil
	}
	return p.base.PublishKlines(ctx, klines)
}

func (p *GatedPublisher) Close() error {
	// Ownership of the underlying producer stays with the caller / process
	// root; per-stream gates are lightweight views.
	return nil
}
