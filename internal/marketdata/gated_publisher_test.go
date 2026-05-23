package marketdata

import (
	"context"
	"testing"
	"time"

	"github.com/hushine-tech/scraper/internal/models"
)

type gatedCapturePublisher struct {
	klines [][]models.Kline
}

func (p *gatedCapturePublisher) PublishKlines(_ context.Context, klines []models.Kline) error {
	p.klines = append(p.klines, append([]models.Kline(nil), klines...))
	return nil
}

func (p *gatedCapturePublisher) Close() error { return nil }

func TestGatedPublisherTogglesLiveDelivery(t *testing.T) {
	base := &gatedCapturePublisher{}
	gate := NewGatedPublisher(base, false)
	batch := []models.Kline{{
		Symbol:    "BTCUSDT",
		Market:    "futures",
		Exchange:  "binance",
		Interval:  "1m",
		OpenTime:  time.UnixMilli(1_711_929_600_000).UTC(),
		CloseTime: time.UnixMilli(1_711_929_659_999).UTC(),
	}}

	if err := gate.PublishKlines(context.Background(), batch); err != nil {
		t.Fatalf("PublishKlines disabled gate: %v", err)
	}
	if len(base.klines) != 0 {
		t.Fatalf("expected disabled gate to skip publish, got %d batches", len(base.klines))
	}

	gate.SetEnabled(true)
	if !gate.Enabled() {
		t.Fatal("expected gate to report enabled")
	}
	if err := gate.PublishKlines(context.Background(), batch); err != nil {
		t.Fatalf("PublishKlines enabled gate: %v", err)
	}
	if len(base.klines) != 1 {
		t.Fatalf("expected enabled gate to forward one batch, got %d", len(base.klines))
	}
}
