package spotkline

import (
	"context"
	"testing"
	"time"

	"github.com/hushine-tech/scraper/internal/models"
	"github.com/hushine-tech/scraper/internal/scraper"
)

type fakeStore struct {
	klines [][]models.Kline
}

func (s *fakeStore) InsertKline(_ context.Context, klines []models.Kline) error {
	s.klines = append(s.klines, append([]models.Kline(nil), klines...))
	return nil
}

type fakePublisher struct {
	klines [][]models.Kline
	err    error
}

func (p *fakePublisher) PublishKlines(_ context.Context, klines []models.Kline) error {
	p.klines = append(p.klines, append([]models.Kline(nil), klines...))
	return p.err
}

func (p *fakePublisher) Close() error { return nil }

func TestHandleForwardKlineIgnoresOpenBars(t *testing.T) {
	store := &fakeStore{}
	publisher := &fakePublisher{}
	sc := &Scraper{
		storage:   store,
		publisher: publisher,
	}

	err := sc.handleForwardKline(context.Background(), models.Kline{
		Symbol:    "BTCUSDT",
		Market:    "spot",
		Exchange:  "binance",
		Interval:  "1m",
		OpenTime:  time.UnixMilli(1_711_929_600_000).UTC(),
		CloseTime: time.UnixMilli(1_711_929_659_999).UTC(),
	}, false)
	if err != nil {
		t.Fatalf("handleForwardKline failed: %v", err)
	}
	if len(store.klines) != 0 {
		t.Fatalf("expected open bar not to hit storage, got %#v", store.klines)
	}
	if len(publisher.klines) != 0 {
		t.Fatalf("expected open bar not to publish, got %#v", publisher.klines)
	}
}

func TestHandleForwardKlineTracksResumeCursorOnSuccess(t *testing.T) {
	store := &fakeStore{}
	publisher := &fakePublisher{}
	kline := models.Kline{
		Symbol:    "BTCUSDT",
		Market:    "spot",
		Exchange:  "binance",
		Interval:  "1m",
		OpenTime:  time.UnixMilli(1_711_929_600_000).UTC(),
		CloseTime: time.UnixMilli(1_711_929_659_999).UTC(),
	}
	sc := &Scraper{
		storage:   store,
		publisher: publisher,
	}

	err := sc.handleForwardKline(context.Background(), kline, true)
	if err != nil {
		t.Fatalf("handleForwardKline failed: %v", err)
	}
	if len(store.klines) != 1 {
		t.Fatalf("expected one storage batch, got %d", len(store.klines))
	}
	if len(publisher.klines) != 1 {
		t.Fatalf("expected one publish batch, got %d", len(publisher.klines))
	}
	if sc.forwardResumeTimeMs != kline.CloseTime.UnixMilli()+1 {
		t.Fatalf("unexpected forward resume cursor: got %d want %d", sc.forwardResumeTimeMs, kline.CloseTime.UnixMilli()+1)
	}
}

func TestHandleForwardKlineFailureDoesNotRewindForLiveReplay(t *testing.T) {
	store := &fakeStore{}
	publisher := &fakePublisher{err: context.DeadlineExceeded}
	kline := models.Kline{
		Symbol:    "BTCUSDT",
		Market:    "spot",
		Exchange:  "binance",
		Interval:  "1m",
		OpenTime:  time.UnixMilli(1_711_929_600_000).UTC(),
		CloseTime: time.UnixMilli(1_711_929_659_999).UTC(),
	}
	sc := &Scraper{
		storage:   store,
		publisher: publisher,
	}

	err := sc.handleForwardKline(context.Background(), kline, true)
	if err == nil {
		t.Fatal("expected publish failure to bubble up")
	}
	if len(store.klines) != 1 {
		t.Fatalf("expected storage write before publish error, got %d", len(store.klines))
	}
	if sc.forwardResumeTimeMs != kline.CloseTime.UnixMilli()+1 {
		t.Fatalf("unexpected resume cursor after publish error: got %d want %d", sc.forwardResumeTimeMs, kline.CloseTime.UnixMilli()+1)
	}
}

func TestStoreCatchUpKlinesDoesNotPublishHistoricalBatch(t *testing.T) {
	store := &fakeStore{}
	publisher := &fakePublisher{}
	first := models.Kline{
		Symbol:    "BTCUSDT",
		Market:    "spot",
		Exchange:  "binance",
		Interval:  "1m",
		OpenTime:  time.UnixMilli(1_711_929_600_000).UTC(),
		CloseTime: time.UnixMilli(1_711_929_659_999).UTC(),
	}
	second := models.Kline{
		Symbol:    "BTCUSDT",
		Market:    "spot",
		Exchange:  "binance",
		Interval:  "1m",
		OpenTime:  time.UnixMilli(1_711_929_660_000).UTC(),
		CloseTime: time.UnixMilli(1_711_929_719_999).UTC(),
	}
	sc := &Scraper{
		storage:   store,
		publisher: publisher,
	}

	if err := sc.storeCatchUpKlines(context.Background(), []models.Kline{first, second}); err != nil {
		t.Fatalf("storeCatchUpKlines failed: %v", err)
	}
	if len(store.klines) != 1 {
		t.Fatalf("expected catch-up batch to be stored, got %d batches", len(store.klines))
	}
	if len(publisher.klines) != 0 {
		t.Fatalf("expected catch-up batch not to publish, got %#v", publisher.klines)
	}
	if sc.forwardResumeTimeMs != second.CloseTime.UnixMilli()+1 {
		t.Fatalf("unexpected catch-up cursor: got %d want %d", sc.forwardResumeTimeMs, second.CloseTime.UnixMilli()+1)
	}
}

func TestStoreKlinesSkipsPublishInReverseMode(t *testing.T) {
	store := &fakeStore{}
	publisher := &fakePublisher{}
	sc := &Scraper{
		storage:   store,
		publisher: publisher,
		direction: scraper.DirectionReverse,
	}

	err := sc.storeKlines(context.Background(), []models.Kline{{
		Symbol:    "BTCUSDT",
		Market:    "spot",
		Exchange:  "binance",
		Interval:  "1m",
		OpenTime:  time.UnixMilli(1_711_929_600_000).UTC(),
		CloseTime: time.UnixMilli(1_711_929_659_999).UTC(),
	}})
	if err != nil {
		t.Fatalf("storeKlines failed: %v", err)
	}
	if len(store.klines) != 1 {
		t.Fatalf("expected reverse mode to keep writing storage, got %d", len(store.klines))
	}
	if len(publisher.klines) != 0 {
		t.Fatalf("expected reverse mode not to publish, got %#v", publisher.klines)
	}
}
