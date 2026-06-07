package futureskline

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
}

func (p *fakePublisher) PublishKlines(_ context.Context, klines []models.Kline) error {
	p.klines = append(p.klines, append([]models.Kline(nil), klines...))
	return nil
}

func (p *fakePublisher) Close() error { return nil }

func TestFilterClosedKlinesDropsOpenTail(t *testing.T) {
	now := time.UnixMilli(1_711_929_630_000).UTC()
	closed := models.Kline{CloseTime: now.Add(-time.Millisecond)}
	open := models.Kline{CloseTime: now.Add(20 * time.Second)}

	filtered := filterClosedKlines([]models.Kline{closed, open}, now.UnixMilli())
	if len(filtered) != 1 {
		t.Fatalf("expected only the closed bar, got %d items", len(filtered))
	}
	if filtered[0].CloseTime != closed.CloseTime {
		t.Fatalf("unexpected filtered kline: %#v", filtered[0])
	}
}

func TestSelectForwardPublishKlinesDoesNotPublishBootstrapBatch(t *testing.T) {
	now := time.UnixMilli(1_711_929_720_000).UTC()
	closed := []models.Kline{
		{CloseTime: now.Add(-2 * time.Minute)},
		{CloseTime: now.Add(-time.Second)},
	}

	publish := selectForwardPublishKlines(closed, false, now.UnixMilli())

	if len(publish) != 0 {
		t.Fatalf("expected bootstrap REST batch not to publish live data, got %#v", publish)
	}
}

func TestSelectForwardPublishKlinesOnlyPublishesSingleFreshBar(t *testing.T) {
	now := time.UnixMilli(1_711_929_720_000).UTC()
	fresh := models.Kline{CloseTime: now.Add(-30 * time.Second)}
	old := models.Kline{CloseTime: now.Add(-2 * time.Minute)}

	if publish := selectForwardPublishKlines([]models.Kline{fresh}, true, now.UnixMilli()); len(publish) != 1 || publish[0].CloseTime != fresh.CloseTime {
		t.Fatalf("expected one fresh live bar to publish, got %#v", publish)
	}
	if publish := selectForwardPublishKlines([]models.Kline{old}, true, now.UnixMilli()); len(publish) != 0 {
		t.Fatalf("expected stale live bar not to publish, got %#v", publish)
	}
	if publish := selectForwardPublishKlines([]models.Kline{old, fresh}, true, now.UnixMilli()); len(publish) != 0 {
		t.Fatalf("expected catch-up batch not to publish, got %#v", publish)
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
		Market:    "futures",
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
