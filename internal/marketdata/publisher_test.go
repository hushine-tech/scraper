package marketdata

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/hushine-tech/scraper/internal/models"
)

type captureSender struct {
	topics []string
	keys   []string
	values [][]byte
}

func (s *captureSender) Send(_ context.Context, topic string, key []byte, value []byte) error {
	s.topics = append(s.topics, topic)
	s.keys = append(s.keys, string(key))
	s.values = append(s.values, append([]byte(nil), value...))
	return nil
}

func TestKlineTopicUsesCanonicalFamily(t *testing.T) {
	topic := KlineTopic("Binance", "Futures", "1m")
	if topic != "md.kline.binance.futures.1m" {
		t.Fatalf("unexpected topic: %q", topic)
	}
}

func TestPublishKlinesUsesCanonicalTopicAndCloseTimeTimestamp(t *testing.T) {
	sender := &captureSender{}
	pub := &KafkaPublisher{sender: sender}
	openTime := time.UnixMilli(1711929600000).UTC()
	closeTime := openTime.Add(time.Minute).Add(-time.Millisecond)

	err := pub.PublishKlines(context.Background(), []models.Kline{
		{
			Symbol:    "BTCUSDT",
			Market:    "futures",
			Exchange:  "binance",
			Interval:  "1m",
			OpenTime:  openTime,
			CloseTime: closeTime,
			Open:      100,
			High:      101,
			Low:       99,
			Close:     100.5,
			Volume:    12.3,
		},
	})
	if err != nil {
		t.Fatalf("PublishKlines failed: %v", err)
	}
	if len(sender.topics) != 1 || sender.topics[0] != "md.kline.binance.futures.1m" {
		t.Fatalf("unexpected topics: %#v", sender.topics)
	}
	if sender.keys[0] != "BTCUSDT" {
		t.Fatalf("unexpected key: %q", sender.keys[0])
	}

	var payload map[string]any
	if err := json.Unmarshal(sender.values[0], &payload); err != nil {
		t.Fatalf("unmarshal payload: %v", err)
	}
	if payload["timestamp"] != float64(closeTime.UnixMilli()) {
		t.Fatalf("expected timestamp to equal close_time, got %v", payload["timestamp"])
	}
	if payload["market"] != "futures" {
		t.Fatalf("expected market to be preserved, got %v", payload["market"])
	}
}
