package scraper

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/hushine-tech/golang-lib/middleware/httpclient"
	elog "github.com/hushine-tech/golang-lib/pkg/log"

	"github.com/hushine-tech/scraper/internal/models"
)

type testDoer struct {
	do func(req *http.Request) (*http.Response, error)
}

func (d testDoer) Do(req *http.Request) (*http.Response, error) {
	return d.do(req)
}

type noopExtAPILogger struct{}

func (noopExtAPILogger) ExtAPI(context.Context, elog.ExtAPILogEntry) {}

func TestParseWSOpenInterest(t *testing.T) {
	s := &OpenInterestScraper{exchange: "binance", lastSeen: map[string]struct{}{}}
	msg := []byte(`{"stream":"btcusdt@openinterest","data":{"E":1711929600000,"s":"BTCUSDT","oi":"12345.67"}}`)

	items, err := s.parseWS(msg)
	if err != nil {
		t.Fatalf("parseWS failed: %v", err)
	}
	if len(items) != 1 {
		t.Fatalf("expected 1 item, got %d", len(items))
	}
	item := items[0]
	if item.Symbol != "BTCUSDT" {
		t.Fatalf("unexpected symbol %s", item.Symbol)
	}
	if item.OpenInterest != 12345.67 {
		t.Fatalf("unexpected open interest %f", item.OpenInterest)
	}
	if item.Period != "realtime" {
		t.Fatalf("unexpected period %s", item.Period)
	}
}

func TestSeenDedupesSameKey(t *testing.T) {
	s := &OpenInterestScraper{exchange: "binance", lastSeen: map[string]struct{}{}}
	item := models.OpenInterest{
		Time:         time.UnixMilli(1711929600000).UTC(),
		Symbol:       "BTCUSDT",
		OpenInterest: 10,
		Period:       "realtime",
		Market:       "futures",
		Exchange:     "binance",
	}

	if s.seen(item) {
		t.Fatal("first occurrence should not be deduped")
	}
	if !s.seen(item) {
		t.Fatal("second occurrence should be deduped")
	}
}

func TestFetchHTTPUsesIndependentTimeoutForHTTPDo(t *testing.T) {
	const httpTimeout = 10 * time.Second

	var seenDeadline time.Time
	doer := testDoer{
		do: func(req *http.Request) (*http.Response, error) {
			deadline, ok := req.Context().Deadline()
			if !ok {
				t.Fatal("request context should have a deadline")
			}
			seenDeadline = deadline
			body := `{"symbol":"BTCUSDT","openInterest":"123.45","time":1711929600000}`
			return &http.Response{
				StatusCode: http.StatusOK,
				Body:       io.NopCloser(strings.NewReader(body)),
				Header:     make(http.Header),
			}, nil
		},
	}

	s := &OpenInterestScraper{
		exchange:   "binance",
		httpClient: httpclient.New(doer, noopExtAPILogger{}, "test_open_interest"),
		lastSeen:   map[string]struct{}{},
	}

	start := time.Now()
	item, err := s.fetchHTTP(context.Background(), "BTCUSDT")
	if err != nil {
		t.Fatalf("fetchHTTP failed: %v", err)
	}
	if item.Symbol != "BTCUSDT" {
		t.Fatalf("unexpected symbol %s", item.Symbol)
	}

	// HTTP request should use its own independent 10s timeout.
	if seenDeadline.IsZero() {
		t.Fatal("expected captured request deadline")
	}
	remaining := time.Until(seenDeadline)
	if remaining <= 0 {
		t.Fatalf("deadline already expired, remaining=%s", remaining)
	}
	if remaining < httpTimeout-1*time.Second {
		t.Fatalf("HTTP request should use ~10s timeout, got remaining=%s", remaining)
	}
	if elapsed := time.Since(start); elapsed > 200*time.Millisecond {
		t.Fatalf("fetchHTTP should not block unexpectedly, elapsed=%s", elapsed)
	}
}
