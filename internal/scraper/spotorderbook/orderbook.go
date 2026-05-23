package spotorderbook

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/hushine-tech/scraper/internal/logger"
	"github.com/hushine-tech/scraper/internal/models"
	"github.com/hushine-tech/scraper/internal/scraper"
	"github.com/hushine-tech/scraper/internal/storage"
	"github.com/hushine-tech/golang-lib/middleware/wsclient"

	"github.com/gorilla/websocket"
)

const (
	wsURL           = "wss://stream.binance.com:9443/ws"
	wsReadWait      = 90 * time.Second
	bookInterval    = 500 * time.Millisecond
)

type Scraper struct {
	symbol   string
	market   string
	exchange string
	storage  *storage.TimescaleDB
	wsClient *wsclient.Client
	done     chan struct{}
	mu       sync.Mutex
}

func NewScraper(symbol, market, exchange string, store *storage.TimescaleDB) scraper.Scraper {
	return &Scraper{
		symbol:   symbol,
		market:   market,
		exchange: exchange,
		storage:  store,
		wsClient: wsclient.New(websocket.DefaultDialer, logger.NewWebSocketAdapter()),
		done:     make(chan struct{}),
	}
}

func (s *Scraper) Run(ctx context.Context) {
	s.run(ctx)
}

func (s *Scraper) run(ctx context.Context) {
	url := fmt.Sprintf("%s/%s@depth20@100ms", wsURL, s.symbol)
	for {
		select {
		case <-ctx.Done():
			return
		case <-s.done:
			return
		default:
		}

		err := s.dialAndRead(ctx, url)
		if err == nil {
			return
		}
		if isClosed(err) {
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-s.done:
			return
		case <-time.After(2 * time.Second):
		}
	}
}

func (s *Scraper) dialAndRead(ctx context.Context, url string) error {
	s.mu.Lock()
	err := s.wsClient.Connect(ctx, url, nil)
	if err != nil {
		s.mu.Unlock()
		return fmt.Errorf("dial: %w", err)
	}
	s.mu.Unlock()

	s.wsClient.SetPongHandler(func(string) error {
		return s.wsClient.SetReadDeadline(time.Now().Add(wsReadWait))
	})

	defer func() {
		s.mu.Lock()
		_ = s.wsClient.Close(ctx)
		s.mu.Unlock()
	}()

	var lastBucket time.Time
	var latestBids, latestAsks [][]float64

	for {
		_ = s.wsClient.SetReadDeadline(time.Now().Add(wsReadWait))
		_, msg, err := s.wsClient.Recv(ctx, "recv")
		if err != nil {
			select {
			case <-s.done:
				return nil
			default:
			}
			return fmt.Errorf("read: %w", err)
		}

		var raw map[string]interface{}
		if err := json.Unmarshal(msg, &raw); err != nil {
			continue
		}

		bids, asks, ok := parseMsg(raw)
		if !ok {
			continue
		}

		bucket := time.Now().Truncate(bookInterval)
		if bucket.After(lastBucket) {
			if !lastBucket.IsZero() && len(latestBids) > 0 {
				ob := models.OrderBook{
					Time:     lastBucket,
					Symbol:   strings.ToUpper(s.symbol),
					Market:   s.market,
					Exchange: s.exchange,
					Bids:     mustMarshalJSON(latestBids),
					Asks:     mustMarshalJSON(latestAsks),
				}
				if err := s.storage.InsertOrderBook(ctx, ob); err != nil {
					logger.NewScraperLifecycleHelper().Event("ERROR", "spot_orderbook_insert_failed", map[string]any{
						"exchange": s.exchange,
						"symbol":   s.symbol,
						"error":    err.Error(),
					})
				}
			}
			lastBucket = bucket
			latestBids = bids
			latestAsks = asks
		}
	}
}

// parseMsg extracts bid/ask depth rows from a Binance-style depth JSON object.
// It prefers short keys "b" / "a", then falls back to "bids" / "asks".
func parseMsg(raw map[string]interface{}) (bids, asks [][]float64, ok bool) {
	bidsRaw, haveB := raw["b"].([]interface{})
	if !haveB {
		bidsRaw, haveB = raw["bids"].([]interface{})
	}
	asksRaw, haveA := raw["a"].([]interface{})
	if !haveA {
		asksRaw, haveA = raw["asks"].([]interface{})
	}
	if !haveB || !haveA {
		return nil, nil, false
	}
	return parseDepthSide(bidsRaw), parseDepthSide(asksRaw), true
}

func parseDepthSide(rows []interface{}) [][]float64 {
	out := make([][]float64, 0, len(rows))
	for _, v := range rows {
		row, ok := v.([]interface{})
		if !ok || len(row) < 2 {
			continue
		}
		price, _ := toFloat(row[0])
		qty, _ := toFloat(row[1])
		out = append(out, []float64{price, qty})
	}
	return out
}

func (s *Scraper) Stop() {
	select {
	case <-s.done:
		return
	default:
		close(s.done)
	}
	s.mu.Lock()
	_ = s.wsClient.Close(context.Background())
	s.mu.Unlock()
}

func isClosed(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.Canceled) {
		return true
	}
	if errors.Is(err, net.ErrClosed) {
		return true
	}
	return websocket.IsCloseError(err, websocket.CloseGoingAway, websocket.CloseNormalClosure)
}

func toFloat(v interface{}) (float64, bool) {
	switch x := v.(type) {
	case float64:
		return x, true
	case json.Number:
		f, _ := x.Float64()
		return f, true
	case string:
		f, _ := strconv.ParseFloat(x, 64)
		return f, true
	default:
		return 0, false
	}
}

func mustMarshalJSON(v interface{}) []byte {
	b, _ := json.Marshal(v)
	return b
}
