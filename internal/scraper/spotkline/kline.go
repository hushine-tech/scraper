package spotkline

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/hushine-tech/scraper/internal/logger"
	"github.com/hushine-tech/scraper/internal/marketdata"
	"github.com/hushine-tech/scraper/internal/models"
	"github.com/hushine-tech/scraper/internal/scraper"
	"github.com/hushine-tech/scraper/internal/storage"
	"github.com/hushine-tech/golang-lib/middleware/httpclient"
	"github.com/hushine-tech/golang-lib/middleware/wsclient"

	"github.com/gorilla/websocket"
)

const (
	restURL    = "https://api.binance.com/api/v3/klines"
	wsURL      = "wss://stream.binance.com:9443/ws"
	wsReadWait = 90 * time.Second
	maxRetries = 3
)

type klineSink interface {
	InsertKline(ctx context.Context, klines []models.Kline) error
}

type Scraper struct {
	symbol              string
	exchange            string
	interval            string // K 线周期，如 "1m", "5m"
	storage             klineSink
	publisher           marketdata.Publisher
	direction           scraper.Direction
	httpClient          *httpclient.Client
	wsClient            *wsclient.Client
	done                chan struct{}
	mu                  sync.Mutex
	observer            scraper.KlineObserver
	forwardResumeTimeMs int64

	// reverse 模式参数
	startTime time.Time
	endTime   time.Time
}

func NewScraper(symbol, exchange string, store *storage.TimescaleDB) *Scraper {
	return NewScraperWithInterval(symbol, exchange, "1m", store)
}

func NewScraperWithInterval(symbol, exchange, interval string, store *storage.TimescaleDB) *Scraper {
	return NewScraperWithIntervalAndPublisher(symbol, exchange, interval, store, nil)
}

func NewScraperWithPublisher(symbol, exchange string, store *storage.TimescaleDB, publisher marketdata.Publisher) *Scraper {
	return NewScraperWithIntervalAndPublisher(symbol, exchange, "1m", store, publisher)
}

func NewScraperWithIntervalAndPublisher(symbol, exchange, interval string, store *storage.TimescaleDB, publisher marketdata.Publisher) *Scraper {
	if interval == "" {
		interval = "1m"
	}
	return &Scraper{
		symbol:    symbol,
		exchange:  exchange,
		interval:  interval,
		storage:   store,
		publisher: publisher,
		httpClient: httpclient.New(
			&http.Client{Timeout: 30 * time.Second},
			logger.NewExtAPIAdapter(),
			"spot_kline_rest",
		),
		wsClient: wsclient.New(websocket.DefaultDialer, logger.NewWebSocketAdapter()),
		done:     make(chan struct{}),
	}
}

func (s *Scraper) SetObserver(observer scraper.KlineObserver) {
	s.observer = observer
}

func (s *Scraper) SetReverse(startTime, endTime time.Time) {
	s.direction = scraper.DirectionReverse
	s.startTime = startTime
	s.endTime = endTime
}

func (s *Scraper) Run(ctx context.Context) {
	s.run(ctx)
}

func (s *Scraper) run(ctx context.Context) {
	if s.direction == scraper.DirectionReverse {
		s.reverse(ctx)
		return
	}
	s.forward(ctx)
}

func (s *Scraper) forward(ctx context.Context) {
	url := fmt.Sprintf("%s/%s@kline_%s", wsURL, s.symbol, s.interval)
	for {
		select {
		case <-ctx.Done():
			return
		case <-s.done:
			return
		default:
		}

		if err := s.catchUpForward(ctx); err != nil {
			logger.NewScraperLifecycleHelper().Event("ERROR", "spot_kline_catchup_failed", map[string]any{
				"exchange": s.exchange,
				"symbol":   s.symbol,
				"interval": s.interval,
				"error":    err.Error(),
			})
			select {
			case <-ctx.Done():
				return
			case <-s.done:
				return
			case <-time.After(2 * time.Second):
			}
			continue
		}

		err := s.dialAndRead(ctx, url)
		if err == nil {
			return
		}
		if isClosed(err) {
			return
		}
		logger.NewScraperLifecycleHelper().Event("ERROR", "spot_kline_forward_failed", map[string]any{
			"exchange": s.exchange,
			"symbol":   s.symbol,
			"interval": s.interval,
			"error":    err.Error(),
		})
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

		kline, isClosed, err := s.parseMsg(msg)
		if err != nil {
			continue
		}
		if err := s.handleForwardKline(ctx, kline, isClosed); err != nil {
			return fmt.Errorf("store forward kline: %w", err)
		}
	}
}

func (s *Scraper) reverse(ctx context.Context) {
	startMs := s.startTime.UnixMilli()
	endMs := s.endTime.UnixMilli()
	if endMs == 0 {
		endMs = time.Now().UnixMilli()
	}

	currentMs := startMs
	for currentMs < endMs {
		select {
		case <-ctx.Done():
			return
		case <-s.done:
			return
		default:
		}

		klines, err := s.fetchBatch(ctx, restURL, currentMs, endMs)
		if err != nil {
			time.Sleep(2 * time.Second)
			continue
		}

		if len(klines) == 0 {
			break
		}

		if err := s.storeKlines(ctx, klines); err != nil {
			continue
		}

		currentMs = klines[len(klines)-1].CloseTime.UnixMilli() + 1
		time.Sleep(200 * time.Millisecond)
	}

	logger.NewScraperLifecycleHelper().Event("INFO", "spot_kline_reverse_completed", map[string]any{
		"exchange": s.exchange,
		"symbol":   s.symbol,
	})
}

func (s *Scraper) handleForwardKline(ctx context.Context, kline models.Kline, isClosed bool) error {
	if !isClosed {
		return nil
	}
	return s.storeForwardKlines(ctx, []models.Kline{kline})
}

func (s *Scraper) catchUpForward(ctx context.Context) error {
	if s.forwardResumeTimeMs <= 0 {
		return nil
	}

	currentMs := s.forwardResumeTimeMs
	for {
		nowMs := time.Now().UnixMilli()
		if currentMs >= nowMs {
			return nil
		}

		klines, err := s.fetchBatch(ctx, restURL, currentMs, nowMs)
		if err != nil {
			return err
		}
		if len(klines) == 0 {
			return nil
		}

		closed := filterClosedKlines(klines, nowMs)
		if len(closed) == 0 {
			return nil
		}
		if err := s.storeForwardKlines(ctx, closed); err != nil {
			return err
		}

		nextMs := s.forwardResumeTimeMs
		if nextMs <= currentMs || len(closed) < len(klines) {
			return nil
		}
		currentMs = nextMs
	}
}

func (s *Scraper) storeForwardKlines(ctx context.Context, klines []models.Kline) error {
	if len(klines) == 0 {
		return nil
	}
	if err := s.storeKlines(ctx, klines); err != nil {
		if s.forwardResumeTimeMs == 0 || klines[0].OpenTime.UnixMilli() < s.forwardResumeTimeMs {
			s.forwardResumeTimeMs = klines[0].OpenTime.UnixMilli()
		}
		return err
	}
	s.forwardResumeTimeMs = klines[len(klines)-1].CloseTime.UnixMilli() + 1
	return nil
}

func (s *Scraper) storeKlines(ctx context.Context, klines []models.Kline) error {
	if len(klines) == 0 {
		return nil
	}
	if err := s.storage.InsertKline(ctx, klines); err != nil {
		return err
	}
	if s.direction == scraper.DirectionReverse || s.publisher == nil {
		s.notifyStored(klines)
		return nil
	}
	if err := s.publisher.PublishKlines(ctx, klines); err != nil {
		return err
	}
	s.notifyStored(klines)
	return nil
}

func (s *Scraper) notifyStored(klines []models.Kline) {
	if s.observer == nil || len(klines) == 0 {
		return
	}
	s.observer.OnStored(klines[len(klines)-1])
}

func filterClosedKlines(klines []models.Kline, nowMs int64) []models.Kline {
	filtered := make([]models.Kline, 0, len(klines))
	for _, kline := range klines {
		if kline.CloseTime.UnixMilli() > nowMs {
			continue
		}
		filtered = append(filtered, kline)
	}
	return filtered
}

func (s *Scraper) fetchBatch(ctx context.Context, baseURL string, startMs, endMs int64) ([]models.Kline, error) {
	q := url.Values{}
	q.Set("symbol", s.symbol)
	q.Set("interval", s.interval)
	q.Set("startTime", strconv.FormatInt(startMs, 10))
	q.Set("endTime", strconv.FormatInt(endMs, 10))
	q.Set("limit", "100")

	var lastErr error

	for attempt := 0; attempt < maxRetries; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, baseURL+"?"+q.Encode(), nil)
		if err != nil {
			return nil, err
		}

		resp, err := s.httpClient.Do(ctx, req)
		if err != nil {
			lastErr = err
			time.Sleep(time.Duration(attempt+1) * time.Second)
			continue
		}

		if resp.StatusCode == http.StatusTooManyRequests {
			_ = resp.Body.Close()
			time.Sleep(time.Duration(attempt+1) * time.Second)
			continue
		}

		if resp.StatusCode != http.StatusOK {
			_ = resp.Body.Close()
			return nil, fmt.Errorf("HTTP %s", resp.Status)
		}

		var rows [][]interface{}
		if err := json.NewDecoder(resp.Body).Decode(&rows); err != nil {
			_ = resp.Body.Close()
			return nil, fmt.Errorf("decode: %w", err)
		}
		_ = resp.Body.Close()

		klines := make([]models.Kline, 0, len(rows))
		for _, row := range rows {
			if len(row) < 12 {
				continue
			}
			openMs, _ := toInt64(row[0])
			closeMs, _ := toInt64(row[6])
			klines = append(klines, models.Kline{
				Time:        time.UnixMilli(openMs).UTC(),
				Symbol:      strings.ToUpper(s.symbol),
				Market:      "spot",
				Exchange:    s.exchange,
				Interval:    s.interval,
				OpenTime:    time.UnixMilli(openMs).UTC(),
				CloseTime:   time.UnixMilli(closeMs).UTC(),
				Open:        toFloat(row[1]),
				High:        toFloat(row[2]),
				Low:         toFloat(row[3]),
				Close:       toFloat(row[4]),
				Volume:      toFloat(row[5]),
				QuoteVolume: toFloat(row[7]),
				NumTrades:   toInt64Safe(row[8]),
			})
		}
		return klines, nil
	}

	return nil, fmt.Errorf("failed after %d retries: %w", maxRetries, lastErr)
}

func (s *Scraper) parseMsg(msg []byte) (models.Kline, bool, error) {
	var raw map[string]interface{}
	if err := json.Unmarshal(msg, &raw); err != nil {
		return models.Kline{}, false, fmt.Errorf("unmarshal: %w", err)
	}

	k, ok := raw["k"].(map[string]interface{})
	if !ok {
		return models.Kline{}, false, fmt.Errorf("no k field")
	}

	openMs, _ := toInt64(k["t"])
	closeMs, _ := toInt64(k["T"])
	isClosed, _ := k["x"].(bool)

	return models.Kline{
		Time:        time.UnixMilli(openMs).UTC(),
		Symbol:      strings.ToUpper(s.symbol),
		Market:      "spot",
		Exchange:    s.exchange,
		Interval:    s.interval,
		OpenTime:    time.UnixMilli(openMs).UTC(),
		CloseTime:   time.UnixMilli(closeMs).UTC(),
		Open:        toFloat(k["o"]),
		High:        toFloat(k["h"]),
		Low:         toFloat(k["l"]),
		Close:       toFloat(k["c"]),
		Volume:      toFloat(k["v"]),
		QuoteVolume: toFloat(k["q"]),
		NumTrades:   toInt64Safe(k["n"]),
	}, isClosed, nil
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

func toInt64(v interface{}) (int64, error) {
	switch x := v.(type) {
	case float64:
		return int64(x), nil
	case json.Number:
		return x.Int64()
	case string:
		return strconv.ParseInt(x, 10, 64)
	default:
		return 0, fmt.Errorf("unsupported type %T", v)
	}
}

func toFloat(v interface{}) float64 {
	switch x := v.(type) {
	case float64:
		return x
	case json.Number:
		f, _ := x.Float64()
		return f
	case string:
		f, _ := strconv.ParseFloat(x, 64)
		return f
	default:
		return 0
	}
}

func toInt64Safe(v interface{}) int64 {
	n, _ := toInt64(v)
	return n
}
