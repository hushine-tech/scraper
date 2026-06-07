package futureskline

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/hushine-tech/golang-lib/middleware/httpclient"
	"github.com/hushine-tech/scraper/internal/logger"
	"github.com/hushine-tech/scraper/internal/marketdata"
	"github.com/hushine-tech/scraper/internal/models"
	"github.com/hushine-tech/scraper/internal/scraper"
	"github.com/hushine-tech/scraper/internal/storage"
)

const (
	restURL                   = "https://fapi.binance.com/fapi/v1/klines"
	forwardInterval           = 5 * time.Second
	maxForwardPublishKlineAge = time.Minute
	maxRetries                = 3
)

type klineSink interface {
	InsertKline(ctx context.Context, klines []models.Kline) error
}

type Scraper struct {
	symbol     string
	exchange   string
	interval   string // K 线周期，如 "1m", "5m"
	storage    klineSink
	publisher  marketdata.Publisher
	direction  scraper.Direction
	httpClient *httpclient.Client
	done       chan struct{}
	observer   scraper.KlineObserver

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
			"futures_kline_rest",
		),
		done: make(chan struct{}),
	}
}

func (s *Scraper) SetObserver(observer scraper.KlineObserver) {
	s.observer = observer
}

// SetReverse 设置为 reverse 模式
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
	ticker := time.NewTicker(forwardInterval)
	defer ticker.Stop()

	var lastCloseTime int64

	for {
		select {
		case <-ctx.Done():
			return
		case <-s.done:
			return
		case <-ticker.C:
			cursorSeeded := lastCloseTime > 0
			klines, err := s.fetch(ctx, lastCloseTime)
			if err != nil {
				continue
			}

			nowMs := time.Now().UnixMilli()
			closed := filterClosedKlines(klines, nowMs)
			if len(closed) == 0 {
				continue
			}

			publish := selectForwardPublishKlines(closed, cursorSeeded, nowMs)
			if len(publish) == 0 {
				s.logForwardPublishSkip(closed, cursorSeeded, nowMs)
			}
			if err := s.storeKlinesWithPublish(ctx, closed, publish); err != nil {
				continue
			}

			lastCloseTime = closed[len(closed)-1].CloseTime.UnixMilli()
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

		klines, err := s.fetch(ctx, currentMs)
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
		time.Sleep(200 * time.Millisecond) // 避免限流
	}

	logger.NewScraperLifecycleHelper().Event("INFO", "futures_kline_reverse_completed", map[string]any{
		"exchange": s.exchange,
		"symbol":   s.symbol,
	})
}

func (s *Scraper) storeKlines(ctx context.Context, klines []models.Kline) error {
	return s.storeKlinesWithPublish(ctx, klines, klines)
}

func (s *Scraper) storeKlinesWithPublish(ctx context.Context, klines []models.Kline, publish []models.Kline) error {
	if len(klines) == 0 {
		return nil
	}
	if err := s.storage.InsertKline(ctx, klines); err != nil {
		return err
	}
	if s.direction == scraper.DirectionReverse || s.publisher == nil || len(publish) == 0 {
		s.notifyStored(klines)
		return nil
	}
	if err := s.publisher.PublishKlines(ctx, publish); err != nil {
		logger.NewScraperLifecycleHelper().Event("ERROR", "futures_kline_live_publish_failed", map[string]any{
			"exchange": s.exchange,
			"symbol":   s.symbol,
			"interval": s.interval,
			"error":    err.Error(),
		})
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

func selectForwardPublishKlines(closed []models.Kline, cursorSeeded bool, nowMs int64) []models.Kline {
	if !cursorSeeded || len(closed) != 1 {
		return nil
	}
	kline := closed[0]
	if nowMs-kline.CloseTime.UnixMilli() > maxForwardPublishKlineAge.Milliseconds() {
		return nil
	}
	return []models.Kline{kline}
}

func (s *Scraper) logForwardPublishSkip(closed []models.Kline, cursorSeeded bool, nowMs int64) {
	if len(closed) == 0 {
		return
	}
	reason := "catchup_batch"
	if !cursorSeeded {
		reason = "bootstrap_batch"
	} else if len(closed) == 1 && nowMs-closed[0].CloseTime.UnixMilli() > maxForwardPublishKlineAge.Milliseconds() {
		reason = "stale_bar"
	}
	logger.NewScraperLifecycleHelper().Event("WARN", "futures_kline_live_publish_skipped", map[string]any{
		"exchange":       s.exchange,
		"symbol":         s.symbol,
		"interval":       s.interval,
		"reason":         reason,
		"rows":           len(closed),
		"first_close_ms": closed[0].CloseTime.UnixMilli(),
		"last_close_ms":  closed[len(closed)-1].CloseTime.UnixMilli(),
	})
}

func (s *Scraper) fetch(ctx context.Context, sinceCloseTime int64) ([]models.Kline, error) {
	endTime := time.Now().UnixMilli()
	q := url.Values{}
	q.Set("symbol", s.symbol)
	q.Set("interval", s.interval)
	q.Set("limit", "100")

	if sinceCloseTime > 0 {
		startTime := sinceCloseTime + 1
		if startTime >= endTime {
			return nil, nil
		}
		q.Set("startTime", strconv.FormatInt(startTime, 10))
		q.Set("endTime", strconv.FormatInt(endTime, 10))
	}

	var lastErr error
	for attempt := 0; attempt < maxRetries; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, restURL+"?"+q.Encode(), nil)
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
			body, _ := io.ReadAll(resp.Body)
			_ = resp.Body.Close()
			return nil, fmt.Errorf("HTTP %s: %s", resp.Status, string(body))
		}

		var rows [][]interface{}
		if err := json.NewDecoder(resp.Body).Decode(&rows); err != nil {
			_ = resp.Body.Close()
			return nil, fmt.Errorf("decode: %w", err)
		}
		_ = resp.Body.Close()

		klines := make([]models.Kline, 0, len(rows))
		for _, row := range rows {
			k, err := s.parseRow(row)
			if err != nil {
				continue
			}
			klines = append(klines, k)
		}

		return klines, nil
	}

	return nil, fmt.Errorf("failed after %d retries: %w", maxRetries, lastErr)
}

func (s *Scraper) parseRow(row []interface{}) (models.Kline, error) {
	if len(row) < 12 {
		return models.Kline{}, fmt.Errorf("short row")
	}

	openMs, err := toInt64(row[0])
	if err != nil {
		return models.Kline{}, fmt.Errorf("parse open_time: %w", err)
	}
	closeMs, err := toInt64(row[6])
	if err != nil {
		return models.Kline{}, fmt.Errorf("parse close_time: %w", err)
	}

	return models.Kline{
		Time:        time.UnixMilli(openMs).UTC(),
		Symbol:      strings.ToUpper(s.symbol),
		Market:      "futures",
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
	}, nil
}

func (s *Scraper) Stop() {
	select {
	case <-s.done:
		return
	default:
		close(s.done)
	}
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
