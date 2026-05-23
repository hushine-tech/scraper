package scraper

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/hushine-tech/scraper/internal/logger"
	"github.com/hushine-tech/scraper/internal/models"
	base "github.com/hushine-tech/scraper/internal/scraper"
	"github.com/hushine-tech/scraper/internal/storage"
	"github.com/gorilla/websocket"
	"github.com/hushine-tech/golang-lib/middleware/httpclient"
	"github.com/hushine-tech/golang-lib/middleware/wsclient"
)

const (
	openInterestWSBaseURL        = "wss://fstream.binance.com/stream?streams="
	openInterestRESTURL          = "https://fapi.binance.com/fapi/v1/openInterest"
	openInterestHistRESTURL      = "https://fapi.binance.com/fapi/v1/futures/data/openInterestHist"
	openInterestWSConnectTimeout = 5 * time.Second
	pollInterval                 = 5 * time.Second
	interSymbolDelay             = 300 * time.Millisecond
)

type OpenInterestScraper struct {
	symbols   []string
	exchange  string
	storage   *storage.TimescaleDB
	direction base.Direction
	startTime time.Time
	endTime   time.Time
	done      chan struct{}
	mu        sync.Mutex

	httpClient *httpclient.Client
	wsClient   *wsclient.Client

	lastSeen map[string]struct{}
}

func NewOpenInterestScraper(symbols []string, exchange string, store *storage.TimescaleDB) *OpenInterestScraper {
	return &OpenInterestScraper{
		symbols:  symbols,
		exchange: exchange,
		storage:  store,
		done:     make(chan struct{}),
		httpClient: httpclient.New(
			&http.Client{Timeout: 15 * time.Second},
			logger.NewExtAPIAdapter(),
			"futures_open_interest_rest",
		),
		wsClient: wsclient.New(websocket.DefaultDialer, logger.NewWebSocketAdapter()),
		lastSeen: make(map[string]struct{}),
	}
}

func (s *OpenInterestScraper) SetReverse(start, end time.Time) {
	s.direction = base.DirectionReverse
	s.startTime = start
	s.endTime = end
}

func (s *OpenInterestScraper) Run(ctx context.Context) {
	if s.direction == base.DirectionReverse {
		s.reverse(ctx)
		return
	}
	s.forward(ctx)
}

func (s *OpenInterestScraper) forward(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-s.done:
			return
		default:
		}

		// Try HTTP first.
		if err := s.pollAllHTTP(ctx, 5*time.Second); err != nil {
			// HTTP failed, try WS as fallback.
			wsErr := s.runWS(ctx)
			if wsErr != nil {
				select {
				case <-ctx.Done():
					return
				case <-s.done:
					return
				default:
				}
				logger.NewExtAPIHelper().Event("WARN", "open_interest_both_failed", map[string]any{
					"exchange":   s.exchange,
					"http_error": err.Error(),
					"ws_error":   wsErr.Error(),
				})
			} else {
				return
			}
		}
		select {
		case <-ctx.Done():
			return
		case <-s.done:
			return
		case <-time.After(pollInterval):
		}
	}
}

func (s *OpenInterestScraper) runWS(ctx context.Context) error {
	// Build combined stream URL for all symbols
	streams := make([]string, len(s.symbols))
	for i, sym := range s.symbols {
		streams[i] = strings.ToLower(sym) + "_openinterest"
	}
	wsURL := openInterestWSBaseURL + strings.Join(streams, "/")

	connectCtx, cancel := context.WithTimeout(ctx, openInterestWSConnectTimeout)
	defer cancel()

	s.mu.Lock()
	err := s.wsClient.Connect(connectCtx, wsURL, nil)
	s.mu.Unlock()
	if err != nil {
		return fmt.Errorf("connect ws: %w", err)
	}
	defer func() {
		s.mu.Lock()
		_ = s.wsClient.Close(ctx)
		s.mu.Unlock()
	}()

	for {
		_, msg, err := s.wsClient.Recv(ctx, "recv")
		if err != nil {
			return err
		}
		items, err := s.parseWS(msg)
		if err != nil {
			continue
		}
		for _, item := range items {
			if s.seen(item) {
				continue
			}
			if err := s.storage.InsertOpenInterest(ctx, item); err != nil {
				logger.NewExtAPIHelper().Event("ERROR", "open_interest_insert_failed", map[string]any{
					"exchange": s.exchange,
					"symbol":   item.Symbol,
					"source":   "ws",
					"error":    err.Error(),
				})
			}
		}
	}
}

func (s *OpenInterestScraper) pollAllHTTP(ctx context.Context, timeout time.Duration) error {
	pollCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	anySuccess := false
	for i, symbol := range s.symbols {
		if i > 0 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-s.done:
				return nil
			case <-time.After(interSymbolDelay):
			}
		}

		item, err := s.fetchHTTP(pollCtx, symbol)
		if err != nil {
			logger.NewExtAPIHelper().Event("WARN", "open_interest_http_failed", map[string]any{
				"exchange": s.exchange,
				"symbol":   symbol,
				"error":    err.Error(),
			})
			continue
		}
		if s.seen(item) {
			anySuccess = true
			continue
		}
		if err := s.storage.InsertOpenInterest(ctx, item); err != nil {
			logger.NewExtAPIHelper().Event("ERROR", "open_interest_insert_failed", map[string]any{
				"exchange": s.exchange,
				"symbol":   symbol,
				"source":   "http",
				"error":    err.Error(),
			})
			continue
		}
		anySuccess = true
	}

	if !anySuccess && len(s.symbols) > 0 {
		return fmt.Errorf("all symbol OI HTTP requests failed")
	}
	return nil
}

func (s *OpenInterestScraper) reverse(ctx context.Context) {
	for _, symbol := range s.symbols {
		if err := s.backfillSymbol(ctx, symbol); err != nil {
			logger.NewExtAPIHelper().Event("ERROR", "open_interest_backfill_failed", map[string]any{
				"exchange": s.exchange,
				"symbol":   symbol,
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
	}
}

func (s *OpenInterestScraper) backfillSymbol(ctx context.Context, symbol string) error {
	start := s.startTime
	if start.IsZero() {
		start = time.Now().UTC().AddDate(0, -1, 0)
	}
	end := s.endTime
	if end.IsZero() {
		end = time.Now().UTC()
	}
	cursor := end
	for cursor.After(start) {
		select {
		case <-ctx.Done():
			return nil
		case <-s.done:
			return nil
		default:
		}

		items, err := s.fetchHistory(ctx, symbol, cursor)
		if err != nil {
			logger.NewExtAPIHelper().Event("ERROR", "open_interest_history_fetch_failed", map[string]any{
				"exchange": s.exchange,
				"symbol":   symbol,
				"error":    err.Error(),
			})
			select {
			case <-ctx.Done():
				return nil
			case <-s.done:
				return nil
			case <-time.After(2 * time.Second):
			}
			continue
		}
		if len(items) == 0 {
			break
		}
		filtered := make([]models.OpenInterest, 0, len(items))
		for _, item := range items {
			if item.Time.Before(start) || item.Time.After(end) {
				continue
			}
			if s.seen(item) {
				continue
			}
			filtered = append(filtered, item)
		}
		if err := s.storage.InsertOpenInterests(ctx, filtered); err != nil {
			logger.NewExtAPIHelper().Event("ERROR", "open_interest_history_insert_failed", map[string]any{
				"exchange": s.exchange,
				"symbol":   symbol,
				"error":    err.Error(),
			})
		}
		cursor = items[0].Time.Add(-time.Millisecond)
	}
	return nil
}

func (s *OpenInterestScraper) fetchHTTP(ctx context.Context, symbol string) (models.OpenInterest, error) {
	httpCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	q := url.Values{}
	q.Set("symbol", strings.ToUpper(symbol))
	req, err := http.NewRequestWithContext(httpCtx, http.MethodGet, openInterestRESTURL+"?"+q.Encode(), nil)
	if err != nil {
		return models.OpenInterest{}, err
	}
	resp, err := s.httpClient.Do(httpCtx, req)
	if err != nil {
		return models.OpenInterest{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return models.OpenInterest{}, fmt.Errorf("HTTP %s: %s", resp.Status, string(body))
	}
	var raw struct {
		Symbol       string `json:"symbol"`
		OpenInterest string `json:"openInterest"`
		Time         int64  `json:"time"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&raw); err != nil {
		return models.OpenInterest{}, err
	}
	val, err := strconv.ParseFloat(raw.OpenInterest, 64)
	if err != nil {
		return models.OpenInterest{}, err
	}
	t := time.Now().UTC()
	if raw.Time > 0 {
		t = time.UnixMilli(raw.Time).UTC()
	}
	return models.OpenInterest{
		Time:         t,
		Symbol:       strings.ToUpper(raw.Symbol),
		OpenInterest: val,
		Period:       "realtime",
		Market:       "futures",
		Exchange:     s.exchange,
	}, nil
}

func (s *OpenInterestScraper) fetchHistory(ctx context.Context, symbol string, end time.Time) ([]models.OpenInterest, error) {
	q := url.Values{}
	q.Set("symbol", strings.ToUpper(symbol))
	q.Set("periodType", "4h")
	q.Set("limit", "50")
	q.Set("endTime", strconv.FormatInt(end.UnixMilli(), 10))
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, openInterestHistRESTURL+"?"+q.Encode(), nil)
	if err != nil {
		return nil, err
	}
	resp, err := s.httpClient.Do(ctx, req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("HTTP %s: %s", resp.Status, string(body))
	}
	var raw []struct {
		Symbol          string `json:"symbol"`
		SumOpenInterest string `json:"sumOpenInterest"`
		Timestamp       int64  `json:"timestamp"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&raw); err != nil {
		return nil, err
	}
	items := make([]models.OpenInterest, 0, len(raw))
	for _, row := range raw {
		oi, err := strconv.ParseFloat(row.SumOpenInterest, 64)
		if err != nil {
			continue
		}
		items = append(items, models.OpenInterest{
			Time:         time.UnixMilli(row.Timestamp).UTC(),
			Symbol:       strings.ToUpper(row.Symbol),
			OpenInterest: oi,
			Period:       "4h",
			Market:       "futures",
			Exchange:     s.exchange,
		})
	}
	return items, nil
}

// parseWS handles Binance combined stream format: {"stream":"btcusdt@openinterest","data":{...}}
func (s *OpenInterestScraper) parseWS(msg []byte) ([]models.OpenInterest, error) {
	var streamMsg struct {
		Stream string `json:"stream"`
		Data   struct {
			EventTime    int64  `json:"E"`
			Symbol       string `json:"s"`
			OpenInterest string `json:"oi"`
		} `json:"data"`
	}
	if err := json.Unmarshal(msg, &streamMsg); err != nil {
		return nil, err
	}

	symbol := strings.ToUpper(streamMsg.Data.Symbol)
	oi, err := strconv.ParseFloat(streamMsg.Data.OpenInterest, 64)
	if err != nil {
		return nil, err
	}

	item := models.OpenInterest{
		Time:         time.UnixMilli(streamMsg.Data.EventTime).UTC(),
		Symbol:       symbol,
		OpenInterest: oi,
		Period:       "realtime",
		Market:       "futures",
		Exchange:     s.exchange,
	}
	return []models.OpenInterest{item}, nil
}

func (s *OpenInterestScraper) seen(item models.OpenInterest) bool {
	key := fmt.Sprintf("%d:%s:%s", item.Time.UnixMilli(), item.Symbol, item.Period)
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.lastSeen[key]; ok {
		return true
	}
	s.lastSeen[key] = struct{}{}
	// 不清空 map：symbol 数量有界，map 大小由 (symbol数 × 时间周期数) 自然限定。
	return false
}

func (s *OpenInterestScraper) Stop() {
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
