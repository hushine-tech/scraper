package futuresorderbook

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
	"github.com/hushine-tech/scraper/internal/models"
	"github.com/hushine-tech/scraper/internal/scraper"
	"github.com/hushine-tech/scraper/internal/storage"
)

const (
	restURL       = "https://fapi.binance.com/fapi/v1/depth"
	pollInterval  = 1 * time.Second
	maxRetries   = 3
)

type Scraper struct {
	symbol   string
	exchange string
	storage  *storage.TimescaleDB
	httpClient *httpclient.Client
	done     chan struct{}
}

func NewScraper(symbol, exchange string, store *storage.TimescaleDB) scraper.Scraper {
	return &Scraper{
		symbol:   symbol,
		exchange: exchange,
		storage:  store,
		httpClient: httpclient.New(
			&http.Client{Timeout: 10 * time.Second},
			logger.NewExtAPIAdapter(),
			"futures_orderbook_rest",
		),
		done: make(chan struct{}),
	}
}

func (s *Scraper) Run(ctx context.Context) {
	s.run(ctx)
}

func (s *Scraper) run(ctx context.Context) {
	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-s.done:
			return
		case <-ticker.C:
			ob, err := s.fetch(ctx)
			if err != nil {
				continue
			}
			if err := s.storage.InsertOrderBook(ctx, ob); err != nil {
				logger.NewScraperLifecycleHelper().Event("ERROR", "futures_orderbook_insert_failed", map[string]any{
					"exchange": s.exchange,
					"symbol":   s.symbol,
					"error":    err.Error(),
				})
			}
		}
	}
}

func (s *Scraper) fetch(ctx context.Context) (models.OrderBook, error) {
	q := url.Values{}
	q.Set("symbol", s.symbol)
	q.Set("limit", "20")

	var lastErr error
	for attempt := 0; attempt < maxRetries; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, restURL+"?"+q.Encode(), nil)
		if err != nil {
			return models.OrderBook{}, err
		}

		resp, err := s.httpClient.Do(ctx, req)
		if err != nil {
			lastErr = err
			continue
		}

		if resp.StatusCode == http.StatusTooManyRequests {
			_, _ = io.Copy(io.Discard, resp.Body)
			_ = resp.Body.Close()
			time.Sleep(time.Duration(attempt+1) * time.Second)
			continue
		}

		if resp.StatusCode != http.StatusOK {
			body, _ := io.ReadAll(resp.Body)
			_ = resp.Body.Close()
			return models.OrderBook{}, fmt.Errorf("HTTP %s: %s", resp.Status, string(body))
		}

		var raw struct {
			LastUpdateId int64           `json:"lastUpdateId"`
			Bids         [][]interface{} `json:"bids"`
			Asks         [][]interface{} `json:"asks"`
		}
		if err := json.NewDecoder(resp.Body).Decode(&raw); err != nil {
			_ = resp.Body.Close()
			return models.OrderBook{}, fmt.Errorf("decode: %w", err)
		}
		_ = resp.Body.Close()

		bids := make([][]float64, 0, len(raw.Bids))
		for _, row := range raw.Bids {
			price, _ := strconv.ParseFloat(row[0].(string), 64)
			qty, _ := strconv.ParseFloat(row[1].(string), 64)
			bids = append(bids, []float64{price, qty})
		}

		asks := make([][]float64, 0, len(raw.Asks))
		for _, row := range raw.Asks {
			price, _ := strconv.ParseFloat(row[0].(string), 64)
			qty, _ := strconv.ParseFloat(row[1].(string), 64)
			asks = append(asks, []float64{price, qty})
		}

		return models.OrderBook{
			Time:     time.Now().UTC(),
			Symbol:   strings.ToUpper(s.symbol),
			Market:   "futures",
			Exchange: s.exchange,
			Bids:     mustMarshalJSON(bids),
			Asks:     mustMarshalJSON(asks),
		}, nil
	}

	return models.OrderBook{}, fmt.Errorf("fetch failed after %d retries: %w", maxRetries, lastErr)
}

func (s *Scraper) Stop() {
	select {
	case <-s.done:
		return
	default:
		close(s.done)
	}
}

func mustMarshalJSON(v interface{}) []byte {
	b, _ := json.Marshal(v)
	return b
}
