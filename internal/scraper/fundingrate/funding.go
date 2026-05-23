package fundingrate

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
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
	restURL           = "https://fapi.binance.com/fapi/v1/fundingRate"
	pollInterval     = 60 * time.Second
	interSymbolDelay = 300 * time.Millisecond
	maxRetries       = 3
)

type Scraper struct {
	symbols  []string
	exchange string
	storage  *storage.TimescaleDB
	direction scraper.Direction
	httpClient *httpclient.Client
	done     chan struct{}
	startTime time.Time
	endTime   time.Time
}

type fundingRatePayload struct {
	Symbol      string `json:"symbol"`
	FundingRate string `json:"fundingRate"`
	MarkPrice   string `json:"markPrice"`
	FundingTime int64  `json:"fundingTime"`
}

func NewScraper(symbols []string, exchange string, store *storage.TimescaleDB) scraper.Scraper {
	return &Scraper{
		symbols:  symbols,
		exchange: exchange,
		storage:  store,
		httpClient: httpclient.New(
			&http.Client{Timeout: 10 * time.Second},
			logger.NewExtAPIAdapter(),
			"funding_rate_rest",
		),
		done: make(chan struct{}),
	}
}

func (s *Scraper) SetReverse(startTime, endTime time.Time) {
	s.direction = scraper.DirectionReverse
	s.startTime = startTime
	s.endTime = endTime
}

func (s *Scraper) Run(ctx context.Context) {
	if s.direction == scraper.DirectionReverse {
		s.reverse(ctx)
		return
	}
	s.run(ctx)
}

func (s *Scraper) run(ctx context.Context) {
	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()

	s.poll(ctx)

	for {
		select {
		case <-ctx.Done():
			return
		case <-s.done:
			return
		case <-ticker.C:
			s.poll(ctx)
		}
	}
}

func (s *Scraper) poll(ctx context.Context) {
	for i, symbol := range s.symbols {
		if i > 0 {
			select {
			case <-ctx.Done():
				return
			case <-s.done:
				return
			case <-time.After(interSymbolDelay):
			}
		}

		fr, err := s.fetchFundingRate(ctx, symbol)
		if err != nil {
			logger.NewScraperLifecycleHelper().Event("WARN", "funding_rate_fetch_failed", map[string]any{
				"exchange": s.exchange,
				"symbol":   symbol,
				"error":    err.Error(),
			})
			continue
		}

		if err := s.storage.InsertFundingRate(ctx, *fr); err != nil {
			logger.NewScraperLifecycleHelper().Event("ERROR", "funding_rate_insert_failed", map[string]any{
				"exchange": s.exchange,
				"symbol":   symbol,
				"error":    err.Error(),
			})
			continue
		}
	}
}

func (s *Scraper) reverse(ctx context.Context) {
	for _, symbol := range s.symbols {
		if err := s.backfillSymbol(ctx, symbol); err != nil {
			continue
		}
	}
}

func (s *Scraper) backfillSymbol(ctx context.Context, symbol string) error {
	startMs := s.startTime.UnixMilli()
	if startMs <= 0 {
		startMs = time.Now().UTC().AddDate(0, -1, 0).UnixMilli()
	}
	endMs := s.endTime.UnixMilli()
	if endMs <= 0 {
		endMs = time.Now().UTC().UnixMilli()
	}
	if endMs <= startMs {
		return fmt.Errorf("invalid reverse range start=%d end=%d", startMs, endMs)
	}

	current := startMs
	for current < endMs {
		select {
		case <-ctx.Done():
			return nil
		case <-s.done:
			return nil
		default:
		}

		batch, err := s.fetchFundingRatesBatch(ctx, symbol, current, endMs)
		if err != nil {
			return err
		}
		if len(batch) == 0 {
			break
		}

		for _, item := range batch {
			if err := s.storage.InsertFundingRate(ctx, item); err != nil {
				continue
			}
		}

		current = batch[len(batch)-1].Time.UnixMilli() + 1
	}

	return nil
}

func (s *Scraper) fetchFundingRate(ctx context.Context, symbol string) (*models.FundingRate, error) {
	url := fmt.Sprintf("%s?symbol=%s", restURL, symbol)

	var lastErr error
	for attempt := 0; attempt < maxRetries; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		if err != nil {
			return nil, fmt.Errorf("create request: %w", err)
		}

		resp, err := s.httpClient.Do(ctx, req)
		if err != nil {
			lastErr = err
			time.Sleep(time.Duration(attempt+1) * time.Second)
			continue
		}

		if resp.StatusCode == http.StatusTooManyRequests {
			_, _ = io.Copy(io.Discard, resp.Body)
			_ = resp.Body.Close()
			time.Sleep(time.Duration(attempt+1) * time.Second)
			continue
		}

		if resp.StatusCode != http.StatusOK {
			_, _ = io.Copy(io.Discard, resp.Body)
			_ = resp.Body.Close()
			return nil, fmt.Errorf("HTTP %d", resp.StatusCode)
		}

		var raw []fundingRatePayload

		if err := json.NewDecoder(resp.Body).Decode(&raw); err != nil {
			_ = resp.Body.Close()
			lastErr = err
			continue
		}
		_ = resp.Body.Close()

		if len(raw) == 0 {
			return nil, fmt.Errorf("empty response")
		}

		latest := raw[len(raw)-1]
		item, err := s.toFundingRate(latest)
		if err != nil {
			lastErr = err
			continue
		}
		return &item, nil
	}

	return nil, fmt.Errorf("failed after %d retries: %w", maxRetries, lastErr)
}

func (s *Scraper) fetchFundingRatesBatch(ctx context.Context, symbol string, startMs, endMs int64) ([]models.FundingRate, error) {
	url := fmt.Sprintf("%s?symbol=%s&startTime=%d&endTime=%d&limit=%d", restURL, symbol, startMs, endMs, 1000)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}
	resp, err := s.httpClient.Do(ctx, req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("HTTP %d", resp.StatusCode)
	}

	var raw []fundingRatePayload
	if err := json.NewDecoder(resp.Body).Decode(&raw); err != nil {
		return nil, err
	}

	result := make([]models.FundingRate, 0, len(raw))
	for _, item := range raw {
		rate, err := s.toFundingRate(item)
		if err != nil {
			continue
		}
		result = append(result, rate)
	}
	return result, nil
}

func (s *Scraper) toFundingRate(raw fundingRatePayload) (models.FundingRate, error) {
	fundingRate, err := strconv.ParseFloat(raw.FundingRate, 64)
	if err != nil {
		return models.FundingRate{}, fmt.Errorf("parse fundingRate %q: %w", raw.FundingRate, err)
	}
	markPrice, err := strconv.ParseFloat(raw.MarkPrice, 64)
	if err != nil {
		return models.FundingRate{}, fmt.Errorf("parse markPrice %q: %w", raw.MarkPrice, err)
	}
	t := time.UnixMilli(raw.FundingTime).UTC()
	return models.FundingRate{
		Time:            t,
		Symbol:          strings.ToUpper(raw.Symbol),
		Market:          "futures",
		Exchange:        s.exchange,
		FundingRate:     fundingRate,
		MarkPrice:       markPrice,
		NextFundingTime: t.Add(8 * time.Hour),
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
