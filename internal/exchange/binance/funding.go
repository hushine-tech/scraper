package binance

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/hushine-tech/golang-lib/middleware/httpclient"
	"github.com/hushine-tech/scraper/internal/logger"
	"github.com/hushine-tech/scraper/internal/models"
	base "github.com/hushine-tech/scraper/internal/scraper"
	"github.com/hushine-tech/scraper/internal/storage"
)

const (
	binanceFuturesBaseURL = "https://fapi.binance.com"
	fundingRatePath       = "/fapi/v1/fundingRate"
	premiumIndexPath      = "/fapi/v1/premiumIndex"
	fundingPollInterval   = time.Minute
	fundingPageLimit      = 1000
)

type fundingRatePayload struct {
	Symbol      string `json:"symbol"`
	FundingRate string `json:"fundingRate"`
	MarkPrice   string `json:"markPrice"`
	FundingTime int64  `json:"fundingTime"`
}

type premiumIndexPayload struct {
	Symbol          string `json:"symbol"`
	MarkPrice       string `json:"markPrice"`
	LastFundingRate string `json:"lastFundingRate"`
	NextFundingTime int64  `json:"nextFundingTime"`
	Time            int64  `json:"time"`
}

type fundingMarketDataCollector struct {
	symbols    []string
	storage    *storage.TimescaleDB
	direction  base.Direction
	startTime  time.Time
	endTime    time.Time
	httpClient *httpclient.Client
	baseURL    string
	done       chan struct{}
}

func NewFundingMarketDataCollector(symbols []string, store *storage.TimescaleDB) *fundingMarketDataCollector {
	return &fundingMarketDataCollector{
		symbols:    append([]string(nil), symbols...),
		storage:    store,
		baseURL:    binanceFuturesBaseURL,
		httpClient: httpclient.New(&http.Client{Timeout: 10 * time.Second}, logger.NewExtAPIAdapter(), "futures_funding_market_data"),
		done:       make(chan struct{}),
	}
}

func (c *fundingMarketDataCollector) SetReverse(start, end time.Time) {
	c.direction = base.DirectionReverse
	c.startTime = start.UTC()
	c.endTime = end.UTC()
}

func (c *fundingMarketDataCollector) Run(ctx context.Context) {
	if c.direction == base.DirectionReverse {
		c.backfill(ctx)
		return
	}

	c.poll(ctx)
	ticker := time.NewTicker(fundingPollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-c.done:
			return
		case <-ticker.C:
			c.poll(ctx)
		}
	}
}

func (c *fundingMarketDataCollector) Stop() {
	select {
	case <-c.done:
	default:
		close(c.done)
	}
}

func (c *fundingMarketDataCollector) poll(ctx context.Context) {
	for _, symbol := range c.symbols {
		rate, err := c.fetchCurrentFundingRate(ctx, symbol)
		if err != nil {
			logger.NewScraperLifecycleHelper().Event("WARN", "funding_rate_fetch_failed", map[string]any{
				"exchange": "binance",
				"symbol":   symbol,
				"error":    err.Error(),
			})
			continue
		}
		if err := c.insert(ctx, rate); err != nil {
			logger.NewScraperLifecycleHelper().Event("ERROR", "funding_rate_insert_failed", map[string]any{
				"exchange": "binance",
				"symbol":   symbol,
				"error":    err.Error(),
			})
		}
	}
}

func (c *fundingMarketDataCollector) backfill(ctx context.Context) {
	start := c.startTime
	if start.IsZero() {
		start = time.Now().UTC().AddDate(0, -1, 0)
	}
	end := c.endTime
	if end.IsZero() {
		end = time.Now().UTC()
	}
	if !end.After(start) {
		logger.NewScraperLifecycleHelper().Event("WARN", "funding_rate_backfill_invalid_range", map[string]any{
			"start": start,
			"end":   end,
		})
		return
	}
	for _, symbol := range c.symbols {
		if err := c.backfillSymbol(ctx, symbol, start, end); err != nil {
			logger.NewScraperLifecycleHelper().Event("ERROR", "funding_rate_backfill_failed", map[string]any{
				"exchange": "binance",
				"symbol":   symbol,
				"error":    err.Error(),
			})
		}
	}
}

func (c *fundingMarketDataCollector) backfillSymbol(ctx context.Context, symbol string, start, end time.Time) error {
	cursor := start
	var pending *models.FundingRate
	for cursor.Before(end) {
		select {
		case <-ctx.Done():
			return nil
		case <-c.done:
			return nil
		default:
		}

		page, err := c.fetchHistoricalFundingRates(ctx, symbol, cursor, end)
		if err != nil {
			return err
		}
		if len(page) == 0 {
			break
		}
		for i := range page {
			item := page[i]
			if pending != nil {
				next := item.FundingTime
				pending.NextFundingTime = &next
				if err := c.insert(ctx, *pending); err != nil {
					return err
				}
			}
			pending = &item
		}
		cursor = page[len(page)-1].FundingTime.Add(time.Millisecond)
	}
	if pending != nil {
		if err := c.insert(ctx, *pending); err != nil {
			return err
		}
	}
	return nil
}

func (c *fundingMarketDataCollector) insert(ctx context.Context, rate models.FundingRate) error {
	if c.storage == nil {
		return nil
	}
	return c.storage.InsertFundingRate(ctx, rate)
}

func (c *fundingMarketDataCollector) fetchHistoricalFundingRates(ctx context.Context, symbol string, start, end time.Time) ([]models.FundingRate, error) {
	query := url.Values{}
	query.Set("symbol", strings.ToUpper(strings.TrimSpace(symbol)))
	query.Set("startTime", strconv.FormatInt(start.UTC().UnixMilli(), 10))
	query.Set("endTime", strconv.FormatInt(end.UTC().UnixMilli(), 10))
	query.Set("limit", strconv.Itoa(fundingPageLimit))

	var raw []fundingRatePayload
	if err := c.getJSON(ctx, fundingRatePath+"?"+query.Encode(), &raw); err != nil {
		return nil, err
	}
	rates := make([]models.FundingRate, 0, len(raw))
	for _, item := range raw {
		rate, err := historicalFundingRate(item)
		if err != nil {
			return nil, err
		}
		rates = append(rates, rate)
	}
	sort.SliceStable(rates, func(i, j int) bool {
		return rates[i].FundingTime.Before(rates[j].FundingTime)
	})
	for i := 0; i+1 < len(rates); i++ {
		next := rates[i+1].FundingTime
		rates[i].NextFundingTime = &next
	}
	return rates, nil
}

func (c *fundingMarketDataCollector) fetchCurrentFundingRate(ctx context.Context, symbol string) (models.FundingRate, error) {
	query := url.Values{}
	query.Set("symbol", strings.ToUpper(strings.TrimSpace(symbol)))
	var raw premiumIndexPayload
	if err := c.getJSON(ctx, premiumIndexPath+"?"+query.Encode(), &raw); err != nil {
		return models.FundingRate{}, err
	}
	if raw.Time <= 0 || raw.NextFundingTime <= 0 {
		return models.FundingRate{}, fmt.Errorf("premium index did not provide exchange funding and next funding times")
	}
	return models.FundingRate{
		Exchange:           models.ExchangeBinance,
		Market:             models.MarketFutures,
		Symbol:             strings.ToUpper(raw.Symbol),
		FundingTime:        time.UnixMilli(raw.Time).UTC(),
		FundingRateDecimal: raw.LastFundingRate,
		MarkPriceDecimal:   raw.MarkPrice,
		NextFundingTime:    timePtr(time.UnixMilli(raw.NextFundingTime).UTC()),
	}, nil
}

func historicalFundingRate(raw fundingRatePayload) (models.FundingRate, error) {
	if raw.FundingTime <= 0 {
		return models.FundingRate{}, fmt.Errorf("funding rate did not provide fundingTime")
	}
	return models.FundingRate{
		Exchange:           models.ExchangeBinance,
		Market:             models.MarketFutures,
		Symbol:             strings.ToUpper(raw.Symbol),
		FundingTime:        time.UnixMilli(raw.FundingTime).UTC(),
		FundingRateDecimal: raw.FundingRate,
		MarkPriceDecimal:   raw.MarkPrice,
	}, nil
}

func (c *fundingMarketDataCollector) getJSON(ctx context.Context, path string, destination any) error {
	baseURL := strings.TrimRight(c.baseURL, "/")
	if baseURL == "" {
		baseURL = binanceFuturesBaseURL
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, baseURL+path, nil)
	if err != nil {
		return fmt.Errorf("create Binance Funding request: %w", err)
	}
	resp, err := c.httpClient.Do(ctx, req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("Binance Funding HTTP %s: %s", resp.Status, string(body))
	}
	if err := json.NewDecoder(resp.Body).Decode(destination); err != nil {
		return fmt.Errorf("decode Binance Funding response: %w", err)
	}
	return nil
}

func timePtr(t time.Time) *time.Time { return &t }
