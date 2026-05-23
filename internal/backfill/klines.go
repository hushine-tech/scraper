package backfill

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/hushine-tech/scraper/internal/config"
	"github.com/hushine-tech/scraper/internal/models"
	"github.com/hushine-tech/scraper/internal/storage"

	_ "github.com/lib/pq"
)

const (
	binanceSpotKlinesURL    = "https://api.binance.com/api/v3/klines"
	binanceFuturesKlinesURL = "https://fapi.binance.com/fapi/v1/klines"
	maxKlinesPerRequest     = 1500
	requestPause            = 200 * time.Millisecond
	max429Retries           = 5
)

var reSafeDBName = regexp.MustCompile(`^[A-Za-z0-9_]+$`)

type KlineRequest struct {
	Exchange string
	Market   string
	Symbol   string
	Interval string
	StartAt  time.Time
	EndAt    time.Time
}

type Coverage struct {
	Ready          bool
	CoveredStartAt *time.Time
	CoveredEndAt   *time.Time
}

func RunKlineRequest(ctx context.Context, dbCfg config.DatabaseConfig, migrationsDir string, req KlineRequest) ([]CoverageSegment, error) {
	req = normalizeKlineRequest(req)
	if err := validateKlineRequest(req); err != nil {
		return nil, err
	}
	if req.Exchange != "binance" {
		return nil, fmt.Errorf("historical kline backfill only supports exchange=binance, got %q", req.Exchange)
	}

	client := &http.Client{Timeout: 60 * time.Second}
	var segments []CoverageSegment
	for _, window := range splitByYear(req.StartAt, req.EndAt) {
		targetDB := databaseForYear(dbCfg.DBName, req.Exchange, window.Year())
		if err := ensureDatabaseExists(ctx, dbCfg, targetDB); err != nil {
			return nil, fmt.Errorf("ensure year database %s: %w", targetDB, err)
		}
		yearCfg := dbCfg
		yearCfg.DBName = targetDB
		store, err := storage.NewTimescaleDB(yearCfg.ConnectionString(), req.Exchange, migrationsDir)
		if err != nil {
			return nil, fmt.Errorf("open year database %s: %w", targetDB, err)
		}
		if err := store.InitSchema(ctx); err != nil {
			_ = store.Close()
			return nil, fmt.Errorf("init schema for %s: %w", targetDB, err)
		}
		windowSegments, err := backfillWindow(ctx, client, store, req, window)
		if err != nil {
			_ = store.Close()
			return nil, err
		}
		segments = appendCoverageSegments(segments, windowSegments...)
		if err := store.Close(); err != nil {
			return nil, fmt.Errorf("close year database %s: %w", targetDB, err)
		}
	}
	return segments, nil
}

func VerifyKlineCoverage(ctx context.Context, dbCfg config.DatabaseConfig, req KlineRequest) (Coverage, error) {
	req = normalizeKlineRequest(req)
	if err := validateKlineRequest(req); err != nil {
		return Coverage{}, err
	}
	if req.Exchange != "binance" {
		return Coverage{}, fmt.Errorf("historical kline coverage only supports exchange=binance, got %q", req.Exchange)
	}

	var (
		overallReady = true
		coveredStart *time.Time
		coveredEnd   *time.Time
	)
	for _, window := range splitByYear(req.StartAt, req.EndAt) {
		segmentReady, segmentStart, segmentEnd, err := verifyWindowCoverage(ctx, dbCfg, req, window)
		if err != nil {
			return Coverage{}, err
		}
		if !segmentReady {
			overallReady = false
		}
		if segmentStart != nil && (coveredStart == nil || segmentStart.Before(*coveredStart)) {
			t := segmentStart.UTC()
			coveredStart = &t
		}
		if segmentEnd != nil && (coveredEnd == nil || segmentEnd.After(*coveredEnd)) {
			t := segmentEnd.UTC()
			coveredEnd = &t
		}
	}
	return Coverage{
		Ready:          overallReady,
		CoveredStartAt: coveredStart,
		CoveredEndAt:   coveredEnd,
	}, nil
}

func normalizeKlineRequest(req KlineRequest) KlineRequest {
	req.Exchange = strings.ToLower(strings.TrimSpace(req.Exchange))
	if req.Exchange == "" {
		req.Exchange = "binance"
	}
	req.Market = strings.ToLower(strings.TrimSpace(req.Market))
	req.Symbol = strings.ToUpper(strings.TrimSpace(req.Symbol))
	req.Interval = strings.TrimSpace(req.Interval)
	if req.Interval == "" {
		req.Interval = "1m"
	}
	req.StartAt = req.StartAt.UTC()
	req.EndAt = req.EndAt.UTC()
	return req
}

func validateKlineRequest(req KlineRequest) error {
	if req.Market != "spot" && req.Market != "futures" {
		return fmt.Errorf("market must be spot or futures")
	}
	if req.Symbol == "" {
		return fmt.Errorf("symbol is required")
	}
	if req.Interval == "" {
		return fmt.Errorf("interval is required")
	}
	if !req.EndAt.After(req.StartAt) {
		return fmt.Errorf("end must be after start")
	}
	if _, err := intervalDuration(req.Interval); err != nil {
		return err
	}
	return nil
}

func backfillWindow(ctx context.Context, client *http.Client, store *storage.TimescaleDB, req KlineRequest, window timeWindow) ([]CoverageSegment, error) {
	baseURL := binanceSpotKlinesURL
	if req.Market == "futures" {
		baseURL = binanceFuturesKlinesURL
	}

	cur := window.Start.UTC().UnixMilli()
	endExclusiveMs := window.End.UTC().UnixMilli()
	endInclusiveMs := window.End.UTC().Add(-time.Millisecond).UnixMilli()
	var segments []CoverageSegment
	for cur < endExclusiveMs {
		if endInclusiveMs < cur {
			return segments, nil
		}
		rows, err := fetchKlines(ctx, client, baseURL, req.Symbol, req.Interval, cur, endInclusiveMs)
		if err != nil {
			return nil, fmt.Errorf("fetch klines for %s %s %s: %w", req.Symbol, req.Market, req.Interval, err)
		}
		if len(rows) == 0 {
			return segments, nil
		}

		klines := make([]models.Kline, 0, len(rows))
		for _, row := range rows {
			kline, err := restRowToKline(row, req.Symbol, req.Market, req.Interval)
			if err != nil {
				return nil, fmt.Errorf("parse row: %w", err)
			}
			klines = append(klines, kline)
		}
		klines = filterKlinesForWindow(klines, window)
		if len(klines) > 0 {
			if err := store.InsertKline(ctx, klines); err != nil {
				return nil, fmt.Errorf("insert klines: %w", err)
			}
			batchSegments, err := BuildCoverageSegments(req.Exchange, req.Market, req.Symbol, req.Interval, klines)
			if err != nil {
				return nil, fmt.Errorf("build coverage segments: %w", err)
			}
			segments = appendCoverageSegments(segments, batchSegments...)
		}
		lastClose, err := toInt64(rows[len(rows)-1][6])
		if err != nil {
			return nil, fmt.Errorf("last row close time: %w", err)
		}
		next := lastClose + 1
		if next <= cur {
			return nil, fmt.Errorf("pagination stuck at close_time=%d", lastClose)
		}
		cur = next
		select {
		case <-time.After(requestPause):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return segments, nil
}

func filterKlinesForWindow(klines []models.Kline, window timeWindow) []models.Kline {
	start := window.Start.UTC()
	end := window.End.UTC()
	out := klines[:0]
	for _, kline := range klines {
		openTime := kline.OpenTime.UTC()
		if openTime.Before(start) || !openTime.Before(end) {
			continue
		}
		out = append(out, kline)
	}
	return out
}

func appendCoverageSegments(dst []CoverageSegment, src ...CoverageSegment) []CoverageSegment {
	for _, seg := range src {
		if len(dst) > 0 && canMergeCoverageSegments(dst[len(dst)-1], seg) {
			dst[len(dst)-1].EndAt = seg.EndAt
			dst[len(dst)-1].RowCount += seg.RowCount
			continue
		}
		dst = append(dst, seg)
	}
	return dst
}

func canMergeCoverageSegments(a, b CoverageSegment) bool {
	return a.Exchange == b.Exchange &&
		a.Market == b.Market &&
		a.Symbol == b.Symbol &&
		a.Interval == b.Interval &&
		a.Year == b.Year &&
		a.Source == b.Source &&
		a.EndAt.Equal(b.StartAt)
}

func verifyWindowCoverage(ctx context.Context, dbCfg config.DatabaseConfig, req KlineRequest, window timeWindow) (bool, *time.Time, *time.Time, error) {
	dbName := databaseForYear(dbCfg.DBName, req.Exchange, window.Year())
	exists, err := databaseExists(ctx, dbCfg, dbName)
	if err != nil {
		return false, nil, nil, err
	}
	if !exists {
		return false, nil, nil, nil
	}
	targetCfg := dbCfg
	targetCfg.DBName = dbName
	db, err := sql.Open("postgres", targetCfg.ConnectionString())
	if err != nil {
		return false, nil, nil, err
	}
	defer db.Close()
	if err := db.PingContext(ctx); err != nil {
		return false, nil, nil, err
	}

	tableName := buildKlineTableName(req.Market, req.Symbol, req.Interval)
	query := fmt.Sprintf(
		`SELECT COUNT(*), MIN(open_time), MAX(open_time) FROM %s
		  WHERE symbol = $1 AND open_time >= $2 AND open_time < $3`,
		tableName,
	)
	var (
		count    int64
		minOpen  sql.NullTime
		maxOpen  sql.NullTime
		startUTC = window.Start.UTC()
		endUTC   = window.End.UTC()
	)
	if err := db.QueryRowContext(ctx, query, req.Symbol, startUTC, endUTC).Scan(&count, &minOpen, &maxOpen); err != nil {
		if isUndefinedTable(err) {
			return false, nil, nil, nil
		}
		return false, nil, nil, err
	}
	var coveredStart *time.Time
	if minOpen.Valid {
		t := minOpen.Time.UTC()
		coveredStart = &t
	}
	var coveredEnd *time.Time
	if maxOpen.Valid {
		t, err := coveredEndFromMaxOpenTime(maxOpen.Time, req.Interval)
		if err != nil {
			return false, coveredStart, nil, err
		}
		coveredEnd = &t
	}
	expected, err := expectedBarCount(startUTC, endUTC, req.Interval)
	if err != nil {
		return false, coveredStart, coveredEnd, err
	}
	return count >= expected && coveredStart != nil && coveredEnd != nil, coveredStart, coveredEnd, nil
}

func databaseForYear(base string, exchange string, year int) string {
	exchange = strings.ToLower(strings.TrimSpace(exchange))
	if exchange == "" {
		exchange = "binance"
	}
	base = strings.TrimSpace(base)
	if base == "" {
		base = "{exchange}_{year}"
	}
	if strings.Contains(base, "{exchange}") {
		base = strings.ReplaceAll(base, "{exchange}", exchange)
	}
	if strings.Contains(base, "{year}") {
		return strings.ReplaceAll(base, "{year}", strconv.Itoa(year))
	}
	return fmt.Sprintf("%s_%d", base, year)
}

func ensureDatabaseExists(ctx context.Context, dbCfg config.DatabaseConfig, dbName string) error {
	exists, err := databaseExists(ctx, dbCfg, dbName)
	if err != nil {
		return err
	}
	if exists {
		return nil
	}
	if !reSafeDBName.MatchString(dbName) {
		return fmt.Errorf("unsafe database name %q", dbName)
	}

	adminCfg := dbCfg
	adminCfg.DBName = "postgres"
	db, err := sql.Open("postgres", adminCfg.ConnectionString())
	if err != nil {
		return err
	}
	defer db.Close()
	if err := db.PingContext(ctx); err != nil {
		return err
	}
	if _, err := db.ExecContext(ctx, fmt.Sprintf(`CREATE DATABASE "%s"`, dbName)); err != nil && !strings.Contains(strings.ToLower(err.Error()), "already exists") {
		return err
	}
	return nil
}

func databaseExists(ctx context.Context, dbCfg config.DatabaseConfig, dbName string) (bool, error) {
	adminCfg := dbCfg
	adminCfg.DBName = "postgres"
	db, err := sql.Open("postgres", adminCfg.ConnectionString())
	if err != nil {
		return false, err
	}
	defer db.Close()
	if err := db.PingContext(ctx); err != nil {
		return false, err
	}
	var exists bool
	if err := db.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM pg_database WHERE datname = $1)`, dbName).Scan(&exists); err != nil {
		return false, err
	}
	return exists, nil
}

func buildKlineTableName(market, symbol, interval string) string {
	market = strings.ToLower(strings.TrimSpace(market))
	if market != "futures" {
		market = "spot"
	}
	symbol = strings.ToLower(strings.TrimSpace(symbol))
	interval = strings.ToLower(strings.TrimSpace(interval))
	return fmt.Sprintf("%s_klines_%s_%s", market, symbol, interval)
}

type timeWindow struct {
	Start time.Time
	End   time.Time
}

func (w timeWindow) Year() int {
	return w.Start.UTC().Year()
}

func splitByYear(start, end time.Time) []timeWindow {
	start = start.UTC()
	end = end.UTC()
	if !end.After(start) {
		return nil
	}
	var out []timeWindow
	cur := start
	for cur.Before(end) {
		nextYear := time.Date(cur.Year()+1, 1, 1, 0, 0, 0, 0, time.UTC)
		windowEnd := end
		if nextYear.Before(windowEnd) {
			windowEnd = nextYear
		}
		out = append(out, timeWindow{Start: cur, End: windowEnd})
		cur = windowEnd
	}
	return out
}

func intervalDuration(interval string) (time.Duration, error) {
	if len(interval) < 2 {
		return 0, fmt.Errorf("invalid interval %q", interval)
	}
	unit := interval[len(interval)-1]
	value, err := strconv.Atoi(interval[:len(interval)-1])
	if err != nil || value <= 0 {
		return 0, fmt.Errorf("invalid interval %q", interval)
	}
	switch unit {
	case 's':
		return time.Duration(value) * time.Second, nil
	case 'm':
		return time.Duration(value) * time.Minute, nil
	case 'h':
		return time.Duration(value) * time.Hour, nil
	case 'd':
		return time.Duration(value) * 24 * time.Hour, nil
	case 'w':
		return time.Duration(value) * 7 * 24 * time.Hour, nil
	case 'M':
		return time.Duration(value) * 30 * 24 * time.Hour, nil
	default:
		return 0, fmt.Errorf("invalid interval unit %q", interval)
	}
}

func expectedBarCount(start, end time.Time, interval string) (int64, error) {
	dur, err := intervalDuration(interval)
	if err != nil {
		return 0, err
	}
	diff := end.UTC().Sub(start.UTC())
	if diff < 0 {
		return 0, fmt.Errorf("end must be >= start")
	}
	return int64(diff / dur), nil
}

func coveredEndFromMaxOpenTime(maxOpen time.Time, interval string) (time.Time, error) {
	dur, err := intervalDuration(interval)
	if err != nil {
		return time.Time{}, err
	}
	return maxOpen.UTC().Add(dur), nil
}

func fetchKlines(ctx context.Context, client *http.Client, endpoint, symbol, interval string, startMs, endMs int64) ([][]interface{}, error) {
	for retry := 0; retry <= max429Retries; retry++ {
		q := url.Values{}
		q.Set("symbol", symbol)
		q.Set("interval", interval)
		q.Set("startTime", strconv.FormatInt(startMs, 10))
		q.Set("endTime", strconv.FormatInt(endMs, 10))
		q.Set("limit", strconv.Itoa(maxKlinesPerRequest))

		req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint+"?"+q.Encode(), nil)
		if err != nil {
			return nil, err
		}
		resp, err := client.Do(req)
		if err != nil {
			return nil, err
		}
		body, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			return nil, fmt.Errorf("read body: %w", err)
		}
		if resp.StatusCode == http.StatusOK {
			var rows [][]interface{}
			if err := json.Unmarshal(body, &rows); err != nil {
				return nil, fmt.Errorf("decode klines: %w", err)
			}
			return rows, nil
		}
		if resp.StatusCode == http.StatusTooManyRequests && retry < max429Retries {
			waitSec := 1
			if h := resp.Header.Get("Retry-After"); h != "" {
				if v, err := strconv.Atoi(h); err == nil && v > 0 {
					waitSec = v
				}
			}
			select {
			case <-time.After(time.Duration(waitSec) * time.Second):
			case <-ctx.Done():
				return nil, ctx.Err()
			}
			continue
		}
		return nil, fmt.Errorf("HTTP %s: %s", resp.Status, truncate(body, 500))
	}
	return nil, errors.New("exhausted retries while fetching klines")
}

func truncate(b []byte, n int) string {
	if len(b) <= n {
		return string(b)
	}
	return string(b[:n]) + "..."
}

func restRowToKline(row []interface{}, symbol, market, interval string) (models.Kline, error) {
	if len(row) < 8 {
		return models.Kline{}, fmt.Errorf("short row: need 8 fields, got %d", len(row))
	}
	openMs, err := toInt64(row[0])
	if err != nil {
		return models.Kline{}, fmt.Errorf("open time: %w", err)
	}
	closeMs, err := toInt64(row[6])
	if err != nil {
		return models.Kline{}, fmt.Errorf("close time: %w", err)
	}
	openValue, err := toFloat(row[1])
	if err != nil {
		return models.Kline{}, err
	}
	highValue, err := toFloat(row[2])
	if err != nil {
		return models.Kline{}, err
	}
	lowValue, err := toFloat(row[3])
	if err != nil {
		return models.Kline{}, err
	}
	closeValue, err := toFloat(row[4])
	if err != nil {
		return models.Kline{}, err
	}
	vol, err := toFloat(row[5])
	if err != nil {
		return models.Kline{}, err
	}
	qvol, err := toFloat(row[7])
	if err != nil {
		return models.Kline{}, err
	}
	openTime := time.UnixMilli(openMs).UTC()
	closeTime := time.UnixMilli(closeMs).UTC()
	return models.Kline{
		Time:        openTime,
		Symbol:      symbol,
		Market:      market,
		Exchange:    "binance",
		Interval:    interval,
		OpenTime:    openTime,
		CloseTime:   closeTime,
		Open:        openValue,
		High:        highValue,
		Low:         lowValue,
		Close:       closeValue,
		Volume:      vol,
		QuoteVolume: qvol,
	}, nil
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

func toFloat(v interface{}) (float64, error) {
	switch x := v.(type) {
	case float64:
		return x, nil
	case json.Number:
		return x.Float64()
	case string:
		return strconv.ParseFloat(x, 64)
	default:
		return 0, fmt.Errorf("unsupported type %T", v)
	}
}

func isUndefinedTable(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "does not exist") || strings.Contains(msg, "undefined table")
}
