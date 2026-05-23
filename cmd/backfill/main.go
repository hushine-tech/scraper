package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/hushine-tech/scraper/internal/backfill"
	"github.com/hushine-tech/scraper/internal/config"
)

func init() {
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: %s -symbol SYMBOL -market spot|futures -start TIME [-end TIME] [-interval INTERVAL] [-config PATH]\n\n", os.Args[0])
		flag.PrintDefaults()
	}
}

func main() {
	configPath := flag.String("config", "config.yaml", "path to config file")
	symbol := flag.String("symbol", "", "trading pair, e.g. BTCUSDT")
	market := flag.String("market", "", "spot or futures")
	interval := flag.String("interval", "1m", "Binance kline interval, e.g. 1m, 5m, 1h")
	startStr := flag.String("start", "", "range start (RFC3339 or Unix milliseconds)")
	endStr := flag.String("end", "", "range end (RFC3339 or Unix milliseconds); default: now")
	flag.Parse()

	if *symbol == "" || *market == "" || *startStr == "" {
		flag.Usage()
		os.Exit(2)
	}

	cfg, err := config.LoadConfig(*configPath)
	if err != nil {
		log.Fatalf("load config: %v", err)
	}
	if !cfg.Exchanges.Binance.Enabled {
		log.Fatal("exchanges.binance.enabled must be true in config")
	}

	startAt, err := parseTime(*startStr)
	if err != nil {
		log.Fatalf("parse -start: %v", err)
	}
	endAt := time.Now().UTC()
	if strings.TrimSpace(*endStr) != "" {
		endAt, err = parseTime(*endStr)
		if err != nil {
			log.Fatalf("parse -end: %v", err)
		}
	}

	req := backfill.KlineRequest{
		Exchange: "binance",
		Market:   *market,
		Symbol:   strings.ToUpper(strings.TrimSpace(*symbol)),
		Interval: *interval,
		StartAt:  startAt,
		EndAt:    endAt,
	}
	if _, err := backfill.RunKlineRequest(
		context.Background(),
		cfg.Exchanges.Binance.Database,
		filepath.Join("internal", "storage", "migrations"),
		req,
	); err != nil {
		log.Fatalf("backfill: %v", err)
	}
	log.Printf("backfill done: symbol=%s market=%s interval=%s start=%s end=%s", req.Symbol, req.Market, req.Interval, req.StartAt.Format(time.RFC3339), req.EndAt.Format(time.RFC3339))
}

func parseTime(raw string) (time.Time, error) {
	if ms, err := parseMillis(raw); err == nil {
		return time.UnixMilli(ms).UTC(), nil
	}
	t, err := time.Parse(time.RFC3339, raw)
	if err != nil {
		return time.Time{}, err
	}
	return t.UTC(), nil
}

func parseMillis(raw string) (int64, error) {
	return strconv.ParseInt(strings.TrimSpace(raw), 10, 64)
}
