package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"github.com/hushine-tech/scraper/internal/config"
	"github.com/hushine-tech/scraper/internal/controlplane"
	"github.com/hushine-tech/scraper/internal/exchange"
	"github.com/hushine-tech/scraper/internal/exchange/binance"
	binancekline "github.com/hushine-tech/scraper/internal/exchange/binance/scraper"
	"github.com/hushine-tech/scraper/internal/exchange/okx"
	"github.com/hushine-tech/scraper/internal/logger"
	"github.com/hushine-tech/scraper/internal/marketdata"
	"github.com/hushine-tech/scraper/internal/storage"

	"github.com/google/uuid"
	elog "github.com/hushine-tech/golang-lib/pkg/log"
)

func main() {
	startedAt := time.Now()
	configPath := flag.String("config", getenv("SCRAPER_CONFIG", "config.yaml"), "path to scraper config yaml")
	logConfigPath := flag.String("log-config", getenv("SCRAPER_LOG_CONFIG", "log-config.json"), "path to scraper log config json")
	flag.Parse()

	logCfgFile, err := os.Open(*logConfigPath)
	if err != nil {
		log.Fatalf("Failed to open log config: %v", err)
	}
	defer logCfgFile.Close()

	var logCfg logger.Config
	if err := json.NewDecoder(logCfgFile).Decode(&logCfg); err != nil {
		log.Fatalf("Failed to decode log config: %v", err)
	}

	sessionID := uuid.New().String()
	scraperInstanceID := getenv("SCRAPER_INSTANCE_ID", sessionID)
	if err := logger.Init(logCfg, sessionID); err != nil {
		log.Fatalf("Failed to init logger: %v", err)
	}
	defer logger.Close()

	tracerShutdown, err := elog.InitTracer(*logConfigPath)
	if err != nil {
		log.Printf("init tracer: %v (continuing without tracing)", err)
	}
	if tracerShutdown != nil {
		defer tracerShutdown(context.Background())
	}

	cfg, err := config.LoadConfig(*configPath)
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	mode := cfg.Mode
	if mode == "" {
		mode = "forward"
	}

	var marketDataPublisher marketdata.Publisher
	if cfg.MarketData.Kafka.Enabled {
		publisher, err := marketdata.NewKafkaPublisher(cfg.MarketData.Kafka.Brokers)
		if err != nil {
			log.Fatalf("Failed to init market-data Kafka publisher: %v", err)
		}
		marketDataPublisher = publisher
		defer func() {
			if err := publisher.Close(); err != nil {
				log.Printf("close market-data Kafka publisher: %v", err)
			}
		}()
		logger.NewSystemHelper().Lifecycle("market_data_kafka_ready", map[string]any{
			"enabled": true,
			"brokers": cfg.MarketData.Kafka.Brokers,
		})
	}

	logger.NewSystemHelper().Lifecycle("service_booting", map[string]any{
		"mode":                mode,
		"market_data_enabled": cfg.MarketData.Kafka.Enabled,
		"scraper_instance_id": scraperInstanceID,
	})

	migrationsDir := filepath.Join("internal", "storage", "migrations")
	registry := exchange.NewRegistry()
	registry.Register("binance", binance.New)
	registry.Register("okx", okx.New)

	type runningSet struct {
		name     string
		scrapers []exchange.Scraper
		store    *storage.TimescaleDB
	}
	var runs []runningSet
	managedFactories := make(map[string]controlplane.CollectorFactory)
	historicalFundingStores := make(map[string]exchange.HistoricalFundingStore)

	if cfg.Exchanges.Binance.Enabled {
		store := newRoutedStore("binance", cfg.Exchanges.Binance.Database, migrationsDir)
		historicalFundingStores["binance"] = store
		binanceRuntime := exchange.RuntimeConfig{
			Mode:           mode,
			ExchangeName:   "binance",
			SpotSymbols:    cfg.Exchanges.Binance.SpotSymbols,
			FuturesSymbols: cfg.Exchanges.Binance.FuturesSymbols,
			KlineIntervals: cfg.Exchanges.Binance.KlineIntervals,
			Forward:        cfg.Exchanges.Binance.Forward,
			Reverse:        cfg.Exchanges.Binance.Reverse,
			Publisher:      marketDataPublisher,
		}
		if cfg.MarketData.ControlPlane.Enabled {
			binanceRuntime.Forward.SpotKline = false
			binanceRuntime.Forward.FuturesKline = false
			managedFactories["binance"] = newBinanceManagedKlineFactory(store)
		}
		scrapers, err := registry.Build("binance", binanceRuntime, store)
		if err != nil {
			log.Fatalf("Failed to build binance scrapers: %v", err)
		}
		runs = append(runs, runningSet{name: "binance", scrapers: scrapers, store: store})
	}
	if cfg.Exchanges.OKX.Enabled {
		store := newRoutedStore("okx", cfg.Exchanges.OKX.Database, migrationsDir)
		historicalFundingStores["okx"] = store
		okxRuntime := exchange.RuntimeConfig{
			Mode:           mode,
			ExchangeName:   "okx",
			SpotSymbols:    cfg.Exchanges.OKX.SpotSymbols,
			FuturesSymbols: cfg.Exchanges.OKX.FuturesSymbols,
			KlineIntervals: cfg.Exchanges.OKX.KlineIntervals,
			Forward:        cfg.Exchanges.OKX.Forward,
			Reverse:        cfg.Exchanges.OKX.Reverse,
			Publisher:      marketDataPublisher,
		}
		if cfg.MarketData.ControlPlane.Enabled {
			okxRuntime.Forward.SpotKline = false
			okxRuntime.Forward.FuturesKline = false
			managedFactories["okx"] = unsupportedManagedKlineFactory("okx")
		}
		scrapers, err := registry.Build("okx", okxRuntime, store)
		if err != nil {
			log.Fatalf("Failed to build okx scrapers: %v", err)
		}
		runs = append(runs, runningSet{name: "okx", scrapers: scrapers, store: store})
	}
	defer func() {
		for _, r := range runs {
			_ = r.store.Close()
		}
	}()

	var plugins []exchange.Scraper
	for _, r := range runs {
		plugins = append(plugins, r.scrapers...)
	}
	if cfg.MarketData.ControlPlane.Enabled {
		requestTimeout := time.Duration(cfg.MarketData.ControlPlane.RequestTimeoutSeconds) * time.Second
		client, err := controlplane.NewGRPCClient(cfg.MarketData.ControlPlane.MarketDataControlPanelGRPC, requestTimeout)
		if err != nil {
			log.Fatalf("Failed to init market-data control-plane client: %v", err)
		}
		leaseClient, ok := client.(controlplane.WriterLeaseClient)
		if !ok {
			log.Fatalf("Market-data control-plane client does not support writer leases")
		}
		writerLeaseManager := controlplane.NewWriterLeaseManager(controlplane.WriterLeaseManagerConfig{
			Client:            leaseClient,
			OwnerInstanceID:   scraperInstanceID,
			ScraperInstanceID: scraperInstanceID,
			TTL:               90 * time.Second,
		})
		for _, r := range runs {
			r.store.SetWriterLeaseManager(writerLeaseManager)
		}
		runtime := controlplane.NewRuntime(controlplane.RuntimeConfig{
			Client:              client,
			Factories:           managedFactories,
			Publisher:           marketDataPublisher,
			ReconcileInterval:   time.Duration(cfg.MarketData.ControlPlane.ReconcileIntervalSeconds) * time.Second,
			DrainingGracePeriod: time.Duration(cfg.MarketData.ControlPlane.DrainingGracePeriodSeconds) * time.Second,
			ReportTimeout:       requestTimeout,
		})
		plugins = append(plugins, runtime)
		historyRuntime := controlplane.NewHistoricalRuntime(controlplane.HistoricalRuntimeConfig{
			Client:   client,
			Registry: registry,
			Databases: map[string]config.DatabaseConfig{
				"binance": cfg.Exchanges.Binance.Database,
				"okx":     cfg.Exchanges.OKX.Database,
			},
			FundingStores:     historicalFundingStores,
			MigrationsDir:     migrationsDir,
			ReconcileInterval: time.Duration(cfg.MarketData.ControlPlane.ReconcileIntervalSeconds) * time.Second,
		})
		plugins = append(plugins, historyRuntime)
		logger.NewSystemHelper().Lifecycle("market_data_control_plane_ready", map[string]any{
			"grpc_target":             cfg.MarketData.ControlPlane.MarketDataControlPanelGRPC,
			"reconcile_interval_secs": cfg.MarketData.ControlPlane.ReconcileIntervalSeconds,
			"draining_grace_secs":     cfg.MarketData.ControlPlane.DrainingGracePeriodSeconds,
			"request_timeout_secs":    cfg.MarketData.ControlPlane.RequestTimeoutSeconds,
			"scraper_instance_id":     scraperInstanceID,
		})
	}

	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	for _, sc := range plugins {
		wg.Add(1)
		go func(s exchange.Scraper) {
			defer wg.Done()
			s.Run(ctx)
		}(sc)
	}

	logger.NewSystemHelper().Lifecycle("service_ready", map[string]any{
		"mode":                mode,
		"plugin_count":        len(plugins),
		"scraper_instance_id": scraperInstanceID,
	})

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	sig := <-sigCh

	logger.NewSystemHelper().Lifecycle("shutdown_signal_received", map[string]any{
		"signal": sig.String(),
	})
	cancel()
	for _, sc := range plugins {
		sc.Stop()
	}

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), time.Duration(cfg.App.ShutdownTimeout)*time.Second)
	defer shutdownCancel()

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-shutdownCtx.Done():
		logger.NewSystemHelper().Warning("shutdown_wait_timeout", map[string]any{
			"timeout_seconds": cfg.App.ShutdownTimeout,
		})
	case <-done:
	}

	logger.NewSystemHelper().Lifecycle("service_stopped", map[string]any{
		"runtime_ms": time.Since(startedAt).Milliseconds(),
	})
	fmt.Println("Shutdown complete")
}

func newBinanceManagedKlineFactory(store *storage.TimescaleDB) controlplane.CollectorFactory {
	return func(stream controlplane.Stream, publisher marketdata.Publisher) (controlplane.ManagedCollector, error) {
		switch stream.Key.Market {
		case "spot", "futures":
			return binancekline.NewKlineScraperWithInterval(
				stream.Key.Symbol,
				stream.Key.Market,
				stream.Key.Exchange,
				stream.Key.Interval,
				store,
				publisher,
			), nil
		default:
			return nil, fmt.Errorf("managed kline market %q is not supported for exchange %q", stream.Key.Market, stream.Key.Exchange)
		}
	}
}

func unsupportedManagedKlineFactory(exchangeName string) controlplane.CollectorFactory {
	return func(stream controlplane.Stream, publisher marketdata.Publisher) (controlplane.ManagedCollector, error) {
		_ = stream
		_ = publisher
		return nil, fmt.Errorf("managed control-plane kline collectors are not implemented for exchange %q", exchangeName)
	}
}

func newRoutedStore(exchangeName string, dbCfg config.DatabaseConfig, migrationsDir string) *storage.TimescaleDB {
	return storage.NewRoutedTimescaleDB(func(ctx context.Context, exchange string, year int) (storage.MarketDataStore, error) {
		targetDB, err := storage.DatabaseNameForYear(dbCfg.DBName, exchange, year)
		if err != nil {
			return nil, err
		}
		yearCfg := dbCfg
		yearCfg.DBName = targetDB
		store, err := storage.NewTimescaleDB(yearCfg.ConnectionString(), exchangeName, migrationsDir)
		if err != nil {
			return nil, fmt.Errorf("connect TimescaleDB(%s): %w", targetDB, err)
		}
		initCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		defer cancel()
		if err := store.InitSchema(initCtx); err != nil {
			_ = store.Close()
			return nil, fmt.Errorf("init schema(%s): %w", targetDB, err)
		}
		return store, nil
	})
}

func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
