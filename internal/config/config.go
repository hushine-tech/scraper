package config

import (
	"fmt"
	"os"
	"strings"

	"gopkg.in/yaml.v3"
)

type Config struct {
	Mode       string           `yaml:"mode"`
	Exchanges  ExchangesConfig  `yaml:"exchanges"`
	MarketData MarketDataConfig `yaml:"market_data"`
	App        AppConfig        `yaml:"app"`
}

type ExchangesConfig struct {
	Binance ExchangeConfig `yaml:"binance"`
	OKX     ExchangeConfig `yaml:"okx"`
}

// ExchangeConfig is the canonical per-exchange configuration.
type ExchangeConfig struct {
	Enabled        bool           `yaml:"enabled"`
	Database       DatabaseConfig `yaml:"database"`
	SpotSymbols    []string       `yaml:"spot_symbols"`
	FuturesSymbols []string       `yaml:"futures_symbols"`
	KlineInterval  string         `yaml:"kline_interval"`
	KlineIntervals []string       `yaml:"kline_intervals"` // 抓取的 K 线周期列表，默认 ["1m"]
	DepthInterval  string         `yaml:"depth_interval"`
	Forward        ForwardConfig  `yaml:"forward"`
	Reverse        ReverseConfig  `yaml:"reverse"`
}

type AppConfig struct {
	ScraperInterval int `yaml:"scraper_interval"`
	ShutdownTimeout int `yaml:"shutdown_timeout"`
}

type MarketDataConfig struct {
	Kafka        MarketDataKafkaConfig        `yaml:"kafka"`
	ControlPlane MarketDataControlPlaneConfig `yaml:"control_plane"`
}

type MarketDataKafkaConfig struct {
	Enabled bool     `yaml:"enabled"`
	Brokers []string `yaml:"brokers"`
}

type MarketDataControlPlaneConfig struct {
	Enabled bool `yaml:"enabled"`
	// Target for the current market-data control-plane API.
	MarketDataControlPanelGRPC string `yaml:"market_data_control_panel_grpc"`
	ReconcileIntervalSeconds   int    `yaml:"reconcile_interval_seconds"`
	DrainingGracePeriodSeconds int    `yaml:"draining_grace_period_seconds"`
	RequestTimeoutSeconds      int    `yaml:"request_timeout_seconds"`
}

type ForwardConfig struct {
	SpotKline           bool `yaml:"spot_kline"`
	FuturesKline        bool `yaml:"futures_kline"`
	SpotOrderBook       bool `yaml:"spot_orderbook"`
	FuturesOrderBook    bool `yaml:"futures_orderbook"`
	FundingRate         bool `yaml:"funding_rate"`
	FuturesOpenInterest bool `yaml:"futures_open_interest"`
}

type ReverseConfig struct {
	SpotKline           bool   `yaml:"spot_kline"`
	FuturesKline        bool   `yaml:"futures_kline"`
	SpotOrderBook       bool   `yaml:"spot_orderbook"`
	FuturesOrderBook    bool   `yaml:"futures_orderbook"`
	FundingRate         bool   `yaml:"funding_rate"`
	FuturesOpenInterest bool   `yaml:"futures_open_interest"`
	StartTime           string `yaml:"start_time"`
	EndTime             string `yaml:"end_time"`
}

type DatabaseConfig struct {
	Host     string `yaml:"host"`
	Port     int    `yaml:"port"`
	User     string `yaml:"user"`
	Password string `yaml:"password"`
	DBName   string `yaml:"dbname"`
	SSLMode  string `yaml:"sslmode"`
}

func LoadConfig(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}

	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("failed to parse config file: %w", err)
	}

	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("config validation failed: %w", err)
	}

	return &cfg, nil
}

func (c *Config) Validate() error {
	c.Mode = strings.ToLower(strings.TrimSpace(c.Mode))
	if c.Mode == "" {
		c.Mode = "forward"
	}
	if c.Mode != "forward" && c.Mode != "reverse" {
		return fmt.Errorf("mode must be either forward or reverse")
	}

	if !c.Exchanges.Binance.Enabled && !c.Exchanges.OKX.Enabled {
		return fmt.Errorf("at least one exchange must be enabled")
	}
	if c.Exchanges.Binance.Enabled && !c.MarketData.ControlPlane.Enabled && len(c.Exchanges.Binance.SpotSymbols) == 0 && len(c.Exchanges.Binance.FuturesSymbols) == 0 {
		return fmt.Errorf("binance is enabled but no symbols configured")
	}
	if c.Exchanges.Binance.Enabled {
		if err := validateDatabaseTarget("binance", c.Exchanges.Binance.Database.DBName); err != nil {
			return err
		}
	}
	if c.Exchanges.OKX.Enabled && !c.MarketData.ControlPlane.Enabled && len(c.Exchanges.OKX.SpotSymbols) == 0 && len(c.Exchanges.OKX.FuturesSymbols) == 0 {
		return fmt.Errorf("okx is enabled but no symbols configured")
	}
	if c.Exchanges.OKX.Enabled {
		if err := validateDatabaseTarget("okx", c.Exchanges.OKX.Database.DBName); err != nil {
			return err
		}
	}
	if c.App.ShutdownTimeout <= 0 {
		c.App.ShutdownTimeout = 30
	}
	c.MarketData.Kafka.Brokers = normalizeList(c.MarketData.Kafka.Brokers)
	if c.MarketData.Kafka.Enabled && len(c.MarketData.Kafka.Brokers) == 0 {
		return fmt.Errorf("market_data.kafka is enabled but no brokers configured")
	}
	c.MarketData.ControlPlane.MarketDataControlPanelGRPC = strings.TrimSpace(c.MarketData.ControlPlane.MarketDataControlPanelGRPC)
	if c.MarketData.ControlPlane.Enabled {
		if c.Mode != "forward" {
			return fmt.Errorf("market_data.control_plane is only supported in forward mode")
		}
		if c.MarketData.ControlPlane.MarketDataControlPanelGRPC == "" {
			return fmt.Errorf("market_data.control_plane.market_data_control_panel_grpc is required when control plane is enabled")
		}
	}
	if c.MarketData.ControlPlane.ReconcileIntervalSeconds <= 0 {
		c.MarketData.ControlPlane.ReconcileIntervalSeconds = 5
	}
	if c.MarketData.ControlPlane.DrainingGracePeriodSeconds <= 0 {
		c.MarketData.ControlPlane.DrainingGracePeriodSeconds = 60
	}
	if c.MarketData.ControlPlane.RequestTimeoutSeconds <= 0 {
		c.MarketData.ControlPlane.RequestTimeoutSeconds = 15
	}
	// 默认 kline intervals
	setDefaultIntervals := func(ec *ExchangeConfig) {
		if len(ec.KlineIntervals) == 0 {
			ec.KlineIntervals = []string{"1m"}
		}
	}
	setDefaultIntervals(&c.Exchanges.Binance)
	setDefaultIntervals(&c.Exchanges.OKX)
	return nil
}

func validateDatabaseTarget(exchange, dbName string) error {
	dbName = strings.TrimSpace(dbName)
	if dbName == "" {
		return nil
	}
	if strings.EqualFold(dbName, exchange) {
		return fmt.Errorf("%s database.dbname must target a year database template, not fixed %q", exchange, dbName)
	}
	return nil
}

func (c *DatabaseConfig) ConnectionString() string {
	return fmt.Sprintf(
		"host=%s port=%d user=%s password=%s dbname=%s sslmode=%s",
		c.Host, c.Port, c.User, c.Password, c.DBName, c.SSLMode,
	)
}

func normalizeList(values []string) []string {
	if len(values) == 0 {
		return nil
	}
	out := make([]string, 0, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		out = append(out, value)
	}
	return out
}
