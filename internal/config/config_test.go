package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadConfig(t *testing.T) {
	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "config.yaml")

	validConfig := `
exchanges:
  binance:
    enabled: true
    database:
      host: "localhost"
      port: 5432
      user: "postgres"
      password: "postgres"
      dbname: "{exchange}_{year}"
      sslmode: "disable"
    spot_symbols:
      - "btcusdt"
      - "ethusdt"
    futures_symbols:
      - "btcusdt"
      - "ethusdt"
    kline_interval: "1s"
    depth_interval: "1s"

app:
  scraper_interval: 1
  shutdown_timeout: 30
`

	if err := os.WriteFile(configPath, []byte(validConfig), 0644); err != nil {
		t.Fatalf("Failed to write test config: %v", err)
	}

	cfg, err := LoadConfig(configPath)
	if err != nil {
		t.Fatalf("LoadConfig failed: %v", err)
	}

	if !cfg.Exchanges.Binance.Enabled {
		t.Error("Expected Binance to be enabled")
	}
	if cfg.Exchanges.Binance.Database.Host != "localhost" {
		t.Errorf("Expected host 'localhost', got '%s'", cfg.Exchanges.Binance.Database.Host)
	}
	if cfg.Exchanges.Binance.Database.Port != 5432 {
		t.Errorf("Expected port 5432, got %d", cfg.Exchanges.Binance.Database.Port)
	}
	if len(cfg.Exchanges.Binance.SpotSymbols) != 2 {
		t.Errorf("Expected 2 spot symbols, got %d", len(cfg.Exchanges.Binance.SpotSymbols))
	}
	if len(cfg.Exchanges.Binance.FuturesSymbols) != 2 {
		t.Errorf("Expected 2 futures symbols, got %d", len(cfg.Exchanges.Binance.FuturesSymbols))
	}
	if cfg.App.ShutdownTimeout != 30 {
		t.Errorf("Expected shutdown_timeout 30, got %d", cfg.App.ShutdownTimeout)
	}
	if cfg.Mode != "forward" {
		t.Errorf("Expected default mode 'forward', got %q", cfg.Mode)
	}
}

func TestCurrentConfigUsesLoopbackInfrastructure(t *testing.T) {
	cfg, err := LoadConfig(filepath.Join("..", "..", "config.yaml"))
	if err != nil {
		t.Fatalf("LoadConfig current config: %v", err)
	}
	for name, host := range map[string]string{
		"binance database": cfg.Exchanges.Binance.Database.Host,
		"okx database":     cfg.Exchanges.OKX.Database.Host,
	} {
		if host != "127.0.0.1" {
			t.Errorf("%s host = %q, want loopback", name, host)
		}
	}
	if got := cfg.MarketData.Kafka.Brokers; len(got) != 1 || got[0] != "127.0.0.1:9092" {
		t.Fatalf("Kafka brokers = %#v, want loopback", got)
	}
}

func TestConfigValidation(t *testing.T) {
	tests := []struct {
		name    string
		config  Config
		wantErr string
	}{
		{
			name: "no exchange enabled",
			config: Config{
				Exchanges: ExchangesConfig{
					Binance: ExchangeConfig{Enabled: false},
					OKX:     ExchangeConfig{Enabled: false},
				},
				App: AppConfig{
					ShutdownTimeout: 30,
				},
			},
			wantErr: "at least one exchange must be enabled",
		},
		{
			name: "binance enabled but no symbols",
			config: Config{
				Exchanges: ExchangesConfig{
					Binance: ExchangeConfig{
						Enabled:        true,
						SpotSymbols:    []string{},
						FuturesSymbols: []string{},
					},
				},
				App: AppConfig{
					ShutdownTimeout: 30,
				},
			},
			wantErr: "binance is enabled but no symbols configured",
		},
		{
			name: "fixed exchange database rejected",
			config: Config{
				Exchanges: ExchangesConfig{
					Binance: ExchangeConfig{
						Enabled:        true,
						SpotSymbols:    []string{"btcusdt"},
						FuturesSymbols: []string{"btcusdt"},
						Database: DatabaseConfig{
							DBName: "binance",
						},
					},
				},
				App: AppConfig{
					ShutdownTimeout: 30,
				},
			},
			wantErr: "binance database.dbname must target a year database template, not fixed \"binance\"",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.config.Validate()
			if err == nil {
				t.Error("Expected validation error, got nil")
			}
			if err != nil && err.Error() != tt.wantErr {
				t.Errorf("Expected error '%s', got '%s'", tt.wantErr, err.Error())
			}
		})
	}
}

func TestDatabaseConnectionString(t *testing.T) {
	cfg := DatabaseConfig{
		Host:     "localhost",
		Port:     5432,
		User:     "postgres",
		Password: "secret",
		DBName:   "testdb",
		SSLMode:  "disable",
	}

	connStr := cfg.ConnectionString()
	expected := "host=localhost port=5432 user=postgres password=secret dbname=testdb sslmode=disable"

	if connStr != expected {
		t.Errorf("Connection string mismatch:\nExpected: %s\nGot: %s", expected, connStr)
	}
}

func TestLoadConfigFileNotFound(t *testing.T) {
	_, err := LoadConfig("/nonexistent/path/config.yaml")
	if err == nil {
		t.Error("Expected error for nonexistent file, got nil")
	}
}

func TestLoadConfigInvalidYAML(t *testing.T) {
	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "invalid.yaml")

	invalidYAML := `
exchanges:
  binance:
    enabled: true
    database:
      host: [invalid yaml
`

	if err := os.WriteFile(configPath, []byte(invalidYAML), 0644); err != nil {
		t.Fatalf("Failed to write test config: %v", err)
	}

	_, err := LoadConfig(configPath)
	if err == nil {
		t.Error("Expected error for invalid YAML, got nil")
	}
}

func TestValidateModeNormalizationAndValidation(t *testing.T) {
	cfg := Config{
		Mode: " ReVeRsE ",
		Exchanges: ExchangesConfig{
			Binance: ExchangeConfig{
				Enabled:        true,
				SpotSymbols:    []string{"btcusdt"},
				FuturesSymbols: []string{"btcusdt"},
			},
		},
		App: AppConfig{
			ShutdownTimeout: 30,
		},
	}

	if err := cfg.Validate(); err != nil {
		t.Fatalf("unexpected validate error: %v", err)
	}
	if cfg.Mode != "reverse" {
		t.Fatalf("expected normalized mode reverse, got %q", cfg.Mode)
	}

	invalid := cfg
	invalid.Mode = "history"
	if err := invalid.Validate(); err == nil {
		t.Fatal("expected mode validation error")
	}
}

func TestValidateMarketDataKafkaBrokers(t *testing.T) {
	cfg := Config{
		Exchanges: ExchangesConfig{
			Binance: ExchangeConfig{
				Enabled:        true,
				SpotSymbols:    []string{"btcusdt"},
				FuturesSymbols: []string{"btcusdt"},
			},
		},
		MarketData: MarketDataConfig{
			Kafka: MarketDataKafkaConfig{
				Enabled: true,
				Brokers: []string{" kafka-1:9092 ", "", "kafka-2:9092"},
			},
		},
		App: AppConfig{
			ShutdownTimeout: 30,
		},
	}

	if err := cfg.Validate(); err != nil {
		t.Fatalf("unexpected validate error: %v", err)
	}
	if len(cfg.MarketData.Kafka.Brokers) != 2 {
		t.Fatalf("expected 2 normalized brokers, got %d", len(cfg.MarketData.Kafka.Brokers))
	}
	if cfg.MarketData.Kafka.Brokers[0] != "kafka-1:9092" || cfg.MarketData.Kafka.Brokers[1] != "kafka-2:9092" {
		t.Fatalf("unexpected normalized brokers: %#v", cfg.MarketData.Kafka.Brokers)
	}

	cfg.MarketData.Kafka.Brokers = nil
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected broker validation error when kafka is enabled")
	}
}

func TestValidateControlPlaneDefaultsAndRequirements(t *testing.T) {
	cfg := Config{
		Mode: "forward",
		Exchanges: ExchangesConfig{
			Binance: ExchangeConfig{
				Enabled:        true,
				SpotSymbols:    []string{"btcusdt"},
				FuturesSymbols: []string{"btcusdt"},
			},
		},
		MarketData: MarketDataConfig{
			ControlPlane: MarketDataControlPlaneConfig{
				Enabled:                    true,
				MarketDataControlPanelGRPC: " localhost:50051 ",
			},
		},
		App: AppConfig{
			ShutdownTimeout: 30,
		},
	}

	if err := cfg.Validate(); err != nil {
		t.Fatalf("unexpected validate error: %v", err)
	}
	if got := cfg.MarketData.ControlPlane.MarketDataControlPanelGRPC; got != "localhost:50051" {
		t.Fatalf("expected trimmed grpc target, got %q", got)
	}
	if got := cfg.MarketData.ControlPlane.ReconcileIntervalSeconds; got != 5 {
		t.Fatalf("expected default reconcile interval 5, got %d", got)
	}
	if got := cfg.MarketData.ControlPlane.DrainingGracePeriodSeconds; got != 60 {
		t.Fatalf("expected default draining grace 60, got %d", got)
	}
	if got := cfg.MarketData.ControlPlane.RequestTimeoutSeconds; got != 15 {
		t.Fatalf("expected default request timeout 15, got %d", got)
	}
}

func TestValidateControlPlaneRejectsReverseMode(t *testing.T) {
	cfg := Config{
		Mode: "reverse",
		Exchanges: ExchangesConfig{
			Binance: ExchangeConfig{
				Enabled:        true,
				SpotSymbols:    []string{"btcusdt"},
				FuturesSymbols: []string{"btcusdt"},
			},
		},
		MarketData: MarketDataConfig{
			ControlPlane: MarketDataControlPlaneConfig{
				Enabled:                    true,
				MarketDataControlPanelGRPC: "localhost:50051",
			},
		},
		App: AppConfig{
			ShutdownTimeout: 30,
		},
	}

	if err := cfg.Validate(); err == nil {
		t.Fatal("expected reverse-mode control-plane validation error")
	}
}

func TestValidateControlPlaneRequiresTarget(t *testing.T) {
	cfg := Config{
		Mode: "forward",
		Exchanges: ExchangesConfig{
			Binance: ExchangeConfig{
				Enabled:        true,
				SpotSymbols:    []string{"btcusdt"},
				FuturesSymbols: []string{"btcusdt"},
			},
		},
		MarketData: MarketDataConfig{
			ControlPlane: MarketDataControlPlaneConfig{
				Enabled: true,
			},
		},
		App: AppConfig{
			ShutdownTimeout: 30,
		},
	}

	if err := cfg.Validate(); err == nil {
		t.Fatal("expected missing core-service target validation error")
	}
}

func TestValidateControlPlaneAllowsManagedExchangeWithoutStaticSymbols(t *testing.T) {
	cfg := Config{
		Mode: "forward",
		Exchanges: ExchangesConfig{
			Binance: ExchangeConfig{
				Enabled: true,
			},
		},
		MarketData: MarketDataConfig{
			ControlPlane: MarketDataControlPlaneConfig{
				Enabled:                    true,
				MarketDataControlPanelGRPC: "localhost:50051",
			},
		},
		App: AppConfig{
			ShutdownTimeout: 30,
		},
	}

	if err := cfg.Validate(); err != nil {
		t.Fatalf("expected managed exchange without static symbols to validate, got %v", err)
	}
}
