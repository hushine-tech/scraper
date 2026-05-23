package log

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"gopkg.in/yaml.v3"
)

type Config struct {
	OutputDir     string              `json:"output_dir" yaml:"output_dir"`
	Kafka        KafkaConfig         `json:"kafka" yaml:"kafka"`
	LocalFile    LocalFileConfig     `json:"local_file" yaml:"local_file"`
	Elasticsearch ElasticsearchConfig `json:"elasticsearch" yaml:"elasticsearch"`
}

type KafkaConfig struct {
	Enabled     bool     `json:"enabled" yaml:"enabled"`
	Brokers     []string `json:"brokers" yaml:"brokers"`
	Topic       string   `json:"topic" yaml:"topic"`
	TopicPrefix string   `json:"topic_prefix" yaml:"topic_prefix"`
}

type LocalFileConfig struct {
	Enabled bool `json:"enabled" yaml:"enabled"`
}

type ElasticsearchConfig struct {
	Enabled     bool     `json:"enabled" yaml:"enabled"`
	Addresses   []string `json:"addresses" yaml:"addresses"`
	IndexPrefix string   `json:"index_prefix" yaml:"index_prefix"`
	Username    string   `json:"username" yaml:"username"`
	Password    string   `json:"password" yaml:"password"`
}

func DefaultConfig() *Config {
	return &Config{
		OutputDir: "/var/log/app",
		Kafka: KafkaConfig{
			Enabled: false,
			Brokers: []string{},
			Topic:   "app-logs",
		},
		LocalFile: LocalFileConfig{
			Enabled: true,
		},
	}
}

func LoadConfig(path string) (*Config, error) {
	if path == "" {
		return findConfig()
	}

	ext := strings.ToLower(filepath.Ext(path))
	var data []byte
	var err error

	data, err = os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config file: %w", err)
	}

	cfg := &Config{}
	switch ext {
	case ".json":
		err = json.Unmarshal(data, cfg)
	case ".yaml", ".yml":
		err = yaml.Unmarshal(data, cfg)
	default:
		return nil, fmt.Errorf("unsupported config format: %s", ext)
	}

	if err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}

	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("validate config: %w", err)
	}

	return cfg, nil
}

func findConfig() (*Config, error) {
	paths := []string{
		"./log.json",
		"./log.yaml",
		"./log.yml",
		"/etc/app/log.json",
		"/etc/app/log.yaml",
		"/etc/app/log.yml",
	}

	for _, p := range paths {
		data, err := os.ReadFile(p)
		if err != nil {
			continue
		}

		cfg := &Config{}
		ext := strings.ToLower(filepath.Ext(p))

		switch ext {
		case ".json":
			err = json.Unmarshal(data, cfg)
		case ".yaml", ".yml":
			err = yaml.Unmarshal(data, cfg)
		}

		if err == nil && cfg.Validate() == nil {
			return cfg, nil
		}
	}

	return DefaultConfig(), nil
}

func (c *Config) Validate() error {
	if c.OutputDir == "" {
		return fmt.Errorf("output_dir is required")
	}
	if c.Kafka.Enabled {
		if len(c.Kafka.Brokers) == 0 {
			return fmt.Errorf("kafka.brokers is required when kafka.enabled=true")
		}
		if c.Kafka.Topic == "" {
			return fmt.Errorf("kafka.topic is required when kafka.enabled=true")
		}
	}
	if c.Elasticsearch.Enabled {
		if len(c.Elasticsearch.Addresses) == 0 {
			return fmt.Errorf("elasticsearch.addresses is required when elasticsearch.enabled=true")
		}
	}
	if !c.Kafka.Enabled && !c.LocalFile.Enabled && !c.Elasticsearch.Enabled {
		return fmt.Errorf("at least one of kafka.enabled, local_file.enabled, or elasticsearch.enabled must be true")
	}
	return nil
}
