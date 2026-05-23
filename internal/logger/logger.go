package logger

import (
	"context"
	"encoding/json"
	"os"

	elog "github.com/hushine-tech/golang-lib/pkg/log"
)

var sessionID string

// Config matches the JSON structure in log-config.json
type Config struct {
	OutputDir  string           `json:"output_dir"`
	Kafka      KafkaConfig      `json:"kafka"`
	LocalFile  LocalFileConfig  `json:"local_file"`
}

type KafkaConfig struct {
	Enabled bool     `json:"enabled"`
	Brokers []string `json:"brokers"`
	Topic   string   `json:"topic"`
}

type LocalFileConfig struct {
	Enabled bool `json:"enabled"`
}

func Init(cfg Config, sid string) error {
	// Write config to temp file for InitLog
	data, err := json.Marshal(cfg)
	if err != nil {
		return err
	}

	tmpFile, err := os.CreateTemp("", "log-config-*.json")
	if err != nil {
		return err
	}
	defer os.Remove(tmpFile.Name())

	if _, err := tmpFile.Write(data); err != nil {
		return err
	}
	tmpFile.Close()

	if err := elog.InitLog(tmpFile.Name()); err != nil {
		return err
	}
	sessionID = sid
	return nil
}

func Close() error {
	return elog.Close()
}

func SessionID() string {
	return sessionID
}

type logFunc struct {
	logType string
}

func (l *logFunc) Error(msg string) {
	ctx := context.Background()
	if sessionID != "" {
		ctx = elog.WithSessionID(ctx, sessionID)
	}
	elog.Error(ctx, l.logType, msg)
}

func (l *logFunc) Info(msg string) {
	ctx := context.Background()
	if sessionID != "" {
		ctx = elog.WithSessionID(ctx, sessionID)
	}
	elog.Info(ctx, l.logType, msg)
}

func (l *logFunc) Warn(msg string) {
	ctx := context.Background()
	if sessionID != "" {
		ctx = elog.WithSessionID(ctx, sessionID)
	}
	elog.Warn(ctx, l.logType, msg)
}

func Root() *logFunc {
	return &logFunc{logType: "root"}
}

func System() *logFunc {
	return &logFunc{logType: "system"}
}

func Websocket() *logFunc {
	return &logFunc{logType: "websocket"}
}

func ExtAPI() *logFunc {
	return &logFunc{logType: "ext_api"}
}

func SQL() *logFunc {
	return &logFunc{logType: "sql"}
}

func Access() *logFunc {
	return &logFunc{logType: "access"}
}
