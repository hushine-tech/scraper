package log

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

type Logger interface {
	Info(ctx context.Context, logType string, msg string)
	Debug(ctx context.Context, logType string, msg string)
	Warn(ctx context.Context, logType string, msg string)
	Error(ctx context.Context, logType string, msg string)
	Fatal(ctx context.Context, logType string, msg string)
	Access(ctx context.Context, access AccessLogEntry)
	ExtAPI(ctx context.Context, extAPI ExtAPILogEntry)
	WebSocket(ctx context.Context, ws WebSocketLogEntry)
	SQL(ctx context.Context, sqlLog SQLLogEntry)
	Close() error
}

type logger struct {
	outputDir string
	files     map[LogType]*os.File
	mu        sync.Mutex
	host      string
	cfg       *Config
}

func NewLogger(cfg *Config) (Logger, error) {
	l := &logger{
		outputDir: cfg.OutputDir,
		files:     make(map[LogType]*os.File),
		host:      getHost(),
		cfg:       cfg,
	}

	if cfg.LocalFile.Enabled {
		if err := os.MkdirAll(cfg.OutputDir, 0755); err != nil {
			return nil, err
		}

		types := []LogType{TypeRoot, TypeSystem, TypeAccess, TypeExtAPI, TypeWebSocket, TypeSQL}
		for _, t := range types {
			f, err := os.OpenFile(
				filepath.Join(cfg.OutputDir, t.FileName()),
				os.O_CREATE|os.O_APPEND|os.O_WRONLY,
				0644,
			)
			if err != nil {
				l.Close()
				return nil, err
			}
			l.files[t] = f
		}
	}

	return l, nil
}

func (l *logger) log(ctx context.Context, logType string, msg string, level Level) {
	sessionID := GetSessionID(ctx)

	entry := map[string]interface{}{
		"timestamp":  time.Now().UnixMilli(),
		"log_time":  time.Now().In(time.Local).Format(time.RFC3339Nano),
		"level":      level.String(),
		"type":       logType,
		"host":       l.host,
		"message":    truncateMessage(msg),
		"session_id": sessionID,
	}

	l.writeEntry(logType, entry)
}

func (l *logger) writeEntry(logType string, entry map[string]interface{}) {
	if !l.cfg.LocalFile.Enabled {
		return
	}

	t, ok := ParseLogType(logType)
	if !ok {
		t = TypeRoot
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	f, ok := l.files[t]
	if !ok {
		f = l.files[TypeRoot]
	}

	data, err := json.Marshal(entry)
	if err != nil {
		return
	}

	f.Write(append(data, '\n'))
}

func (l *logger) Info(ctx context.Context, logType string, msg string) {
	l.log(ctx, logType, msg, LevelInfo)
}

func (l *logger) Debug(ctx context.Context, logType string, msg string) {
	l.log(ctx, logType, msg, LevelDebug)
}

func (l *logger) Warn(ctx context.Context, logType string, msg string) {
	l.log(ctx, logType, msg, LevelWarn)
}

func (l *logger) Error(ctx context.Context, logType string, msg string) {
	l.log(ctx, logType, msg, LevelError)
}

func (l *logger) Fatal(ctx context.Context, logType string, msg string) {
	l.log(ctx, logType, msg, LevelFatal)
}

func (l *logger) Access(ctx context.Context, access AccessLogEntry) {
	sessionID := GetSessionID(ctx)

	entry := map[string]interface{}{
		"timestamp":      time.Now().UnixMilli(),
		"log_time":       time.Now().In(time.Local).Format(time.RFC3339Nano),
		"level":          LevelInfo.String(),
		"type":           "access",
		"host":           l.host,
		"message":        truncateMessage(fmt.Sprintf("%s %s", access.Method, access.Path)),
		"session_id":     sessionID,
		"method":          access.Method,
		"path":            access.Path,
		"request_header":  access.RequestHeader,
		"request_params":  access.RequestParams,
		"request_body":    access.RequestBody,
		"response_body":   access.ResponseBody,
		"http_status":     access.HTTPStatus,
		"latency_ms":      access.LatencyMs,
	}

	l.writeEntry("access", entry)
}

func (l *logger) ExtAPI(ctx context.Context, extAPI ExtAPILogEntry) {
	sessionID := GetSessionID(ctx)

	entry := map[string]interface{}{
		"timestamp":      time.Now().UnixMilli(),
		"log_time":       time.Now().In(time.Local).Format(time.RFC3339Nano),
		"level":          LevelInfo.String(),
		"type":           "ext_api",
		"host":           l.host,
		"message":        truncateMessage(fmt.Sprintf("%s %s", extAPI.APIName, extAPI.URL)),
		"session_id":     sessionID,
		"api_name":        extAPI.APIName,
		"url":            extAPI.URL,
		"full_url":       extAPI.FullURL,
		"request_header":  extAPI.RequestHeader,
		"request_params":  extAPI.RequestParams,
		"request_body":    extAPI.RequestBody,
		"response_body":   extAPI.ResponseBody,
		"http_status":     extAPI.HTTPStatus,
		"latency_ms":     extAPI.LatencyMs,
	}

	l.writeEntry("ext_api", entry)
}

func (l *logger) WebSocket(ctx context.Context, ws WebSocketLogEntry) {
	sessionID := GetSessionID(ctx)

	entry := map[string]interface{}{
		"timestamp":  time.Now().UnixMilli(),
		"log_time":   time.Now().In(time.Local).Format(time.RFC3339Nano),
		"level":      LevelInfo.String(),
		"type":       "websocket",
		"host":       l.host,
		"message":    truncateMessage(fmt.Sprintf("%s %s %s", ws.URL, ws.EventType, ws.Direction)),
		"session_id": sessionID,
		"url":        ws.URL,
		"full_url":   ws.FullURL,
		"event_type": ws.EventType,
		"direction":  ws.Direction,
		"frame":      truncateMessage(ws.Frame),
		"latency_ms": ws.LatencyMs,
	}

	l.writeEntry("websocket", entry)
}

func (l *logger) SQL(ctx context.Context, sqlLog SQLLogEntry) {
	sessionID := GetSessionID(ctx)

	entry := map[string]interface{}{
		"timestamp":     time.Now().UnixMilli(),
		"log_time":     time.Now().In(time.Local).Format(time.RFC3339Nano),
		"level":        LevelInfo.String(),
		"type":         "sql",
		"host":         l.host,
		"message":      truncateMessage(sqlLog.Statement),
		"session_id":   sessionID,
		"statement":    sqlLog.Statement,
		"rows_affected": sqlLog.RowsAffected,
		"latency_ms":   sqlLog.LatencyMs,
	}

	l.writeEntry("sql", entry)
}

func (l *logger) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()

	var err error
	for _, f := range l.files {
		if closeErr := f.Close(); closeErr != nil && err == nil {
			err = closeErr
		}
	}
	l.files = make(map[LogType]*os.File)
	return err
}
