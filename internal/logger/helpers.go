package logger

import (
	"context"
	"fmt"
	"sort"
	"strings"

	elog "github.com/hushine-tech/golang-lib/pkg/log"
)

type SystemHelper struct{}

func (h *SystemHelper) Lifecycle(event string, fields map[string]any) {
	System().Info(formatEvent(event, fields))
}

func (h *SystemHelper) Warning(event string, fields map[string]any) {
	System().Warn(formatEvent(event, fields))
}

func (h *SystemHelper) Error(event string, fields map[string]any) {
	System().Error(formatEvent(event, fields))
}

type WebsocketHelper struct{}

func (h *WebsocketHelper) Event(level, event string, fields map[string]any) {
	msg := formatEvent(event, fields)
	switch strings.ToUpper(level) {
	case "ERROR":
		Websocket().Error(msg)
	case "WARN":
		Websocket().Warn(msg)
	default:
		Websocket().Info(msg)
	}
}

type ExtAPIHelper struct{}

func (h *ExtAPIHelper) Event(level, event string, fields map[string]any) {
	msg := formatEvent(event, fields)
	switch strings.ToUpper(level) {
	case "ERROR":
		ExtAPI().Error(msg)
	case "WARN":
		ExtAPI().Warn(msg)
	default:
		ExtAPI().Info(msg)
	}
}

type ExtAPIAdapter struct{}
type KafkaAdapter struct{}

func (a *ExtAPIAdapter) ExtAPI(ctx context.Context, extAPI elog.ExtAPILogEntry) {
	elog.ExtAPI(ctx, extAPI)
}

func (a *KafkaAdapter) KafkaSent(ctx context.Context, entry elog.KafkaSentLogEntry) {
	elog.KafkaSent(ctx, entry)
}

type SQLHelper struct{}

func (h *SQLHelper) Statement(statement string, durationMs int64, rowsAffected int64, sessionID string) {
	ctx := withSessionID(context.Background(), sessionID)
	entry := elog.SQLLogEntry{
		Statement:    statement,
		RowsAffected: rowsAffected,
		LatencyMs:    durationMs,
	}
	elog.SQL(ctx, entry)
}

type SQLAdapter struct{}

func (a *SQLAdapter) SQL(ctx context.Context, sqlLog elog.SQLLogEntry) {
	elog.SQL(ctx, sqlLog)
}

func (a *SQLAdapter) Fatal(ctx context.Context, logType, msg string) {
	elog.Fatal(ctx, logType, msg)
}

type ScraperLifecycleHelper struct{}

type WebSocketAdapter struct{}

func (a *WebSocketAdapter) WebSocket(ctx context.Context, ws elog.WebSocketLogEntry) {
	elog.WebSocket(ctx, ws)
}

func (h *ScraperLifecycleHelper) Event(level, event string, fields map[string]any) {
	msg := formatEvent(event, fields)
	switch strings.ToUpper(level) {
	case "ERROR":
		Root().Error(msg)
	case "WARN":
		Root().Warn(msg)
	default:
		Root().Info(msg)
	}
}

func (h *ScraperLifecycleHelper) Started(kind, market, symbol, direction string) {
	Root().Info(formatEvent("plugin_started", map[string]any{
		"kind":      kind,
		"market":    market,
		"symbol":    symbol,
		"direction": direction,
	}))
}

func (h *ScraperLifecycleHelper) Stopped(kind, market, symbol, direction string) {
	Root().Info(formatEvent("plugin_stopped", map[string]any{
		"kind":      kind,
		"market":    market,
		"symbol":    symbol,
		"direction": direction,
	}))
}

func withSessionID(ctx context.Context, sessionID string) context.Context {
	if sessionID == "" {
		return ctx
	}
	return elog.WithSessionID(ctx, sessionID)
}

func NewSystemHelper() *SystemHelper {
	return &SystemHelper{}
}

func NewWebsocketHelper() *WebsocketHelper {
	return &WebsocketHelper{}
}

func NewExtAPIHelper() *ExtAPIHelper {
	return &ExtAPIHelper{}
}

func NewExtAPIAdapter() *ExtAPIAdapter {
	return &ExtAPIAdapter{}
}

func NewKafkaAdapter() *KafkaAdapter {
	return &KafkaAdapter{}
}

func NewSQLHelper() *SQLHelper {
	return &SQLHelper{}
}

func NewSQLAdapter() *SQLAdapter {
	return &SQLAdapter{}
}

func NewWebSocketAdapter() *WebSocketAdapter {
	return &WebSocketAdapter{}
}

func NewScraperLifecycleHelper() *ScraperLifecycleHelper {
	return &ScraperLifecycleHelper{}
}

func formatEvent(event string, fields map[string]any) string {
	if len(fields) == 0 {
		return fmt.Sprintf("event=%s", event)
	}

	keys := make([]string, 0, len(fields))
	for k := range fields {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	parts := make([]string, 0, len(keys)+1)
	parts = append(parts, fmt.Sprintf("event=%s", event))
	for _, k := range keys {
		parts = append(parts, fmt.Sprintf("%s=%v", k, fields[k]))
	}

	return strings.Join(parts, " ")
}
