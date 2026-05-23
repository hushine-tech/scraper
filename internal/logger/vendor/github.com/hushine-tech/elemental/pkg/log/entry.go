package log

import (
	"fmt"
	"time"
)

func buildEntry(logType, msg string, level Level, sessionID string) map[string]interface{} {
	return map[string]interface{}{
		"timestamp":  time.Now().UnixMilli(),
		"log_time":  time.Now().In(time.Local).Format(time.RFC3339Nano),
		"level":      level.String(),
		"type":       logType,
		"host":       getHost(),
		"message":    truncateMessage(msg),
		"session_id": sessionID,
	}
}

func buildAccessEntry(access AccessLogEntry, sessionID string) map[string]interface{} {
	return map[string]interface{}{
		"timestamp":      time.Now().UnixMilli(),
		"log_time":       time.Now().In(time.Local).Format(time.RFC3339Nano),
		"level":          LevelInfo.String(),
		"type":           "access",
		"host":           getHost(),
		"message":        truncateMessage(fmt.Sprintf("%s %s", access.Method, access.Path)),
		"session_id":      sessionID,
		"method":          access.Method,
		"path":            access.Path,
		"request_header":  access.RequestHeader,
		"request_params":  access.RequestParams,
		"request_body":    access.RequestBody,
		"response_body":   access.ResponseBody,
		"http_status":     access.HTTPStatus,
		"latency_ms":      access.LatencyMs,
	}
}

func buildExtAPIEntry(extAPI ExtAPILogEntry, sessionID string) map[string]interface{} {
	return map[string]interface{}{
		"timestamp":      time.Now().UnixMilli(),
		"log_time":       time.Now().In(time.Local).Format(time.RFC3339Nano),
		"level":          LevelInfo.String(),
		"type":           "ext_api",
		"host":           getHost(),
		"message":        truncateMessage(fmt.Sprintf("%s %s", extAPI.APIName, extAPI.URL)),
		"session_id":     sessionID,
		"api_name":       extAPI.APIName,
		"url":            extAPI.URL,
		"full_url":       extAPI.FullURL,
		"request_header": extAPI.RequestHeader,
		"request_params": extAPI.RequestParams,
		"request_body":   extAPI.RequestBody,
		"response_body":  extAPI.ResponseBody,
		"http_status":    extAPI.HTTPStatus,
		"latency_ms":     extAPI.LatencyMs,
	}
}

func buildSQLEntry(sqlLog SQLLogEntry, sessionID string) map[string]interface{} {
	return map[string]interface{}{
		"timestamp":     time.Now().UnixMilli(),
		"log_time":     time.Now().In(time.Local).Format(time.RFC3339Nano),
		"level":        LevelInfo.String(),
		"type":         "sql",
		"host":         getHost(),
		"message":      truncateMessage(sqlLog.Statement),
		"session_id":   sessionID,
		"statement":    sqlLog.Statement,
		"rows_affected": sqlLog.RowsAffected,
		"latency_ms":   sqlLog.LatencyMs,
	}
}

func buildWebSocketEntry(ws WebSocketLogEntry, sessionID string) map[string]interface{} {
	return map[string]interface{}{
		"timestamp":  time.Now().UnixMilli(),
		"log_time":   time.Now().In(time.Local).Format(time.RFC3339Nano),
		"level":      LevelInfo.String(),
		"type":       "websocket",
		"host":       getHost(),
		"message":    truncateMessage(fmt.Sprintf("%s %s %s", ws.URL, ws.EventType, ws.Direction)),
		"session_id": sessionID,
		"url":        ws.URL,
		"full_url":   ws.FullURL,
		"event_type": ws.EventType,
		"direction":  ws.Direction,
		"frame":      truncateMessage(ws.Frame),
		"latency_ms": ws.LatencyMs,
	}
}
