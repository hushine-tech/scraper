package log

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestNewLogger(t *testing.T) {
	tmpDir := t.TempDir()

	cfg := &Config{
		OutputDir: tmpDir,
		LocalFile: LocalFileConfig{Enabled: true},
	}

	logger, err := NewLogger(cfg)
	if err != nil {
		t.Fatalf("NewLogger failed: %v", err)
	}
	defer logger.Close()

	files := []string{"root.log", "system.log", "access.log"}
	for _, f := range files {
		path := filepath.Join(tmpDir, f)
		if _, err := os.Stat(path); os.IsNotExist(err) {
			t.Errorf("expected file %s to exist", f)
		}
	}
}

func TestLogEntryFormat(t *testing.T) {
	tmpDir := t.TempDir()

	cfg := &Config{
		OutputDir: tmpDir,
		LocalFile: LocalFileConfig{Enabled: true},
	}

	logger, err := NewLogger(cfg)
	if err != nil {
		t.Fatalf("NewLogger failed: %v", err)
	}
	defer logger.Close()

	ctx := context.Background()
	ctx = WithSessionID(ctx, "test-session-123")
	logger.Info(ctx, "root", "test message")

	logPath := filepath.Join(tmpDir, "root.log")
	data, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatalf("failed to read log file: %v", err)
	}

	lines := strings.Split(strings.TrimSpace(string(data)), "\n")
	if len(lines) < 1 {
		t.Fatal("expected at least one log line")
	}

	var entry map[string]interface{}
	if err := json.Unmarshal([]byte(lines[len(lines)-1]), &entry); err != nil {
		t.Fatalf("failed to parse JSON: %v", err)
	}

	if entry["type"] != "root" {
		t.Errorf("expected type=root, got %v", entry["type"])
	}
	if entry["level"] != "INFO" {
		t.Errorf("expected level=INFO, got %v", entry["level"])
	}
	if entry["message"] != "test message" {
		t.Errorf("expected message=test message, got %v", entry["message"])
	}
	if entry["session_id"] != "test-session-123" {
		t.Errorf("expected session_id=test-session-123, got %v", entry["session_id"])
	}

	// Check dual timestamp
	if _, ok := entry["timestamp"]; !ok {
		t.Error("expected timestamp field")
	}
	if _, ok := entry["log_time"]; !ok {
		t.Error("expected log_time field")
	}
}

func TestSessionIDPropagation(t *testing.T) {
	tmpDir := t.TempDir()

	cfg := &Config{
		OutputDir: tmpDir,
		LocalFile: LocalFileConfig{Enabled: true},
	}

	logger, err := NewLogger(cfg)
	if err != nil {
		t.Fatalf("NewLogger failed: %v", err)
	}
	defer logger.Close()

	ctx := WithSessionID(context.Background(), "my-session")
	logger.Info(ctx, "root", "message without explicit session")

	logPath := filepath.Join(tmpDir, "root.log")
	data, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatalf("failed to read log file: %v", err)
	}

	var entry map[string]interface{}
	json.Unmarshal([]byte(strings.TrimSpace(string(data))), &entry)

	if entry["session_id"] != "my-session" {
		t.Errorf("expected session_id=my-session, got %v", entry["session_id"])
	}
}

func TestAutoGenerateSessionID(t *testing.T) {
	tmpDir := t.TempDir()

	cfg := &Config{
		OutputDir: tmpDir,
		LocalFile: LocalFileConfig{Enabled: true},
	}

	logger, err := NewLogger(cfg)
	if err != nil {
		t.Fatalf("NewLogger failed: %v", err)
	}
	defer logger.Close()

	logger.Info(context.Background(), "root", "auto-gen session")

	logPath := filepath.Join(tmpDir, "root.log")
	data, _ := os.ReadFile(logPath)

	var entry map[string]interface{}
	json.Unmarshal([]byte(strings.TrimSpace(string(data))), &entry)

	sessionID, ok := entry["session_id"].(string)
	if !ok || sessionID == "" {
		t.Error("expected non-empty auto-generated session_id")
	}

	if len(sessionID) != 36 {
		t.Errorf("expected UUID format (36 chars), got %d chars", len(sessionID))
	}
}

func TestMessageTruncation(t *testing.T) {
	tmpDir := t.TempDir()

	cfg := &Config{
		OutputDir: tmpDir,
		LocalFile: LocalFileConfig{Enabled: true},
	}

	logger, err := NewLogger(cfg)
	if err != nil {
		t.Fatalf("NewLogger failed: %v", err)
	}
	defer logger.Close()

	longMsg := strings.Repeat("a", 70000)
	logger.Info(context.Background(), "root", longMsg)

	logPath := filepath.Join(tmpDir, "root.log")
	data, _ := os.ReadFile(logPath)

	var entry map[string]interface{}
	json.Unmarshal([]byte(strings.TrimSpace(string(data))), &entry)

	msg, ok := entry["message"].(string)
	if !ok {
		t.Fatal("message field not found")
	}

	if !strings.HasSuffix(msg, "...(truncated)") {
		t.Error("expected message to be truncated with suffix")
	}

	if len(msg) > MaxMessageLen {
		t.Errorf("message length %d exceeds max %d", len(msg), MaxMessageLen)
	}
}

func TestAccessLog(t *testing.T) {
	tmpDir := t.TempDir()

	cfg := &Config{
		OutputDir: tmpDir,
		LocalFile: LocalFileConfig{Enabled: true},
	}

	logger, err := NewLogger(cfg)
	if err != nil {
		t.Fatalf("NewLogger failed: %v", err)
	}
	defer logger.Close()

	ctx := WithSessionID(context.Background(), "access-session")
	logger.Access(ctx, AccessLogEntry{
		Method:         "POST",
		Path:           "/api/trade",
		RequestHeader:  map[string]string{"Content-Type": "application/json"},
		RequestParams:  map[string]any{"symbol": "BTCUSDT", "side": "BUY"},
		RequestBody:    `{"symbol":"BTCUSDT"}`,
		ResponseBody:   `{"orderId":12345}`,
		HTTPStatus:     200,
		LatencyMs:      42,
	})

	logPath := filepath.Join(tmpDir, "access.log")
	data, _ := os.ReadFile(logPath)

	var entry map[string]interface{}
	json.Unmarshal([]byte(strings.TrimSpace(string(data))), &entry)

	if entry["type"] != "access" {
		t.Errorf("expected type=access, got %v", entry["type"])
	}
	if entry["method"] != "POST" {
		t.Errorf("expected method=POST, got %v", entry["method"])
	}
	if entry["path"] != "/api/trade" {
		t.Errorf("expected path=/api/trade, got %v", entry["path"])
	}
	if entry["http_status"] != float64(200) {
		t.Errorf("expected http_status=200, got %v", entry["http_status"])
	}
	if entry["latency_ms"] != float64(42) {
		t.Errorf("expected latency_ms=42, got %v", entry["latency_ms"])
	}
	if params, ok := entry["request_params"].(map[string]any); !ok {
		t.Errorf("expected request_params map, got %T", entry["request_params"])
	} else {
		if params["symbol"] != "BTCUSDT" {
			t.Errorf("expected symbol=BTCUSDT, got %v", params["symbol"])
		}
	}
}

func TestSQLLog(t *testing.T) {
	tmpDir := t.TempDir()

	cfg := &Config{
		OutputDir: tmpDir,
		LocalFile: LocalFileConfig{Enabled: true},
	}

	logger, err := NewLogger(cfg)
	if err != nil {
		t.Fatalf("NewLogger failed: %v", err)
	}
	defer logger.Close()

	logger.SQL(context.Background(), SQLLogEntry{
		Statement:    "INSERT INTO orders VALUES (1, 'BTCUSDT', 100)",
		RowsAffected: 1,
		LatencyMs:    5,
	})

	logPath := filepath.Join(tmpDir, "sql.log")
	data, _ := os.ReadFile(logPath)

	var entry map[string]interface{}
	json.Unmarshal([]byte(strings.TrimSpace(string(data))), &entry)

	if entry["type"] != "sql" {
		t.Errorf("expected type=sql, got %v", entry["type"])
	}
	if entry["statement"] != "INSERT INTO orders VALUES (1, 'BTCUSDT', 100)" {
		t.Errorf("unexpected statement: %v", entry["statement"])
	}
	if entry["rows_affected"] != float64(1) {
		t.Errorf("expected rows_affected=1, got %v", entry["rows_affected"])
	}
	if entry["latency_ms"] != float64(5) {
		t.Errorf("expected latency_ms=5, got %v", entry["latency_ms"])
	}
}

func TestExtAPILog(t *testing.T) {
	tmpDir := t.TempDir()

	cfg := &Config{
		OutputDir: tmpDir,
		LocalFile: LocalFileConfig{Enabled: true},
	}

	logger, err := NewLogger(cfg)
	if err != nil {
		t.Fatalf("NewLogger failed: %v", err)
	}
	defer logger.Close()

	logger.ExtAPI(context.Background(), ExtAPILogEntry{
		APIName:        "Binance",
		URL:            "GET /api/v3/portfolio",
		FullURL:        "https://api.binance.com/api/v3/portfolio",
		RequestHeader:  map[string]string{"X-MBX-APIKEY": "test-key"},
		RequestParams:  map[string]any{"recvWindow": 5000},
		RequestBody:    "",
		ResponseBody:   `{"balances":[{"asset":"BTC","free":"1.0"}]}`,
		HTTPStatus:    200,
		LatencyMs:     150,
	})

	logPath := filepath.Join(tmpDir, "ext_api.log")
	data, _ := os.ReadFile(logPath)

	var entry map[string]interface{}
	json.Unmarshal([]byte(strings.TrimSpace(string(data))), &entry)

	if entry["type"] != "ext_api" {
		t.Errorf("expected type=ext_api, got %v", entry["type"])
	}
	if entry["api_name"] != "Binance" {
		t.Errorf("expected api_name=Binance, got %v", entry["api_name"])
	}
	if entry["latency_ms"] != float64(150) {
		t.Errorf("expected latency_ms=150, got %v", entry["latency_ms"])
	}
	if entry["url"] != "GET /api/v3/portfolio" {
		t.Errorf("expected url=GET /api/v3/portfolio, got %v", entry["url"])
	}
	if entry["full_url"] != "https://api.binance.com/api/v3/portfolio" {
		t.Errorf("expected full_url=https://api.binance.com/api/v3/portfolio, got %v", entry["full_url"])
	}
	if params, ok := entry["request_params"].(map[string]any); !ok {
		t.Errorf("expected request_params map, got %T", entry["request_params"])
	} else {
		if params["recvWindow"] != float64(5000) {
			t.Errorf("expected recvWindow=5000, got %v", params["recvWindow"])
		}
	}
}

func TestHostDetection(t *testing.T) {
	tmpDir := t.TempDir()

	t.Setenv("HOSTNAME", "test-hostname")

	cfg := &Config{
		OutputDir: tmpDir,
		LocalFile: LocalFileConfig{Enabled: true},
	}

	logger, err := NewLogger(cfg)
	if err != nil {
		t.Fatalf("NewLogger failed: %v", err)
	}
	defer logger.Close()

	logger.Info(context.Background(), "root", "host test")

	logPath := filepath.Join(tmpDir, "root.log")
	data, _ := os.ReadFile(logPath)

	var entry map[string]interface{}
	json.Unmarshal([]byte(strings.TrimSpace(string(data))), &entry)

	if entry["host"] != "test-hostname" {
		t.Errorf("expected host=test-hostname, got %v", entry["host"])
	}
}

func TestDualTimestamp(t *testing.T) {
	tmpDir := t.TempDir()

	cfg := &Config{
		OutputDir: tmpDir,
		LocalFile: LocalFileConfig{Enabled: true},
	}

	logger, err := NewLogger(cfg)
	if err != nil {
		t.Fatalf("NewLogger failed: %v", err)
	}
	defer logger.Close()

	beforeTime := time.Now().UnixMilli()
	logger.Info(context.Background(), "root", "timestamp test")
	afterTime := time.Now().UnixMilli()

	logPath := filepath.Join(tmpDir, "root.log")
	data, _ := os.ReadFile(logPath)

	var entry map[string]interface{}
	json.Unmarshal([]byte(strings.TrimSpace(string(data))), &entry)

	ts, ok := entry["timestamp"].(float64)
	if !ok {
		t.Fatal("timestamp field not found or not a number")
	}

	if int64(ts) < beforeTime || int64(ts) > afterTime {
		t.Errorf("timestamp %d not in range [%d, %d]", int64(ts), beforeTime, afterTime)
	}

	logTime, ok := entry["log_time"].(string)
	if !ok {
		t.Fatal("log_time field not found or not a string")
	}

	if logTime == "" {
		t.Error("log_time should not be empty")
	}
}

func TestWebSocketFrameTruncation(t *testing.T) {
	long := strings.Repeat("x", MaxMessageLen+500)
	entry := buildWebSocketEntry(WebSocketLogEntry{
		URL:       "GET /ws/x",
		FullURL:   "wss://example/ws",
		EventType: "kline",
		Direction: "recv",
		Frame:     long,
	}, "sid")
	frame, ok := entry["frame"].(string)
	if !ok {
		t.Fatal("frame missing or not string")
	}
	if !strings.HasSuffix(frame, "...(truncated)") {
		t.Fatalf("expected truncated frame suffix, len=%d", len(frame))
	}
}
