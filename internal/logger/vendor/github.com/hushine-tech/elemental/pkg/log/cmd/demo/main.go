package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"time"

	sdklog "github.com/hushine-tech/golang-lib/pkg/log"
)

func main() {
	// 从 pkg/log 目录执行: go run ./cmd/demo
	configPath := flag.String("config", "../../examples/log-local-demo.json", "path to log config file")
	flag.Parse()

	if err := sdklog.InitLog(*configPath); err != nil {
		log.Fatalf("init log failed: %v", err)
	}
	defer func() {
		if err := sdklog.Close(); err != nil {
			log.Printf("close log failed: %v", err)
		}
	}()

	sessionID := fmt.Sprintf("demo-session-%d", time.Now().Unix())
	ctx := sdklog.WithSessionID(context.Background(), sessionID)

	sdklog.Info(ctx, "root", "demo root")
	sdklog.Info(ctx, "system", "demo app started")

	sdklog.Access(ctx, sdklog.AccessLogEntry{
		Method:        "POST",
		Path:          "/api/trade",
		RequestHeader: map[string]string{"Content-Type": "application/json"},
		RequestParams: map[string]any{"symbol": "BTCUSDT", "side": "BUY", "quantity": 0.01},
		RequestBody:   `{"symbol":"BTCUSDT","side":"BUY","quantity":0.01}`,
		ResponseBody:  `{"orderId":123456789}`,
		HTTPStatus:    200,
		LatencyMs:     42,
	})

	sdklog.ExtAPI(ctx, sdklog.ExtAPILogEntry{
		APIName:       "Binance",
		URL:           "GET /api/v3/account",
		FullURL:       "https://api.binance.com/api/v3/account",
		RequestHeader: map[string]string{"X-MBX-APIKEY": "demo-key"},
		RequestParams: map[string]any{"recvWindow": 5000},
		RequestBody:   "",
		ResponseBody:  `{"balances":[{"asset":"BTC","free":"1.0"}]}`,
		HTTPStatus:    200,
		LatencyMs:     118,
	})

	sdklog.WebSocket(ctx, sdklog.WebSocketLogEntry{
		URL:       "GET /ws/btcusdt@kline_1m",
		FullURL:   "wss://stream.binance.com:9443/ws/btcusdt@kline_1m",
		EventType: "kline",
		Direction: "recv",
		Frame:     `{"e":"kline","s":"BTCUSDT","k":{"t":1710938625000}}`,
		LatencyMs: 3,
	})

	sdklog.SQL(ctx, sdklog.SQLLogEntry{
		Statement:    "SELECT * FROM orders WHERE session = ?",
		RowsAffected: 0,
		LatencyMs:    5,
	})

	fmt.Printf("demo logs sent with session_id=%s\n", sessionID)
}
