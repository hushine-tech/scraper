package log

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/IBM/sarama"
)

const (
	defaultBufferSize = 10000
	maxRetries       = 3
	retryBaseDelay   = 100 * time.Millisecond
)

type KafkaProducer struct {
	producer sarama.SyncProducer
	topic   string
	prefix  string
	buffer  chan []byte
	done    chan struct{}
	wg      sync.WaitGroup
	cfg     KafkaConfig
}

func NewKafkaProducer(cfg KafkaConfig) (*KafkaProducer, error) {
	saramaConfig := sarama.NewConfig()
	saramaConfig.Producer.RequiredAcks = sarama.WaitForAll
	saramaConfig.Producer.Retry.Max = maxRetries
	saramaConfig.Producer.Return.Successes = true

	producer, err := sarama.NewSyncProducer(cfg.Brokers, saramaConfig)
	if err != nil {
		return nil, fmt.Errorf("create kafka producer: %w", err)
	}

	k := &KafkaProducer{
		producer: producer,
		topic:   cfg.Topic,
		prefix:  cfg.TopicPrefix,
		buffer:  make(chan []byte, defaultBufferSize),
		done:    make(chan struct{}),
		cfg:     cfg,
	}

	k.wg.Add(1)
	go k.worker()

	return k, nil
}

func (k *KafkaProducer) worker() {
	defer k.wg.Done()

	for {
		select {
		case <-k.done:
			k.flushBuffer()
			return
		case msg := <-k.buffer:
			k.sendWithRetry(msg)
		}
	}
}

func (k *KafkaProducer) flushBuffer() {
	for {
		select {
		case msg := <-k.buffer:
			k.sendWithRetry(msg)
		default:
			return
		}
	}
}

func (k *KafkaProducer) sendWithRetry(msg []byte) {
	topic := k.topic
	if k.prefix != "" {
		var entry map[string]interface{}
		if err := json.Unmarshal(msg, &entry); err == nil {
			if logType, ok := entry["type"].(string); ok {
				topic = fmt.Sprintf("%s-%s", k.prefix, logType)
			}
		}
	}

	for i := 0; i < maxRetries; i++ {
		_, _, err := k.producer.SendMessage(&sarama.ProducerMessage{
			Topic: topic,
			Value: sarama.ByteEncoder(msg),
		})

		if err == nil {
			return
		}

		time.Sleep(retryBaseDelay * time.Duration(1<<uint(i)))
	}

	k.writeFallback(msg)
}

func (k *KafkaProducer) writeFallback(msg []byte) {
	filename := fmt.Sprintf("fallback_%d.log", time.Now().Unix())
	fallbackPath := filepath.Join("/var/log/app", filename)

	if dir := os.Getenv("LOG_FALLBACK_DIR"); dir != "" {
		fallbackPath = filepath.Join(dir, filename)
	}

	os.MkdirAll(filepath.Dir(fallbackPath), 0755)

	f, err := os.OpenFile(fallbackPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return
	}
	defer f.Close()

	f.Write(append(msg, '\n'))
}

func (k *KafkaProducer) Send(ctx context.Context, logType string, msg string, level Level) {
	sessionID := GetSessionID(ctx)

	entry := map[string]interface{}{
		"timestamp":  time.Now().UnixMilli(),
		"log_time":  time.Now().In(time.Local).Format(time.RFC3339Nano),
		"level":      level.String(),
		"type":       logType,
		"host":       getHost(),
		"message":    truncateMessage(msg),
		"session_id": sessionID,
	}

	k.enqueue(entry)
}

func (k *KafkaProducer) SendAccess(ctx context.Context, access AccessLogEntry) {
	sessionID := GetSessionID(ctx)

	entry := map[string]interface{}{
		"timestamp":      time.Now().UnixMilli(),
		"log_time":       time.Now().In(time.Local).Format(time.RFC3339Nano),
		"level":          LevelInfo.String(),
		"type":           "access",
		"host":           getHost(),
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

	k.enqueue(entry)
}

func (k *KafkaProducer) SendExtAPI(ctx context.Context, extAPI ExtAPILogEntry) {
	sessionID := GetSessionID(ctx)

	entry := map[string]interface{}{
		"timestamp":      time.Now().UnixMilli(),
		"log_time":       time.Now().In(time.Local).Format(time.RFC3339Nano),
		"level":          LevelInfo.String(),
		"type":           "ext_api",
		"host":           getHost(),
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
		"latency_ms":      extAPI.LatencyMs,
	}

	k.enqueue(entry)
}

func (k *KafkaProducer) SendSQL(ctx context.Context, sqlLog SQLLogEntry) {
	sessionID := GetSessionID(ctx)

	entry := map[string]interface{}{
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

	k.enqueue(entry)
}

func (k *KafkaProducer) SendWebSocket(ctx context.Context, ws WebSocketLogEntry) {
	sessionID := GetSessionID(ctx)

	entry := map[string]interface{}{
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

	k.enqueue(entry)
}

func (k *KafkaProducer) enqueue(entry map[string]interface{}) {
	data, err := json.Marshal(entry)
	if err != nil {
		return
	}

	select {
	case k.buffer <- data:
	default:
		// Buffer full, write to fallback
		k.writeFallback(data)
	}
}

func (k *KafkaProducer) Close() error {
	close(k.done)
	k.wg.Wait()

	if k.producer != nil {
		return k.producer.Close()
	}
	return nil
}
