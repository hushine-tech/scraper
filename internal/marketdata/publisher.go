package marketdata

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/hushine-tech/scraper/internal/logger"
	"github.com/hushine-tech/scraper/internal/models"

	"github.com/IBM/sarama"
	kmw "github.com/hushine-tech/golang-lib/middleware/kafka"
)

const (
	KlineTopicPrefix = "md.kline"
)

type Publisher interface {
	PublishKlines(ctx context.Context, klines []models.Kline) error
	Close() error
}

type sender interface {
	Send(ctx context.Context, topic string, key []byte, value []byte) error
}

type KafkaPublisher struct {
	producer sarama.SyncProducer
	sender   sender
}

func NewKafkaPublisher(brokers []string) (*KafkaPublisher, error) {
	cfg := sarama.NewConfig()
	cfg.Producer.RequiredAcks = sarama.WaitForAll
	cfg.Producer.Retry.Max = 3
	cfg.Producer.Return.Successes = true

	producer, err := sarama.NewSyncProducer(brokers, cfg)
	if err != nil {
		return nil, fmt.Errorf("create market-data kafka producer: %w", err)
	}

	return &KafkaPublisher{
		producer: producer,
		sender:   kmw.NewKafkaProducerMiddleware(producer, logger.NewKafkaAdapter()),
	}, nil
}

func KlineTopic(exchange, market, interval string) string {
	exchange = strings.ToLower(strings.TrimSpace(exchange))
	market = strings.ToLower(strings.TrimSpace(market))
	interval = strings.ToLower(strings.TrimSpace(interval))
	if interval == "" {
		interval = "1m"
	}
	return fmt.Sprintf("%s.%s.%s.%s", KlineTopicPrefix, exchange, market, interval)
}

func (p *KafkaPublisher) PublishKlines(ctx context.Context, klines []models.Kline) error {
	for _, kline := range klines {
		body, err := marshalKline(kline)
		if err != nil {
			return err
		}
		topic := KlineTopic(kline.Exchange, kline.Market, kline.Interval)
		key := strings.ToUpper(strings.TrimSpace(kline.Symbol))
		if err := p.sender.Send(ctx, topic, []byte(key), body); err != nil {
			return err
		}
	}
	return nil
}

func (p *KafkaPublisher) Close() error {
	if p == nil || p.producer == nil {
		return nil
	}
	return p.producer.Close()
}

type klinePayload struct {
	Symbol    string  `json:"symbol"`
	Interval  string  `json:"interval"`
	OpenTime  int64   `json:"open_time"`
	CloseTime int64   `json:"close_time"`
	Open      float64 `json:"open"`
	High      float64 `json:"high"`
	Low       float64 `json:"low"`
	Close     float64 `json:"close"`
	Volume    float64 `json:"volume"`
	Timestamp int64   `json:"timestamp"`
	Market    string  `json:"market,omitempty"`
}

func marshalKline(kline models.Kline) ([]byte, error) {
	return json.Marshal(klinePayload{
		Symbol:    strings.ToUpper(strings.TrimSpace(kline.Symbol)),
		Interval:  strings.ToLower(strings.TrimSpace(kline.Interval)),
		OpenTime:  kline.OpenTime.UnixMilli(),
		CloseTime: kline.CloseTime.UnixMilli(),
		Open:      kline.Open,
		High:      kline.High,
		Low:       kline.Low,
		Close:     kline.Close,
		Volume:    kline.Volume,
		Timestamp: kline.CloseTime.UnixMilli(),
		Market:    strings.ToLower(strings.TrimSpace(kline.Market)),
	})
}
