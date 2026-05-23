package scraper

import (
	"context"

	"github.com/hushine-tech/scraper/internal/models"
)

// Direction 采集方向
type Direction string

const (
	DirectionForward Direction = "forward"
	DirectionReverse Direction = "reverse"
)

// Scraper 采集器通用接口
type Scraper interface {
	// Run 启动采集（根据 direction 不同实现不同逻辑）
	Run(ctx context.Context)
	// Stop 停止采集，幂等安全
	Stop()
}

// KlineObserver is an optional hook for forward/live K-line collectors.
// It lets higher-level runtimes observe successful closed-bar ingestion
// without taking ownership of the collector's storage/publish logic.
type KlineObserver interface {
	OnStored(models.Kline)
}
