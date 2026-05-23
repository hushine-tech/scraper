package common

import (
	"context"

	"github.com/hushine-tech/scraper/internal/scraper"
)

type ScraperPlugin interface {
	Run(ctx context.Context)
	Stop()
	Kind() string
	Market() string
	Symbol() string
}

type Kind string

const (
	KindKline        Kind = "kline"
	KindOrderBook    Kind = "orderbook"
	KindFundingRate  Kind = "fundingrate"
	KindOpenInterest Kind = "openinterest"
)

type Market string

const (
	MarketSpot    Market = "spot"
	MarketFutures Market = "futures"
)

// Direction 采集方向，复用 scraper 包定义，避免重复声明。
type Direction = scraper.Direction

const (
	DirectionForward Direction = scraper.DirectionForward
	DirectionReverse Direction = scraper.DirectionReverse
)

var SupportedReverseModes = map[Kind]bool{
	KindKline:        true,
	KindOrderBook:    false,
	KindFundingRate:  true,
	KindOpenInterest: true,
}
