package scraper

import (
	"context"
	"time"

	"github.com/hushine-tech/scraper/internal/marketdata"
	base "github.com/hushine-tech/scraper/internal/scraper"
	"github.com/hushine-tech/scraper/internal/scraper/futureskline"
	"github.com/hushine-tech/scraper/internal/scraper/spotkline"
	"github.com/hushine-tech/scraper/internal/storage"
)

type KlineScraper struct {
	market string
	spot   *spotkline.Scraper
	fut    *futureskline.Scraper
}

func NewKlineScraper(symbol, market, exchange string, store *storage.TimescaleDB, publisher marketdata.Publisher) *KlineScraper {
	return NewKlineScraperWithInterval(symbol, market, exchange, "1m", store, publisher)
}

func NewKlineScraperWithInterval(symbol, market, exchange, interval string, store *storage.TimescaleDB, publisher marketdata.Publisher) *KlineScraper {
	if market == "futures" {
		return &KlineScraper{
			market: market,
			fut:    futureskline.NewScraperWithIntervalAndPublisher(symbol, exchange, interval, store, publisher),
		}
	}
	return &KlineScraper{
		market: market,
		spot:   spotkline.NewScraperWithIntervalAndPublisher(symbol, exchange, interval, store, publisher),
	}
}

func (s *KlineScraper) SetReverse(start, end time.Time) {
	if s.market == "futures" {
		s.fut.SetReverse(start, end)
		return
	}
	s.spot.SetReverse(start, end)
}

func (s *KlineScraper) SetObserver(observer base.KlineObserver) {
	if s.market == "futures" {
		s.fut.SetObserver(observer)
		return
	}
	s.spot.SetObserver(observer)
}

func (s *KlineScraper) Run(ctx context.Context) {
	if s.market == "futures" {
		s.fut.Run(ctx)
		return
	}
	s.spot.Run(ctx)
}

func (s *KlineScraper) Stop() {
	if s.market == "futures" {
		s.fut.Stop()
		return
	}
	s.spot.Stop()
}

var _ base.Scraper = (*KlineScraper)(nil)
