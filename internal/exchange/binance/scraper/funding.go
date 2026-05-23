package scraper

import (
	"context"
	"time"

	base "github.com/hushine-tech/scraper/internal/scraper"
	"github.com/hushine-tech/scraper/internal/scraper/fundingrate"
	"github.com/hushine-tech/scraper/internal/storage"
)

type FundingScraper struct {
	inner base.Scraper
}

func NewFundingScraper(symbols []string, exchange string, store *storage.TimescaleDB) *FundingScraper {
	return &FundingScraper{
		inner: fundingrate.NewScraper(symbols, exchange, store),
	}
}

func (s *FundingScraper) SetReverse(start, end time.Time) {
	if rs, ok := s.inner.(interface {
		SetReverse(time.Time, time.Time)
	}); ok {
		rs.SetReverse(start, end)
	}
}

func (s *FundingScraper) Run(ctx context.Context) { s.inner.Run(ctx) }
func (s *FundingScraper) Stop()                   { s.inner.Stop() }
