package scraper

import (
	base "github.com/hushine-tech/scraper/internal/scraper"
	"github.com/hushine-tech/scraper/internal/scraper/futuresorderbook"
	"github.com/hushine-tech/scraper/internal/scraper/spotorderbook"
	"github.com/hushine-tech/scraper/internal/storage"
)

func NewOrderbookScraper(symbol, market, exchange string, store *storage.TimescaleDB) base.Scraper {
	if market == "futures" {
		return futuresorderbook.NewScraper(symbol, exchange, store)
	}
	return spotorderbook.NewScraper(symbol, market, exchange, store)
}
