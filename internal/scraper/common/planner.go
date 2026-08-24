package common

import (
	"context"
	"strings"
	"time"

	"github.com/hushine-tech/scraper/internal/config"
	bscraper "github.com/hushine-tech/scraper/internal/exchange/binance/scraper"
	"github.com/hushine-tech/scraper/internal/logger"
	"github.com/hushine-tech/scraper/internal/scraper"
	"github.com/hushine-tech/scraper/internal/scraper/fundingrate"
	"github.com/hushine-tech/scraper/internal/scraper/futureskline"
	"github.com/hushine-tech/scraper/internal/scraper/futuresorderbook"
	"github.com/hushine-tech/scraper/internal/scraper/spotkline"
	"github.com/hushine-tech/scraper/internal/scraper/spotorderbook"
	"github.com/hushine-tech/scraper/internal/storage"
)

type Planner struct {
	config    *config.ExchangeConfig
	storage   *storage.TimescaleDB
	sessionID string
}

func NewPlanner(cfg *config.ExchangeConfig, store *storage.TimescaleDB, sessionID string) *Planner {
	return &Planner{
		config:    cfg,
		storage:   store,
		sessionID: sessionID,
	}
}

func (p *Planner) BuildPlugins(plan CollectionPlan) []ScraperPlugin {
	plugins := make([]ScraperPlugin, 0, len(plan.EnabledKinds))
	for _, item := range plan.EnabledKinds {
		switch {
		case item.Kind == KindKline && item.Market == MarketSpot:
			for _, sym := range plan.SpotSymbols {
				sc := spotkline.NewScraper(sym, "binance", p.storage)
				if plan.Mode == DirectionReverse {
					sc.SetReverse(plan.Scope.StartTime, plan.Scope.EndTime)
				}
				plugins = append(plugins, &pluginAdapter{
					kind:      KindKline,
					market:    MarketSpot,
					symbol:    sym,
					direction: plan.Mode,
					scraper:   sc,
				})
			}
		case item.Kind == KindKline && item.Market == MarketFutures:
			for _, sym := range plan.FuturesSymbols {
				sc := futureskline.NewScraper(sym, "binance", p.storage)
				if plan.Mode == DirectionReverse {
					sc.SetReverse(plan.Scope.StartTime, plan.Scope.EndTime)
				}
				plugins = append(plugins, &pluginAdapter{
					kind:      KindKline,
					market:    MarketFutures,
					symbol:    sym,
					direction: plan.Mode,
					scraper:   sc,
				})
			}
		case item.Kind == KindOrderBook && item.Market == MarketSpot:
			for _, sym := range plan.SpotSymbols {
				sc := spotorderbook.NewScraper(sym, "spot", "binance", p.storage)
				plugins = append(plugins, &pluginAdapter{
					kind:      KindOrderBook,
					market:    MarketSpot,
					symbol:    sym,
					direction: plan.Mode,
					scraper:   sc,
				})
			}
		case item.Kind == KindOrderBook && item.Market == MarketFutures:
			for _, sym := range plan.FuturesSymbols {
				sc := futuresorderbook.NewScraper(sym, "binance", p.storage)
				plugins = append(plugins, &pluginAdapter{
					kind:      KindOrderBook,
					market:    MarketFutures,
					symbol:    sym,
					direction: plan.Mode,
					scraper:   sc,
				})
			}
		case item.Kind == KindFundingRate && item.Market == MarketFutures:
			if len(plan.FuturesSymbols) == 0 {
				logger.NewSystemHelper().Warning("fundingrate_skipped_no_futures_symbols", map[string]any{
					"mode": string(plan.Mode),
				})
				continue
			}
			sc := fundingrate.NewScraper(plan.FuturesSymbols, "binance", p.storage)
			if plan.Mode == DirectionReverse {
				if rs, ok := sc.(interface {
					SetReverse(time.Time, time.Time)
				}); ok {
					rs.SetReverse(plan.Scope.StartTime, plan.Scope.EndTime)
				}
			}
			plugins = append(plugins, &pluginAdapter{
				kind:      KindFundingRate,
				market:    MarketFutures,
				symbol:    "",
				direction: plan.Mode,
				scraper:   sc,
			})
		case item.Kind == KindOpenInterest && item.Market == MarketFutures:
			sc := bscraper.NewOpenInterestScraper(plan.FuturesSymbols, "binance", p.storage)
			if plan.Mode == DirectionReverse {
				sc.SetReverse(plan.Scope.StartTime, plan.Scope.EndTime)
			}
			plugins = append(plugins, &pluginAdapter{
				kind:      KindOpenInterest,
				market:    MarketFutures,
				symbol:    strings.Join(plan.FuturesSymbols, ","),
				direction: plan.Mode,
				scraper:   sc,
			})
		}
	}

	return plugins
}

func (p *Planner) CanReverse(kind Kind, market Market) bool {
	_ = market
	return SupportedReverseModes[kind]
}

type pluginAdapter struct {
	kind      Kind
	market    Market
	symbol    string
	direction Direction
	scraper   scraper.Scraper
}

func (p *pluginAdapter) Kind() string {
	return string(p.kind)
}

func (p *pluginAdapter) Market() string {
	return string(p.market)
}

func (p *pluginAdapter) Symbol() string {
	return p.symbol
}

func (p *pluginAdapter) Run(ctx context.Context) {
	logger.NewScraperLifecycleHelper().Started(string(p.kind), string(p.market), p.symbol, string(p.direction))
	p.scraper.Run(ctx)
	logger.NewScraperLifecycleHelper().Stopped(string(p.kind), string(p.market), p.symbol, string(p.direction))
}

func (p *pluginAdapter) Stop() {
	p.scraper.Stop()
}
