package common

import (
	"fmt"
	"strings"
	"time"

	"github.com/hushine-tech/scraper/internal/config"
)

type KindSelection struct {
	Kind   Kind
	Market Market
}

type PlanScope struct {
	Realtime  bool
	StartTime time.Time
	EndTime   time.Time
}

type CollectionPlan struct {
	Mode           Direction
	SpotSymbols    []string
	FuturesSymbols []string
	EnabledKinds   []KindSelection
	Scope          PlanScope
	Warnings       []string
}

func BuildCollectionPlan(cfg config.ExchangeConfig, mode string) CollectionPlan {
	normalizedMode := strings.ToLower(strings.TrimSpace(mode))
	plan := CollectionPlan{
		Mode:           DirectionForward,
		SpotSymbols:    cfg.SpotSymbols,
		FuturesSymbols: cfg.FuturesSymbols,
		Scope: PlanScope{
			Realtime: true,
		},
	}

	if normalizedMode == string(DirectionReverse) {
		plan.Mode = DirectionReverse
		plan.Scope.Realtime = false
		plan.EnabledKinds = append(plan.EnabledKinds, reverseKinds(cfg)...)
		if cfg.Reverse.SpotOrderBook {
			plan.Warnings = append(plan.Warnings, "reverse mode does not support spot orderbook; skipped")
		}
		if cfg.Reverse.FuturesOrderBook {
			plan.Warnings = append(plan.Warnings, "reverse mode does not support futures orderbook; skipped")
		}
		start, end, rangeWarnings := normalizeReverseRange(cfg.Reverse.StartTime, cfg.Reverse.EndTime)
		plan.Scope.StartTime = start
		plan.Scope.EndTime = end
		plan.Warnings = append(plan.Warnings, rangeWarnings...)
		return plan
	}

	if normalizedMode != "" && normalizedMode != string(DirectionForward) {
		plan.Warnings = append(plan.Warnings, fmt.Sprintf("unsupported mode %q, fallback to forward", mode))
	}

	plan.EnabledKinds = append(plan.EnabledKinds, forwardKinds(cfg)...)
	return plan
}

func forwardKinds(cfg config.ExchangeConfig) []KindSelection {
	fwd := cfg.Forward
	if !fwd.SpotKline && !fwd.FuturesKline && !fwd.SpotOrderBook && !fwd.FuturesOrderBook && !fwd.FundingRate && !fwd.FuturesOpenInterest {
		fwd = config.ForwardConfig{
			SpotKline:           true,
			FuturesKline:        true,
			SpotOrderBook:       true,
			FuturesOrderBook:    true,
			FundingRate:         true,
			FuturesOpenInterest: true,
		}
	}

	kinds := make([]KindSelection, 0, 6)
	if fwd.SpotKline {
		kinds = append(kinds, KindSelection{Kind: KindKline, Market: MarketSpot})
	}
	if fwd.FuturesKline {
		kinds = append(kinds, KindSelection{Kind: KindKline, Market: MarketFutures})
	}
	if fwd.SpotOrderBook {
		kinds = append(kinds, KindSelection{Kind: KindOrderBook, Market: MarketSpot})
	}
	if fwd.FuturesOrderBook {
		kinds = append(kinds, KindSelection{Kind: KindOrderBook, Market: MarketFutures})
	}
	if fwd.FundingRate {
		kinds = append(kinds, KindSelection{Kind: KindFundingRate, Market: MarketFutures})
	}
	if fwd.FuturesOpenInterest {
		kinds = append(kinds, KindSelection{Kind: KindOpenInterest, Market: MarketFutures})
	}
	return kinds
}

func reverseKinds(cfg config.ExchangeConfig) []KindSelection {
	rev := cfg.Reverse
	if !rev.SpotKline && !rev.FuturesKline && !rev.FundingRate && !rev.FuturesOpenInterest {
		rev = config.ReverseConfig{
			SpotKline:           true,
			FuturesKline:        true,
			FundingRate:         true,
			FuturesOpenInterest: true,
		}
	}

	kinds := make([]KindSelection, 0, 4)
	if rev.SpotKline {
		kinds = append(kinds, KindSelection{Kind: KindKline, Market: MarketSpot})
	}
	if rev.FuturesKline {
		kinds = append(kinds, KindSelection{Kind: KindKline, Market: MarketFutures})
	}
	if rev.FundingRate {
		kinds = append(kinds, KindSelection{Kind: KindFundingRate, Market: MarketFutures})
	}
	if rev.FuturesOpenInterest {
		kinds = append(kinds, KindSelection{Kind: KindOpenInterest, Market: MarketFutures})
	}
	return kinds
}

func normalizeReverseRange(rawStart, rawEnd string) (time.Time, time.Time, []string) {
	warnings := []string{}
	now := time.Now().UTC()
	start := now.AddDate(0, -1, 0)
	end := now

	if rawStart != "" {
		if parsed, err := time.Parse(time.RFC3339, rawStart); err == nil {
			start = parsed.UTC()
		} else {
			warnings = append(warnings, fmt.Sprintf("invalid start_time %q, fallback to now-30d", rawStart))
		}
	} else {
		warnings = append(warnings, "empty start_time, fallback to now-30d")
	}

	if rawEnd != "" {
		if parsed, err := time.Parse(time.RFC3339, rawEnd); err == nil {
			end = parsed.UTC()
		} else {
			warnings = append(warnings, fmt.Sprintf("invalid end_time %q, fallback to now", rawEnd))
		}
	} else {
		warnings = append(warnings, "empty end_time, fallback to now")
	}

	if !end.After(start) {
		warnings = append(warnings, fmt.Sprintf("end_time (%s) <= start_time (%s), fallback to [now-30d, now]", end.Format(time.RFC3339), start.Format(time.RFC3339)))
		start = now.AddDate(0, -1, 0)
		end = now
	}

	return start, end, warnings
}
