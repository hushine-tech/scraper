package common

import (
	"testing"

	"github.com/hushine-tech/scraper/internal/config"
)

func TestBuildCollectionPlanForwardDefaultsToAllKinds(t *testing.T) {
	cfg := config.BinanceConfig{
		SpotSymbols:    []string{"btcusdt"},
		FuturesSymbols: []string{"btcusdt"},
	}
	plan := BuildCollectionPlan(cfg, "forward")

	if plan.Mode != DirectionForward {
		t.Fatalf("expected forward mode, got %s", plan.Mode)
	}
	if len(plan.EnabledKinds) != 6 {
		t.Fatalf("expected 6 enabled kinds, got %d", len(plan.EnabledKinds))
	}
	if !plan.Scope.Realtime {
		t.Fatalf("forward mode must be realtime")
	}
}

func TestBuildCollectionPlanReverseSkipsOrderbookWithWarning(t *testing.T) {
	cfg := config.BinanceConfig{
		SpotSymbols:    []string{"btcusdt"},
		FuturesSymbols: []string{"btcusdt"},
		Reverse: config.ReverseConfig{
			SpotKline:        true,
			FuturesKline:     true,
			FundingRate:      true,
			SpotOrderBook:    true,
			FuturesOrderBook: true,
		},
	}
	plan := BuildCollectionPlan(cfg, "reverse")

	for _, kind := range plan.EnabledKinds {
		if kind.Kind == KindOrderBook {
			t.Fatalf("orderbook must not be enabled in reverse plan")
		}
	}
	if len(plan.Warnings) < 2 {
		t.Fatalf("expected reverse warnings for orderbook and range fallback")
	}
}

func TestBuildCollectionPlanReverseDefaultsToSupportedKinds(t *testing.T) {
	cfg := config.BinanceConfig{
		SpotSymbols:    []string{"btcusdt"},
		FuturesSymbols: []string{"btcusdt"},
	}
	plan := BuildCollectionPlan(cfg, "reverse")

	if plan.Mode != DirectionReverse {
		t.Fatalf("expected reverse mode, got %s", plan.Mode)
	}
	if plan.Scope.Realtime {
		t.Fatalf("reverse mode must be non-realtime")
	}
	if len(plan.EnabledKinds) != 4 {
		t.Fatalf("expected 4 enabled kinds in reverse defaults, got %d", len(plan.EnabledKinds))
	}
	for _, kind := range plan.EnabledKinds {
		if kind.Kind == KindOrderBook {
			t.Fatalf("orderbook must not be enabled in reverse defaults")
		}
	}
}

func TestBuildCollectionPlanUnknownModeFallsBackToForwardWithWarning(t *testing.T) {
	cfg := config.BinanceConfig{
		SpotSymbols:    []string{"btcusdt"},
		FuturesSymbols: []string{"btcusdt"},
	}
	plan := BuildCollectionPlan(cfg, "ReVeRsE-invalid")

	if plan.Mode != DirectionForward {
		t.Fatalf("expected fallback to forward mode, got %s", plan.Mode)
	}
	if len(plan.Warnings) == 0 {
		t.Fatalf("expected warning for unsupported mode")
	}
}
