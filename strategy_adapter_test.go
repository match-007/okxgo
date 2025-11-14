package main

import "testing"

func TestStrategyAdapterRegimeWeights(t *testing.T) {
	cfg := StrategyConfig{TrendGain: 1.0, MRGain: 1.0, BreakoutGain: 1.0}
	sa := NewStrategyAdapter(cfg, RiskConfig{}, 15)

	trend := sa.computeWeights("trending")
	if trend.trend <= 1.0 {
		t.Fatalf("expected trend weight to increase in trending regime, got %.2f", trend.trend)
	}
	if trend.mr >= 1.0 {
		t.Fatalf("expected mr weight to drop in trending regime, got %.2f", trend.mr)
	}

	rangeWeights := sa.computeWeights("ranging")
	if rangeWeights.mr <= 1.0 {
		t.Fatalf("expected mr weight to increase in ranging regime, got %.2f", rangeWeights.mr)
	}
	if rangeWeights.trend >= 1.0 {
		t.Fatalf("expected trend weight to decline in ranging regime, got %.2f", rangeWeights.trend)
	}
}

func TestStrategyAdapterFallbackGate(t *testing.T) {
	cfg := StrategyConfig{Fallback: FallbackConfig{Enable: boolPtr(true), Scale: 0.25, MAPeriod: 20}}
	sa := NewStrategyAdapter(cfg, RiskConfig{}, 15)

	if !sa.shouldUseFallback(0.05, false) {
		t.Fatalf("fallback should trigger when position is light and signal weak")
	}
	if sa.shouldUseFallback(0.2, false) {
		t.Fatalf("fallback must not trigger when exposure is already material")
	}
	if sa.shouldUseFallback(0.05, true) {
		t.Fatalf("fallback must not override strong regime detections")
	}
}
