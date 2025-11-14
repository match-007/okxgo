package main

import "testing"

func TestRiskAdapterVolTargetScaling(t *testing.T) {
	ra := NewRiskAdapter(RiskConfig{RiskTarget: 0.3, ATRPeriod: 14, MaxLeverage: 2.0}, 15)
	st := &riskState{atr: newATRTracker(14)}
	st.atr.value = 500
	st.atr.ready = true

	scaled := ra.applyVolTarget(1.0, 1000, st)
	if scaled >= 1.0 {
		t.Fatalf("expected vol targeting to reduce target, got %.3f", scaled)
	}
}

func TestRiskAdapterDrawdownSummaryFlush(t *testing.T) {
	cfg := RiskConfig{DDCircuit: DDCircuitConfig{Enable: boolPtr(true), Threshold: 0.1, CooldownBars: 2}}
	ra := NewRiskAdapter(cfg, 15)
	ra.peakEquity = 1.0
	ra.equity = 0.7
	ra.evaluateDrawdown(123)
	ra.lastTs = 123

	summary := ra.Summary()
	if len(summary.DDWindows) != 1 {
		t.Fatalf("expected 1 DD window, got %d", len(summary.DDWindows))
	}
	window := summary.DDWindows[0]
	if window.Start != 123 || window.End != 123 {
		t.Fatalf("unexpected dd window: %+v", window)
	}
}
