package main

import (
	"math"
	"testing"

	"Mod/src/backtest"
)

func TestSummarizeAttribution(t *testing.T) {
	trades := []backtest.Trade{
		{SubStrategy: "trend", Return: 0.02},
		{SubStrategy: "trend", Return: -0.01},
		{SubStrategy: "mr", Return: 0.005},
	}
	stats := summarizeAttribution(trades)
	trend := stats["trend"]
	if trend.Trades != 2 || math.Abs(trend.TotalReturn-0.01) > 1e-9 {
		t.Fatalf("unexpected trend stats: %+v", trend)
	}
	if trend.WinRate <= 0.49 || trend.WinRate >= 0.51 {
		t.Fatalf("expected 50%% win rate, got %.2f", trend.WinRate)
	}
	if stats["mr"].Trades != 1 || stats["mr"].AvgWin <= 0 {
		t.Fatalf("mr attribution not captured: %+v", stats["mr"])
	}
}

func TestCalcVolStats(t *testing.T) {
	curve := []backtest.BarRecord{
		{Ret: 0.01},
		{Ret: -0.005},
		{Ret: 0.0},
		{Ret: 0.007},
	}
	vs := calcVolStats(curve, 15, 0.5)
	if vs.Target != 0.5 {
		t.Fatalf("expected target passthrough, got %.2f", vs.Target)
	}
	if vs.Actual <= 0 {
		t.Fatalf("expected positive realized vol, got %.4f", vs.Actual)
	}
}
