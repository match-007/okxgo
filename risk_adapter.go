package main

import (
	"math"

	"Mod/src/backtest"
)

type RiskAdapter struct {
	cfg        RiskConfig
	barMinutes int

	states map[string]*riskState

	equity     float64
	peakEquity float64
	ddCooldown int
	ddScaler   float64
	stopCounts map[string]int
	ddActive   bool
	ddStart    int64
	ddEvents   []DDWindow
	lastTs     int64
}

func NewRiskAdapter(cfg RiskConfig, barMinutes int) *RiskAdapter {
	return &RiskAdapter{
		cfg:        cfg,
		barMinutes: barMinutes,
		states:     make(map[string]*riskState),
		equity:     1.0,
		peakEquity: 1.0,
		ddScaler:   1.0,
		stopCounts: make(map[string]int),
	}
}

func (ra *RiskAdapter) OnCandle(c backtest.Candle) {
	st := ra.ensureState(c.InstID)
	st.atr.Update(c.H, c.L, st.lastClose)
	if st.position != 0 && st.lastClose > 0 && c.C > 0 {
		ret := math.Log(c.C / st.lastClose)
		ra.equity *= math.Exp(st.position * ret)
		if st.position > 0 {
			if c.H > st.maxFavorable {
				st.maxFavorable = c.H
			}
		} else if st.position < 0 {
			if st.maxFavorable == 0 || c.L < st.maxFavorable {
				st.maxFavorable = c.L
			}
		}
	}
	st.lastClose = c.C
	if ra.equity > ra.peakEquity {
		ra.peakEquity = ra.equity
	}
	ra.evaluateDrawdown(c.T)
	ra.lastTs = c.T
}

func (ra *RiskAdapter) OnTicker(backtest.Ticker) {}

func (ra *RiskAdapter) Approve(inst string, current, target, price float64, holdingBars int) (float64, []backtest.Action) {
	st := ra.ensureState(inst)
	st.holding = holdingBars

	if act := ra.checkStops(inst, price, st); act != nil {
		st.resetPosition()
		return 0, []backtest.Action{*act}
	}

	scaled := ra.applyVolTarget(target, price, st)
	maxAbs := nonZeroOrFloat(ra.cfg.MaxAbsPosition, 1.0)
	scaled = clamp(scaled, -maxAbs, maxAbs)
	if ra.cfg.MaxLeverage > 0 {
		scaled = clamp(scaled, -ra.cfg.MaxLeverage, ra.cfg.MaxLeverage)
	}

	if current == 0 && scaled != 0 && price > 0 {
		st.entryPrice = price
		st.maxFavorable = price
	}
	if scaled == 0 {
		st.resetPosition()
	} else {
		st.position = scaled
	}
	return scaled, nil
}

func (ra *RiskAdapter) ensureState(inst string) *riskState {
	if st, ok := ra.states[inst]; ok {
		return st
	}
	st := &riskState{
		atr: newATRTracker(nonZeroOr(ra.cfg.ATRPeriod, 14)),
	}
	ra.states[inst] = st
	return st
}

func (ra *RiskAdapter) applyVolTarget(target, price float64, st *riskState) float64 {
	atr := st.atr.Value()
	if atr <= 0 || price <= 0 {
		return target
	}
	riskTarget := nonZeroOrFloat(ra.cfg.RiskTarget, 0.6) * ra.ddScaler
	perBarVol := atr / price
	if perBarVol <= 0 {
		return target
	}
	scale := riskTarget / math.Max(perBarVol, 1e-6)
	if ra.cfg.MaxLeverage > 0 {
		scale = math.Min(scale, ra.cfg.MaxLeverage)
	}
	return target * scale
}

func (ra *RiskAdapter) checkStops(inst string, price float64, st *riskState) *backtest.Action {
	if st.position == 0 || price <= 0 {
		return nil
	}
	atr := st.atr.Value()
	if atr <= 0 || st.entryPrice <= 0 {
		return nil
	}
	stopK := nonZeroOrFloat(ra.cfg.ATRStopK, 2.5)
	trailK := nonZeroOrFloat(ra.cfg.ATRTrailK, 3.0)

	stopDist := stopK * atr
	if st.position > 0 && price <= st.entryPrice-stopDist {
		ra.bumpStop("atr_stop")
		return &backtest.Action{InstID: inst, Type: "close", Reason: "atr_stop", Size: math.Abs(st.position), Price: price}
	}
	if st.position < 0 && price >= st.entryPrice+stopDist {
		ra.bumpStop("atr_stop")
		return &backtest.Action{InstID: inst, Type: "close", Reason: "atr_stop", Size: math.Abs(st.position), Price: price}
	}

	trailDist := trailK * atr
	if st.position > 0 && st.maxFavorable > 0 && price <= st.maxFavorable-trailDist {
		ra.bumpStop("atr_trail")
		return &backtest.Action{InstID: inst, Type: "close", Reason: "atr_trail", Size: math.Abs(st.position), Price: price}
	}
	if st.position < 0 && st.maxFavorable > 0 && price >= st.maxFavorable+trailDist {
		ra.bumpStop("atr_trail")
		return &backtest.Action{InstID: inst, Type: "close", Reason: "atr_trail", Size: math.Abs(st.position), Price: price}
	}
	return nil
}

func (ra *RiskAdapter) evaluateDrawdown(ts int64) {
	if ra.peakEquity <= 0 {
		ra.peakEquity = 1.0
	}
	dd := (ra.peakEquity - ra.equity) / ra.peakEquity
	if boolValue(ra.cfg.DDCircuit.Enable, true) && dd >= ra.cfg.DDCircuit.Threshold && ra.ddCooldown == 0 {
		ra.ddScaler = 0.5
		ra.ddCooldown = nonZeroOr(ra.cfg.DDCircuit.CooldownBars, 96)
		ra.ddActive = true
		ra.ddStart = ts
	}
	if ra.ddCooldown > 0 {
		ra.ddCooldown--
		if ra.ddCooldown == 0 {
			ra.ddScaler = 1.0
			if ra.ddActive {
				ra.ddEvents = append(ra.ddEvents, DDWindow{Start: ra.ddStart, End: ts})
				ra.ddActive = false
			}
		}
	}
}

func (ra *RiskAdapter) bumpStop(reason string) {
	if reason == "" {
		reason = "stop"
	}
	ra.stopCounts[reason]++
}

type DDWindow struct {
	Start int64 `json:"start"`
	End   int64 `json:"end"`
}

type RiskSummary struct {
	StopCounts map[string]int `json:"stop_counts"`
	DDWindows  []DDWindow     `json:"dd_windows"`
}

func (ra *RiskAdapter) Summary() RiskSummary {
	if ra.ddActive {
		end := ra.lastTs
		if end == 0 && len(ra.ddEvents) > 0 {
			end = ra.ddEvents[len(ra.ddEvents)-1].End
		}
		ra.ddEvents = append(ra.ddEvents, DDWindow{Start: ra.ddStart, End: end})
		ra.ddActive = false
	}
	counts := make(map[string]int, len(ra.stopCounts))
	for k, v := range ra.stopCounts {
		counts[k] = v
	}
	windows := make([]DDWindow, len(ra.ddEvents))
	copy(windows, ra.ddEvents)
	return RiskSummary{
		StopCounts: counts,
		DDWindows:  windows,
	}
}

type riskState struct {
	atr *atrTracker

	position     float64
	entryPrice   float64
	maxFavorable float64
	lastClose    float64
	holding      int
}

func (st *riskState) resetPosition() {
	st.position = 0
	st.entryPrice = 0
	st.maxFavorable = 0
}
