package main

import (
	"math"

	"Mod/src/backtest"
)

type StrategyAdapter struct {
	cfg             StrategyConfig
	riskCfg         RiskConfig
	barMinutes      int
	higherTFMinutes int
	name            string

	states map[string]*strategyState

	mtfChecks    int
	mtfAligned   int
	mtfFiltered  int
	fallbackUse  int
	regimeCounts map[string]int
}

func NewStrategyAdapter(cfg StrategyConfig, riskCfg RiskConfig, barMinutes int) *StrategyAdapter {
	higher := normalizeTimeframe(cfg.MTF.HigherTF)
	if higher <= 0 {
		higher = barMinutes
	}
	return &StrategyAdapter{
		cfg:             cfg,
		riskCfg:         riskCfg,
		barMinutes:      barMinutes,
		higherTFMinutes: higher,
		name:            "regime_dynamic_v1",
		states:          make(map[string]*strategyState),
		regimeCounts:    make(map[string]int),
	}
}

func (sa *StrategyAdapter) Name() string { return sa.name }

func (sa *StrategyAdapter) OnCandle(c backtest.Candle) []backtest.Signal {
	st := sa.ensureState(c.InstID)
	st.update(c, sa.barMinutes, sa.higherTFMinutes)

	if len(st.closes) < 50 {
		return nil
	}

	trendSignal := sa.computeTrendSignal(st)
	mrSignal := sa.computeMeanReversionSignal(st)
	breakoutSignal := sa.computeBreakoutSignal(st)

	regime := sa.detectRegime(st)
	weights := sa.computeWeights(regime)

	alignment := sa.multiTimeframeScaler(trendSignal, st)
	trendComponent := weights.trend * trendSignal * alignment
	mrComponent := weights.mr * mrSignal * alignment
	breakoutComponent := weights.breakout * breakoutSignal * alignment
	posRaw := trendComponent + mrComponent + breakoutComponent

	strongSignal := regime == "trending" || regime == "ranging"
	if !boolValue(sa.cfg.Fallback.Enable, true) {
		strongSignal = true
	}
	fallbackComponent := 0.0
	if sa.shouldUseFallback(posRaw, strongSignal) {
		fallbackComponent = sa.computeFallbackSignal(st)
		if fallbackComponent != 0 {
			posRaw += fallbackComponent
			sa.fallbackUse++
		}
	}

	maxAbs := sa.maxAbsPosition()
	pos := clamp(posRaw, -maxAbs, maxAbs)
	if math.Abs(pos) < 1e-4 {
		return nil
	}

	side := "close"
	size := 0.0
	if pos > 0 {
		side, size = "buy", pos
	} else {
		side, size = "sell", -pos
	}

	dominant := dominantComponent(trendComponent, mrComponent, breakoutComponent, fallbackComponent)
	meta := map[string]any{
		"trend_component":    trendComponent,
		"mr_component":       mrComponent,
		"breakout_component": breakoutComponent,
		"fallback_component": fallbackComponent,
		"regime":             regime,
		"mtf_alignment":      alignment,
		"sub_strategy":       dominant,
		"atr":                st.atr.Value(),
	}

	return []backtest.Signal{{
		InstID: c.InstID,
		Side:   side,
		Size:   size,
		Price:  c.C,
		Tag:    "regime_target",
		Meta:   meta,
	}}
}

func (sa *StrategyAdapter) OnTicker(backtest.Ticker) []backtest.Signal { return nil }

func (sa *StrategyAdapter) maxAbsPosition() float64 {
	if sa.riskCfg.MaxAbsPosition > 0 {
		return sa.riskCfg.MaxAbsPosition
	}
	return 1.0
}

func (sa *StrategyAdapter) ensureState(inst string) *strategyState {
	if st, ok := sa.states[inst]; ok {
		return st
	}
	adxPeriod := nonZeroOr(sa.cfg.Regime.TrendAdxPeriod, 14)
	st := &strategyState{
		closes: make([]float64, 0, 1024),
		highs:  make([]float64, 0, 1024),
		lows:   make([]float64, 0, 1024),
		adx:    newADXTracker(adxPeriod),
		atr:    newATRTracker(nonZeroOr(sa.riskCfg.ATRPeriod, 14)),
	}
	sa.states[inst] = st
	return st
}

func (sa *StrategyAdapter) computeTrendSignal(st *strategyState) float64 {
	fast := emaLast(st.closes, 8)
	slow := emaLast(st.closes, 32)
	if slow == 0 {
		return 0
	}
	slope := (fast - slow) / slow
	return softsign(slope)
}

func (sa *StrategyAdapter) computeMeanReversionSignal(st *strategyState) float64 {
	period := nonZeroOr(sa.cfg.Regime.RangeBwPeriod, 20)
	if len(st.closes) < period {
		return 0
	}
	mean := smaLast(st.closes, period)
	std := stdLast(st.closes, period)
	if std <= 0 {
		return 0
	}
	z := (st.closes[len(st.closes)-1] - mean) / std
	return -softsign(z)
}

func (sa *StrategyAdapter) computeBreakoutSignal(st *strategyState) float64 {
	lookback := 2 * nonZeroOr(sa.cfg.Regime.RangeBwPeriod, 20)
	if len(st.highs) < lookback || len(st.lows) < lookback {
		return 0
	}
	price := st.closes[len(st.closes)-1]
	hi := maxLast(st.highs, lookback)
	lo := minLast(st.lows, lookback)
	if price >= hi {
		return 1
	}
	if price <= lo {
		return -1
	}
	mid := (hi + lo) / 2
	if mid == 0 {
		return 0
	}
	return softsign((price - mid) / (hi - lo + 1e-9))
}

func (sa *StrategyAdapter) detectRegime(st *strategyState) string {
	if !boolValue(sa.cfg.Regime.Enable, true) {
		return sa.recordRegime("neutral")
	}
	adx := st.adx.Value()
	if adx >= sa.cfg.Regime.TrendAdxTh {
		return sa.recordRegime("trending")
	}
	bw := st.rangeBandwidth(nonZeroOr(sa.cfg.Regime.RangeBwPeriod, 20))
	if bw > 0 && bw <= sa.cfg.Regime.RangeBwTh {
		return sa.recordRegime("ranging")
	}
	return sa.recordRegime("neutral")
}

func (sa *StrategyAdapter) computeWeights(regime string) regimeWeights {
	w := regimeWeights{
		trend:    nonZeroOrFloat(sa.cfg.TrendGain, 2.0),
		mr:       nonZeroOrFloat(sa.cfg.MRGain, 0.7),
		breakout: nonZeroOrFloat(sa.cfg.BreakoutGain, 1.0),
	}
	switch regime {
	case "trending":
		w.trend *= 1.35
		w.breakout *= 1.25
		w.mr *= 0.65
	case "ranging":
		w.mr *= 1.5
		w.trend *= 0.6
		w.breakout *= 0.6
	}
	return w
}

func (sa *StrategyAdapter) multiTimeframeScaler(trendSignal float64, st *strategyState) float64 {
	if !boolValue(sa.cfg.MTF.ConfirmEnable, true) {
		return 1.0
	}
	sa.mtfChecks++
	fast := st.mtfFast
	slow := st.mtfSlow
	if fast == 0 || slow == 0 {
		return 1.0
	}
	diff := fast - slow
	if diff == 0 || trendSignal == 0 {
		return 1.0
	}
	if boolValue(sa.cfg.MTF.TrendAlign, true) {
		if diff*trendSignal < 0 {
			sa.mtfFiltered++
			return 0.6
		}
		sa.mtfAligned++
		return 1.25
	}
	return 1.0
}

func (sa *StrategyAdapter) shouldUseFallback(pos float64, strong bool) bool {
	if !boolValue(sa.cfg.Fallback.Enable, true) {
		return false
	}
	if strong {
		return false
	}
	return math.Abs(pos) < 0.1
}

func (sa *StrategyAdapter) computeFallbackSignal(st *strategyState) float64 {
	period := nonZeroOr(sa.cfg.Fallback.MAPeriod, 100)
	if len(st.closes) < period {
		return 0
	}
	ma := smaLast(st.closes, period)
	price := st.closes[len(st.closes)-1]
	scale := nonZeroOrFloat(sa.cfg.Fallback.Scale, 0.25)
	if price > ma {
		return scale
	}
	if price < ma {
		return -scale
	}
	return 0
}

type regimeWeights struct {
	trend    float64
	mr       float64
	breakout float64
}

type strategyState struct {
	closes []float64
	highs  []float64
	lows   []float64

	adx *adxTracker
	atr *atrTracker

	mtfFast   float64
	mtfSlow   float64
	lastClose float64
}

func (st *strategyState) update(c backtest.Candle, baseTF, higherTF int) {
	st.closes = appendWithLimit(st.closes, c.C, 3000)
	st.highs = appendWithLimit(st.highs, c.H, 3000)
	st.lows = appendWithLimit(st.lows, c.L, 3000)

	st.adx.Update(c.H, c.L, c.C)
	if st.atr != nil {
		st.atr.Update(c.H, c.L, st.lastClose)
	}

	ratio := higherTF / maxInts(1, baseTF)
	fastPeriod := maxInts(4*ratio, 8)
	slowPeriod := maxInts(8*ratio, 16)
	st.mtfFast = emaUpdate(st.mtfFast, c.C, fastPeriod)
	st.mtfSlow = emaUpdate(st.mtfSlow, c.C, slowPeriod)
	st.lastClose = c.C
}

func (st *strategyState) rangeBandwidth(period int) float64 {
	if len(st.closes) < period || period <= 0 {
		return 0
	}
	mean := smaLast(st.closes, period)
	std := stdLast(st.closes, period)
	if mean <= 0 {
		return 0
	}
	upper := mean + 2*std
	lower := mean - 2*std
	if upper <= lower {
		return 0
	}
	return (upper - lower) / mean
}

func boolValue(ptr *bool, def bool) bool {
	if ptr == nil {
		return def
	}
	return *ptr
}

func emaUpdate(prev, value float64, period int) float64 {
	if period <= 1 {
		return value
	}
	alpha := 2.0 / (float64(period) + 1)
	if prev == 0 {
		return value
	}
	return alpha*value + (1-alpha)*prev
}

func dominantComponent(trend, mr, breakout, fallback float64) string {
	if fallback != 0 {
		return "fallback"
	}
	maxVal := math.Abs(trend)
	label := "trend"
	if math.Abs(mr) > maxVal {
		maxVal = math.Abs(mr)
		label = "mr"
	}
	if math.Abs(breakout) > maxVal {
		label = "breakout"
	}
	return label
}

type StrategySummary struct {
	RegimeCounts  map[string]int `json:"regime_counts"`
	MTFChecks     int            `json:"mtf_checks"`
	MTFAligned    int            `json:"mtf_aligned"`
	MTFFiltered   int            `json:"mtf_filtered"`
	FallbackUsage int            `json:"fallback_usage"`
}

func (sa *StrategyAdapter) Summary() StrategySummary {
	counts := make(map[string]int, len(sa.regimeCounts))
	for k, v := range sa.regimeCounts {
		counts[k] = v
	}
	return StrategySummary{
		RegimeCounts:  counts,
		MTFChecks:     sa.mtfChecks,
		MTFAligned:    sa.mtfAligned,
		MTFFiltered:   sa.mtfFiltered,
		FallbackUsage: sa.fallbackUse,
	}
}

func (sa *StrategyAdapter) recordRegime(regime string) string {
	sa.regimeCounts[regime]++
	return regime
}
