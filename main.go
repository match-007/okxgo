package main

import (
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"Mod/src/backtest"
	_ "Mod/src/netboot"
	"Mod/src/portfolio"
	"Mod/src/strategy"
	"Mod/src/stream"
)

// ==================== Backtest Config ====================

type BacktestConfig struct {
	StartDate          string   `json:"start_date"`
	EndDate            string   `json:"end_date"`
	InitialCash        float64  `json:"initial_cash"`
	Instruments        []string `json:"instruments"`
	Timeframe          string   `json:"timeframe"`
	DataSource         string   `json:"data_source"`
	DataPath           string   `json:"data_path"`
	AutoFetchIfMissing bool     `json:"auto_fetch_if_missing"`
	UseRisk            bool     `json:"use_risk"`
	UsePortfolio       bool     `json:"use_portfolio"`
	BarsLimit          int      `json:"bars_limit"`

	// Legacy flat strategy fields (kept for backward compatibility)
	StrategyRiskTarget     float64 `json:"strategy_risk_target,omitempty"`
	StrategyMaxAbsPosition float64 `json:"strategy_max_abs_position,omitempty"`
	StrategyMaxLeverage    float64 `json:"strategy_max_leverage,omitempty"`
	StrategyTrendGain      float64 `json:"strategy_trend_gain,omitempty"`
	StrategyMRGain         float64 `json:"strategy_mr_gain,omitempty"`
	StrategyBreakoutGain   float64 `json:"strategy_breakout_gain,omitempty"`
	FallbackScale          float64 `json:"fallback_scale,omitempty"`

	Strategy     StrategyConfig     `json:"strategy"`
	Risk         RiskConfig         `json:"risk"`
	Optimization OptimizationConfig `json:"optimization"`

	DebugFallbackMA    bool `json:"debug_fallback_ma"`
	DebugFallbackForce bool `json:"debug_fallback_force"`
}

// StrategyConfig captures tunable knobs for QuantMasterElite plus new regime controls.
type StrategyConfig struct {
	TrendGain    float64        `json:"trend_gain"`
	MRGain       float64        `json:"mr_gain"`
	BreakoutGain float64        `json:"breakout_gain"`
	Regime       RegimeConfig   `json:"regime"`
	MTF          MTFConfig      `json:"mtf"`
	Fallback     FallbackConfig `json:"fallback"`
}

type RegimeConfig struct {
	Enable         *bool   `json:"enable"`
	TrendAdxPeriod int     `json:"trend_adx_period"`
	TrendAdxTh     float64 `json:"trend_adx_th"`
	RangeBwPeriod  int     `json:"range_bw_period"`
	RangeBwTh      float64 `json:"range_bw_th"`
}

type MTFConfig struct {
	ConfirmEnable *bool  `json:"confirm_enable"`
	HigherTF      string `json:"higher_tf"`
	TrendAlign    *bool  `json:"trend_align"`
}

type FallbackConfig struct {
	Enable   *bool   `json:"enable"`
	Scale    float64 `json:"scale"`
	MAPeriod int     `json:"ma_period"`
}

type RiskConfig struct {
	RiskTarget     float64         `json:"risk_target"`
	ATRPeriod      int             `json:"atr_period"`
	ATRStopK       float64         `json:"atr_stop_k"`
	ATRTrailK      float64         `json:"atr_trail_k"`
	MaxLeverage    float64         `json:"max_leverage"`
	MaxAbsPosition float64         `json:"max_abs_position"`
	DDCircuit      DDCircuitConfig `json:"dd_circuit"`
}

type DDCircuitConfig struct {
	Enable       *bool   `json:"enable"`
	Threshold    float64 `json:"threshold"`
	CooldownBars int     `json:"cooldown_bars"`
}

type OptimizationConfig struct {
	Enable     *bool `json:"enable"`
	MaxSamples int   `json:"max_samples"`
	Seed       int64 `json:"seed"`
}

func (c *BacktestConfig) normalize() {
	if c.BarsLimit == 0 {
		c.BarsLimit = 2000
	}
	if c.StrategyRiskTarget <= 0 {
		c.StrategyRiskTarget = 1.0
	}
	if c.StrategyMaxAbsPosition <= 0 {
		c.StrategyMaxAbsPosition = 2.0
	}
	if c.StrategyMaxLeverage <= 0 {
		c.StrategyMaxLeverage = 3.0
	}
	if c.StrategyTrendGain <= 0 {
		c.StrategyTrendGain = 3.0
	}
	if c.StrategyMRGain <= 0 {
		c.StrategyMRGain = 0.30
	}
	if c.StrategyBreakoutGain <= 0 {
		c.StrategyBreakoutGain = 1.0
	}
	if c.FallbackScale <= 0 {
		c.FallbackScale = 1.0
	}

	c.Strategy.applyDefaults(c)
	c.Risk.applyDefaults(c)
	c.Optimization.applyDefaults()
}

func (s *StrategyConfig) applyDefaults(cfg *BacktestConfig) {
	if s.TrendGain <= 0 {
		if cfg.StrategyTrendGain > 0 {
			s.TrendGain = cfg.StrategyTrendGain
		} else {
			s.TrendGain = 2.0
		}
	}
	if s.MRGain <= 0 {
		if cfg.StrategyMRGain > 0 {
			s.MRGain = cfg.StrategyMRGain
		} else {
			s.MRGain = 0.7
		}
	}
	if s.BreakoutGain <= 0 {
		if cfg.StrategyBreakoutGain > 0 {
			s.BreakoutGain = cfg.StrategyBreakoutGain
		} else {
			s.BreakoutGain = 1.0
		}
	}
	s.Regime.applyDefaults()
	s.MTF.applyDefaults()
	s.Fallback.applyDefaults(cfg.FallbackScale)
}

func (r *RiskConfig) applyDefaults(cfg *BacktestConfig) {
	if r.RiskTarget <= 0 {
		if cfg.StrategyRiskTarget > 0 {
			r.RiskTarget = cfg.StrategyRiskTarget
		} else {
			r.RiskTarget = 0.6
		}
	}
	if r.ATRPeriod <= 0 {
		r.ATRPeriod = 14
	}
	if r.ATRStopK <= 0 {
		r.ATRStopK = 2.5
	}
	if r.ATRTrailK <= 0 {
		r.ATRTrailK = 3.0
	}
	if r.MaxAbsPosition <= 0 {
		if cfg.StrategyMaxAbsPosition > 0 {
			r.MaxAbsPosition = cfg.StrategyMaxAbsPosition
		} else {
			r.MaxAbsPosition = 1.5
		}
	}
	if r.MaxLeverage <= 0 {
		if cfg.StrategyMaxLeverage > 0 {
			r.MaxLeverage = cfg.StrategyMaxLeverage
		} else {
			r.MaxLeverage = 2.0
		}
	}
	r.DDCircuit.applyDefaults()
}

func (r *RegimeConfig) applyDefaults() {
	if r.Enable == nil {
		r.Enable = boolPtr(true)
	}
	if r.TrendAdxPeriod <= 0 {
		r.TrendAdxPeriod = 14
	}
	if r.TrendAdxTh <= 0 {
		r.TrendAdxTh = 20
	}
	if r.RangeBwPeriod <= 0 {
		r.RangeBwPeriod = 20
	}
	if r.RangeBwTh <= 0 {
		r.RangeBwTh = 0.05
	}
}

func (m *MTFConfig) applyDefaults() {
	if m.ConfirmEnable == nil {
		m.ConfirmEnable = boolPtr(true)
	}
	if strings.TrimSpace(m.HigherTF) == "" {
		m.HigherTF = "1h"
	}
	if m.TrendAlign == nil {
		m.TrendAlign = boolPtr(true)
	}
}

func (f *FallbackConfig) applyDefaults(legacyScale float64) {
	if f.Enable == nil {
		f.Enable = boolPtr(true)
	}
	if f.Scale <= 0 {
		if legacyScale > 0 {
			f.Scale = legacyScale
		} else {
			f.Scale = 0.25
		}
	}
	if f.MAPeriod <= 0 {
		f.MAPeriod = 100
	}
}

func (d *DDCircuitConfig) applyDefaults() {
	if d.Enable == nil {
		d.Enable = boolPtr(true)
	}
	if d.Threshold <= 0 {
		d.Threshold = 0.15
	}
	if d.CooldownBars <= 0 {
		d.CooldownBars = 96
	}
}

func (o *OptimizationConfig) applyDefaults() {
	if o.Enable == nil {
		o.Enable = boolPtr(true)
	}
	if o.MaxSamples <= 0 {
		o.MaxSamples = defaultGridSampleSize
	}
	if o.Seed == 0 {
		o.Seed = 42
	}
}

// ==================== Portfolio Adapter ====================

type PortfolioAdapter struct{ portfolio *portfolio.Engine }

func (pa *PortfolioAdapter) SetStrategyTargets(name string, targets map[string]float64) {
	pa.portfolio.SetStrategyTargets(name, targets)
}
func (pa *PortfolioAdapter) Propose(mark map[string]float64) (map[string]float64, map[string]any) {
	w, meta := pa.portfolio.Propose(mark)
	return w, map[string]any{
		"strategy_weights": meta.StrategyWeights,
		"portfolio_vol":    meta.PortfolioVolAnn,
		"gross":            meta.Gross,
		"turnover":         meta.Turnover,
		"scaler":           meta.Scaler,
	}
}
func (pa *PortfolioAdapter) OnCandle(c backtest.Candle) {
	pa.portfolio.OnCandle(portfolio.Candle{InstID: c.InstID, T: c.T, O: c.O, H: c.H, L: c.L, C: c.C})
}

// ==================== Runner ====================

type BacktestRunner struct {
	config       BacktestConfig
	strategy     *strategy.QuantMasterElite
	portfolio    *portfolio.Engine
	backtest     *backtest.Engine
	barMinutes   int
	stratAdapter *StrategyAdapter
	riskAdapter  *RiskAdapter
}

type RunAnalytics struct {
	Attribution map[string]AttributionStats `json:"strategy_attribution"`
	Strategy    StrategySummary             `json:"strategy_summary"`
	Risk        RiskSummary                 `json:"risk_summary"`
	VolTarget   VolTargetStats              `json:"vol_target"`
}

type AttributionStats struct {
	Trades      int     `json:"trades"`
	WinRate     float64 `json:"win_rate"`
	AvgWin      float64 `json:"avg_win"`
	AvgLoss     float64 `json:"avg_loss"`
	TotalReturn float64 `json:"total_return"`
}

type VolTargetStats struct {
	Target float64 `json:"target"`
	Actual float64 `json:"actual"`
}

func NewBacktestRunner(configPath string) (*BacktestRunner, error) {
	cfg, err := loadConfig(configPath)
	if err != nil {
		return nil, err
	}

	tfMin := normalizeTimeframe(cfg.Timeframe)
	log.Printf("Config => timeframe=%s(%dmin) source=%s data_path=%s portfolio=%v autofetch=%v",
		strings.TrimSpace(cfg.Timeframe), tfMin, strings.ToLower(cfg.DataSource), cfg.DataPath, cfg.UsePortfolio, cfg.AutoFetchIfMissing)

	strategyEngine := buildStrategyEngine(cfg, tfMin)
	portfolioEngine := buildPortfolioEngine(cfg, tfMin)
	bt := buildBacktestEngine(cfg, tfMin)

	return &BacktestRunner{
		config:     cfg,
		strategy:   strategyEngine,
		portfolio:  portfolioEngine,
		backtest:   bt,
		barMinutes: tfMin,
	}, nil
}

func (br *BacktestRunner) Run() error {
	log.Println("Starting backtest ...")
	start := time.Now()

	series, err := br.loadHistoricalData()
	if err != nil {
		return fmt.Errorf("failed to load historical data: %v", err)
	}

	br.wireStrategyLayer()
	br.wireRiskLayer()
	br.wirePortfolioLayer()

	result := br.backtest.Run(series)
	analytics := br.buildAnalytics(result)
	printResults(result, analytics)
	saveAll(result, analytics)
	leaderboard, report := br.runGridSearch(series, result)
	if len(leaderboard) > 0 {
		if err := saveLeaderboard("./backtest_results/leaderboard.csv", leaderboard); err != nil {
			log.Printf("failed to save leaderboard: %v", err)
		}
		if strings.TrimSpace(report) != "" {
			if err := saveReport("./backtest_results/report.md", report); err != nil {
				log.Printf("failed to save report: %v", err)
			}
		}
	}

	log.Printf("Finished in %v", time.Since(start))
	return nil
}

// wireStrategyLayer connects the strategy implementation to the backtest engine.
func (br *BacktestRunner) wireStrategyLayer() {
	sa := NewStrategyAdapter(br.config.Strategy, br.config.Risk, br.barMinutes)
	br.stratAdapter = sa
	br.backtest.SetStrategy(sa)
}

func (br *BacktestRunner) wireRiskLayer() {
	if !br.config.UseRisk {
		return
	}
	ra := NewRiskAdapter(br.config.Risk, br.barMinutes)
	br.riskAdapter = ra
	br.backtest.SetRisk(ra)
}

// wirePortfolioLayer connects the portfolio engine if enabled.
func (br *BacktestRunner) wirePortfolioLayer() {
	if !br.config.UsePortfolio || br.portfolio == nil {
		return
	}
	pa := &PortfolioAdapter{portfolio: br.portfolio}
	br.backtest.SetPortfolio(pa)
}

func (br *BacktestRunner) buildAnalytics(res backtest.Result) RunAnalytics {
	analytics := RunAnalytics{
		Attribution: summarizeAttribution(res.Trades),
		VolTarget:   calcVolStats(res.EquityCurve, br.barMinutes, br.config.Risk.RiskTarget),
	}
	if br.stratAdapter != nil {
		analytics.Strategy = br.stratAdapter.Summary()
	}
	if br.riskAdapter != nil {
		analytics.Risk = br.riskAdapter.Summary()
	}
	return analytics
}

const defaultGridSampleSize = 60

type paramSet struct {
	TrendGain    float64
	MRGain       float64
	BreakoutGain float64
	RiskTarget   float64
	ATRStop      float64
	ATRTrail     float64
	RegimeMul    float64
}

type gridEntry struct {
	TrendGain    float64
	MRGain       float64
	BreakoutGain float64
	RiskTarget   float64
	ATRStop      float64
	ATRTrail     float64
	RegimeMul    float64
	CAGR         float64
	MaxDD        float64
	Sharpe       float64
	Calmar       float64
	FinalEquity  float64
}

func (br *BacktestRunner) runGridSearch(series backtest.Series, baseline backtest.Result) ([]gridEntry, string) {
	if !boolValue(br.config.Optimization.Enable, true) {
		return nil, ""
	}
	sets := generateParamSets()
	if len(sets) == 0 {
		return nil, ""
	}
	rng := rand.New(rand.NewSource(br.config.Optimization.Seed))
	rng.Shuffle(len(sets), func(i, j int) {
		sets[i], sets[j] = sets[j], sets[i]
	})
	sample := len(sets)
	if br.config.Optimization.MaxSamples > 0 && br.config.Optimization.MaxSamples < sample {
		sample = br.config.Optimization.MaxSamples
	}
	entries := make([]gridEntry, 0, sample)
	bestCalmar := math.Inf(-1)
	var bestEntry gridEntry
	var bestResult backtest.Result
	for i := 0; i < sample; i++ {
		params := sets[i]
		cfg := br.config
		cfg.Strategy.TrendGain = params.TrendGain
		cfg.Strategy.MRGain = params.MRGain
		cfg.Strategy.BreakoutGain = params.BreakoutGain
		cfg.Risk.RiskTarget = params.RiskTarget
		cfg.Risk.ATRStopK = params.ATRStop
		cfg.Risk.ATRTrailK = params.ATRTrail
		cfg.Strategy.Regime.TrendAdxTh = br.config.Strategy.Regime.TrendAdxTh * params.RegimeMul
		cfg.Strategy.Regime.RangeBwTh = br.config.Strategy.Regime.RangeBwTh * params.RegimeMul
		cfg.normalize()
		engine := buildBacktestEngine(cfg, br.barMinutes)
		sa := NewStrategyAdapter(cfg.Strategy, cfg.Risk, br.barMinutes)
		engine.SetStrategy(sa)
		if cfg.UseRisk {
			ra := NewRiskAdapter(cfg.Risk, br.barMinutes)
			engine.SetRisk(ra)
		}
		res := engine.Run(series)
		entry := gridEntry{
			TrendGain:    params.TrendGain,
			MRGain:       params.MRGain,
			BreakoutGain: params.BreakoutGain,
			RiskTarget:   params.RiskTarget,
			ATRStop:      params.ATRStop,
			ATRTrail:     params.ATRTrail,
			RegimeMul:    params.RegimeMul,
			CAGR:         res.CAGR,
			MaxDD:        res.MaxDD,
			Sharpe:       res.Sharpe,
			Calmar:       calcCalmar(res.CAGR, res.MaxDD),
			FinalEquity:  res.FinalEquity,
		}
		entries = append(entries, entry)
		if entry.Calmar > bestCalmar {
			bestCalmar = entry.Calmar
			bestEntry = entry
			bestResult = res
		}
	}
	sort.Slice(entries, func(i, j int) bool {
		if entries[i].Calmar == entries[j].Calmar {
			return entries[i].Sharpe > entries[j].Sharpe
		}
		return entries[i].Calmar > entries[j].Calmar
	})
	report := br.composeReport(baseline, bestResult, bestEntry, sample, len(sets))
	return entries, report
}

func generateParamSets() []paramSet {
	trend := []float64{1.2, 1.5, 1.8, 2.0}
	mr := []float64{0.5, 0.7, 1.0}
	breakout := []float64{0.8, 1.0, 1.2}
	riskTargets := []float64{0.45, 0.55, 0.65}
	atrStops := []float64{2.0, 2.5, 3.0}
	atrTrails := []float64{2.5, 3.0, 3.5}
	regimeMul := []float64{0.9, 1.0, 1.1}
	var sets []paramSet
	for _, tg := range trend {
		for _, mg := range mr {
			for _, bg := range breakout {
				for _, rt := range riskTargets {
					for _, stop := range atrStops {
						for _, trail := range atrTrails {
							for _, mul := range regimeMul {
								sets = append(sets, paramSet{
									TrendGain:    tg,
									MRGain:       mg,
									BreakoutGain: bg,
									RiskTarget:   rt,
									ATRStop:      stop,
									ATRTrail:     trail,
									RegimeMul:    mul,
								})
							}
						}
					}
				}
			}
		}
	}
	return sets
}

func calcCalmar(cagr, maxDD float64) float64 {
	denom := math.Max(maxDD, 1e-6)
	return cagr / denom
}

func (br *BacktestRunner) composeReport(baseline backtest.Result, bestResult backtest.Result, best gridEntry, sampled, total int) string {
	if sampled == 0 {
		return ""
	}
	ddImprovement := 0.0
	if baseline.MaxDD > 0 {
		ddImprovement = (baseline.MaxDD - bestResult.MaxDD) / baseline.MaxDD
	}
	return fmt.Sprintf(`# Backtest Optimization Report

Tested %d of %d parameter combinations (randomized grid).

## Baseline
- Final equity: %.2f
- CAGR: %.2f%%
- Sharpe: %.2f
- Max DD: %.2f%%
- Calmar: %.2f

## Best Candidate
- Params: trend=%.2f, mr=%.2f, breakout=%.2f, risk_target=%.2f, atr_stop=%.2f, atr_trail=%.2f, regime_mul=%.2f
- Final equity: %.2f
- CAGR: %.2f%%
- Sharpe: %.2f
- Max DD: %.2f%%
- Calmar: %.2f
- Max DD improvement vs baseline: %.2f%%
`,
		sampled,
		total,
		baseline.FinalEquity,
		baseline.CAGR*100,
		baseline.Sharpe,
		baseline.MaxDD*100,
		calcCalmar(baseline.CAGR, baseline.MaxDD),
		best.TrendGain,
		best.MRGain,
		best.BreakoutGain,
		best.RiskTarget,
		best.ATRStop,
		best.ATRTrail,
		best.RegimeMul,
		bestResult.FinalEquity,
		bestResult.CAGR*100,
		bestResult.Sharpe,
		bestResult.MaxDD*100,
		best.Calmar,
		ddImprovement*100,
	)
}

// normalizeTimeframe parses timeframe string to minutes, default 15m.
func normalizeTimeframe(tf string) int {
	tfMin := int(timeframeStepMS(tf) / (60 * 1000))
	if tfMin <= 0 {
		return 15
	}
	return tfMin
}

// buildStrategyEngine creates the QuantMasterElite strategy with parameters.
func buildStrategyEngine(cfg BacktestConfig, tfMin int) *strategy.QuantMasterElite {
	return strategy.NewQuantMasterElite(strategy.EliteParams{
		TimeframeMinutes: tfMin,
		TrendWindows:     []int{6, 12, 24, 48},
		TrendGain:        nonZeroOrFloat(cfg.Strategy.TrendGain, 3.0),
		MRWindows:        []int{10, 20},
		MRGain:           nonZeroOrFloat(cfg.Strategy.MRGain, 0.30),
		BreakoutLookback: 20,
		BRGain:           nonZeroOrFloat(cfg.Strategy.BreakoutGain, 1.0),

		VolWindow:          120,
		TargetVolAnnual:    nonZeroOrFloat(cfg.Risk.RiskTarget, 1.0),
		VolTargetSmoothing: 0.08,
		MinSigmaAnnual:     0.06,

		MaxAbsPosition:   nonZeroOrFloat(cfg.Risk.MaxAbsPosition, 2.0),
		MaxLeverage:      nonZeroOrFloat(cfg.Risk.MaxLeverage, 3.0),
		EntryThreshold:   0.004,
		ExitThreshold:    0.002,
		MinPositionDelta: 0.004,
		CooldownBars:     0,

		TakerFeeBps:  0.0,
		MakerFeeBps:  0.0,
		SlippageBps:  0.0,
		ImpactCoef:   0.0,
		EdgeCostCoef: 0.4,

		LotSize:  0.001,
		TickSize: 0.1,

		UseAdaptiveStop:  false,
		UseRegimeFilter:  false,
		UseMetaLearning:  false,
		UseLiquidityGate: false,

		PerformanceWindow: 1000,
		Seed:              time.Now().UnixNano(),
	})
}

// buildPortfolioEngine creates the portfolio engine if enabled.
func buildPortfolioEngine(cfg BacktestConfig, tfMin int) *portfolio.Engine {
	if !cfg.UsePortfolio {
		return nil
	}

	return portfolio.NewEngine(portfolio.Config{
		TargetVolAnnual:       0.20,
		MaxLeverage:           2.0,
		MaxGross:              2.0,
		CashBufferPct:         0.02,
		UseRiskParity:         true,
		EWHalfLifeVol:         96,
		EWHalfLifeCorr:        256,
		RebalanceIntervalBars: 24,
		DriftThreshold:        0.05,
		TurnoverCap:           0.9,
		BarMinutes:            tfMin,
		StrategyWeights:       map[string]float64{"quantmaster": 1.0},
		StrategyLearn:         false,
	})
}

// buildBacktestEngine configures the backtest engine.
func buildBacktestEngine(cfg BacktestConfig, tfMin int) *backtest.Engine {
	return backtest.New(backtest.Config{
		InitialEquity:    cfg.InitialCash,
		BarMinutes:       tfMin,
		TradeOnNextBar:   true,
		UseMaker:         false,
		TakerFeeBps:      0.0,
		MakerFeeBps:      0.0,
		SlippageBps:      0.0,
		MinRebalanceStep: 0.0,
		MaxAbsPosition:   nonZeroOrFloat(cfg.Risk.MaxAbsPosition, 1.0),
		AfterFill: func(inst, side string, delta float64, ref float64) (float64, float64) {
			log.Printf("FILL %-4s %-16s turnover=%.4f @ref=%.2f", strings.ToUpper(side), inst, delta, ref)
			return 0, 0
		},
	})
}

// ==================== Data Loading ====================

func (br *BacktestRunner) loadHistoricalData() (backtest.Series, error) {
	series := make(backtest.Series)
	step := timeframeStepMS(br.config.Timeframe)
	limit := nonZeroOr(br.config.BarsLimit, 2000)

	for _, inst := range br.config.Instruments {
		var candles []backtest.Candle
		var err error

		switch strings.ToLower(strings.TrimSpace(br.config.DataSource)) {
		case "csv":
			path := filepath.Join(br.config.DataPath, fmt.Sprintf("%s.csv", inst))
			candles, err = loadFromCSV(path, inst)
			if err == nil && limit > 0 && len(candles) < limit && br.config.AutoFetchIfMissing {
				log.Printf("鈩癸笍 %s local bars=%d < limit=%d, refreshing from API ...", inst, len(candles), limit)
				if fresh, fetchErr := br.fetchAndCache(inst, path, limit); fetchErr == nil && len(fresh) > 0 {
					candles = fresh
				} else if fetchErr != nil {
					log.Printf("鈿狅笍 fetch %s failed: %v", inst, fetchErr)
				}
			}
			if err != nil && errors.Is(err, os.ErrNotExist) && br.config.AutoFetchIfMissing {
				log.Printf("鈿狅笍 %s CSV missing locally, fetching latest ...", inst)
				candles, err = br.fetchAndCache(inst, path, limit)
			} else if err == nil && len(candles) == 0 && br.config.AutoFetchIfMissing {
				log.Printf("鈿狅笍 %s local data empty, fetching ...", inst)
				candles, err = br.fetchAndCache(inst, path, limit)
			}
		case "api":
			candles, err = fetchFromAPI(inst, br.config.Timeframe, limit)
			if err != nil && br.config.AutoFetchIfMissing {
				log.Printf("閳跨媴绗?API fetch failed; retrying %s ...", inst)
				time.Sleep(700 * time.Millisecond)
				candles, err = fetchFromAPI(inst, br.config.Timeframe, limit)
			}
		default:
			err = fmt.Errorf("unknown DataSource=%s", br.config.DataSource)
		}

		if err != nil {
			log.Printf("閳跨媴绗?load %s failed: %v", inst, err)
			continue
		}
		if len(candles) == 0 {
			log.Printf("閳跨媴绗?%s returned 0 candles", inst)
			continue
		}
		candles = ensureAscUnique(candles, step)
		series[inst] = candles
		log.Printf("loaded %s: %d bars", inst, len(candles))
	}
	if len(series) == 0 {
		return nil, fmt.Errorf("no instruments loaded")
	}
	return series, nil
}

func (br *BacktestRunner) fetchAndCache(instID, csvPath string, limit int) ([]backtest.Candle, error) {
	candles, err := fetchFromAPI(instID, br.config.Timeframe, limit)
	if err != nil {
		return nil, err
	}
	if csvPath != "" {
		if err := persistCandlesToCSV(csvPath, candles); err != nil {
			log.Printf("閳跨媴绗?write %s failed: %v", csvPath, err)
		}
	}
	return candles, nil
}

func persistCandlesToCSV(path string, candles []backtest.Candle) error {
	if len(candles) == 0 {
		return nil
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	w := csv.NewWriter(f)
	defer w.Flush()
	if err := w.Write([]string{"timestamp", "open", "high", "low", "close", "volume"}); err != nil {
		return err
	}
	for _, c := range candles {
		rec := []string{
			fmt.Sprintf("%d", c.T),
			fmt.Sprintf("%.8f", c.O),
			fmt.Sprintf("%.8f", c.H),
			fmt.Sprintf("%.8f", c.L),
			fmt.Sprintf("%.8f", c.C),
			fmt.Sprintf("%.8f", c.V),
		}
		if err := w.Write(rec); err != nil {
			return err
		}
	}
	return nil
}

func fetchFromAPI(instID, timeframe string, limit int) ([]backtest.Candle, error) {
	client := stream.NewHybridClient()
	defer client.Close()

	apiCandles, err := client.GetCandles(instID, timeframe, limit)
	if err != nil {
		return nil, err
	}
	out := make([]backtest.Candle, 0, len(apiCandles))
	for _, c := range apiCandles {
		out = append(out, backtest.Candle{
			InstID: instID, T: c.Timestamp, O: c.Open, H: c.High, L: c.Low, C: c.Close, V: c.Volume,
		})
	}
	return out, nil
}

func loadFromCSV(path, instID string) ([]backtest.Candle, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	r := csv.NewReader(f)
	records, err := r.ReadAll()
	if err != nil {
		return nil, err
	}
	var out []backtest.Candle
	for i, rec := range records {
		if i == 0 || len(rec) < 6 {
			continue
		}
		t, _ := strconv.ParseInt(rec[0], 10, 64)
		o, _ := strconv.ParseFloat(rec[1], 64)
		h, _ := strconv.ParseFloat(rec[2], 64)
		l, _ := strconv.ParseFloat(rec[3], 64)
		c, _ := strconv.ParseFloat(rec[4], 64)
		v, _ := strconv.ParseFloat(rec[5], 64)
		out = append(out, backtest.Candle{InstID: instID, T: t, O: o, H: h, L: l, C: c, V: v})
	}
	return out, nil
}

// ==================== Reporting ====================

func printResults(r backtest.Result, analytics RunAnalytics) {
	log.Printf("\n============================================================")
	log.Printf("Backtest Summary")
	log.Printf("============================================================")
	log.Printf("Final Equity        : %.2f", r.FinalEquity)
	log.Printf("Total Return        : %.2f%%", r.TotalRet*100)
	log.Printf("CAGR                : %.2f%%", r.CAGR*100)
	log.Printf("Sharpe              : %.2f", r.Sharpe)
	log.Printf("Max Drawdown        : %.2f%%", r.MaxDD*100)
	log.Printf("Win Rate            : %.2f%%", r.WinRate*100)
	log.Printf("Number of Trades    : %d", r.NumTrades)
	log.Printf("Actual Volatility   : %.2f%% (target %.2f%%)", analytics.VolTarget.Actual*100, analytics.VolTarget.Target*100)
	hitRate := 0.0
	if analytics.Strategy.MTFChecks > 0 {
		hitRate = float64(analytics.Strategy.MTFAligned) / float64(analytics.Strategy.MTFChecks)
	}
	log.Printf("MTF Hit Rate        : %.2f%% (%d/%d)", hitRate*100, analytics.Strategy.MTFAligned, analytics.Strategy.MTFChecks)
	log.Printf("Fallback Usage      : %d", analytics.Strategy.FallbackUsage)
	log.Printf("Stop Counts         : %+v", analytics.Risk.StopCounts)
	log.Printf("DD Circuit Windows  : %d", len(analytics.Risk.DDWindows))
	log.Printf("Strategy Attribution:")
	for _, key := range orderedAttributionKeys(analytics.Attribution) {
		stats := analytics.Attribution[key]
		log.Printf("  - %-10s trades=%3d win_rate=%.2f%% total=%.2f%%", key, stats.Trades, stats.WinRate*100, stats.TotalReturn*100)
	}
	log.Printf("------------------------------------------------------------")
	log.Printf("Trades Detail")
	log.Printf("------------------------------------------------------------")
	logTrades(r.Trades)
	log.Printf("============================================================\n")
}

func saveAll(r backtest.Result, analytics RunAnalytics) {
	_ = os.MkdirAll("./backtest_results", 0o755)
	_ = saveJSON("./backtest_results/stats.json", map[string]any{
		"final_equity":         r.FinalEquity,
		"total_return":         r.TotalRet,
		"cagr":                 r.CAGR,
		"sharpe":               r.Sharpe,
		"max_dd":               r.MaxDD,
		"win_rate":             r.WinRate,
		"num_trades":           r.NumTrades,
		"actual_vol":           analytics.VolTarget.Actual,
		"vol_target":           analytics.VolTarget.Target,
		"strategy_attribution": analytics.Attribution,
		"strategy_summary":     analytics.Strategy,
		"risk_summary":         analytics.Risk,
	})
	_ = saveEquityCurve("./backtest_results/equity_curve.csv", r.EquityCurve)
	_ = saveJSON("./backtest_results/trades.json", r.Trades)
	_ = saveTradeDetails("./backtest_results/trades_detailed.csv", r.Trades)
}

func saveEquityCurve(path string, curve []backtest.BarRecord) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	w := csv.NewWriter(f)
	defer w.Flush()
	_ = w.Write([]string{"timestamp", "equity", "return", "drawdown"})
	for _, b := range curve {
		_ = w.Write([]string{
			fmt.Sprintf("%d", b.Ts),
			fmt.Sprintf("%.6f", b.Equity),
			fmt.Sprintf("%.6f", b.Ret),
			fmt.Sprintf("%.6f", b.Drawdown),
		})
	}
	return nil
}

func saveTradeDetails(path string, trades []backtest.Trade) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	w := csv.NewWriter(f)
	defer w.Flush()
	_ = w.Write([]string{"idx", "instrument", "dir", "entry_ts", "entry_utc", "entry_price", "exit_ts", "exit_utc", "exit_price", "size", "return_pct", "holding_minutes", "sub_strategy", "regime", "stop_type", "atr_on_entry"})
	for i, tr := range trades {
		ret := computeTradeReturn(tr) * 100
		holdMinutes := ""
		if tr.EntryTime > 0 && tr.ExitTime > 0 {
			holdMinutes = fmt.Sprintf("%.2f", float64(tr.ExitTime-tr.EntryTime)/60000.0)
		}
		_ = w.Write([]string{
			strconv.Itoa(i + 1),
			tr.InstID,
			tr.Dir,
			fmt.Sprintf("%d", tr.EntryTime),
			formatTimestamp(tr.EntryTime),
			fmt.Sprintf("%.6f", tr.EntryPrice),
			fmt.Sprintf("%d", tr.ExitTime),
			formatTimestamp(tr.ExitTime),
			fmt.Sprintf("%.6f", tr.ExitPrice),
			fmt.Sprintf("%.6f", tr.Size),
			fmt.Sprintf("%.4f", ret),
			holdMinutes,
			tr.SubStrategy,
			tr.Regime,
			tr.StopType,
			fmt.Sprintf("%.6f", tr.ATROnEntry),
		})
	}
	return nil
}

func orderedAttributionKeys(m map[string]AttributionStats) []string {
	if len(m) == 0 {
		return nil
	}
	base := []string{"trend", "mr", "breakout", "fallback"}
	seen := make(map[string]bool, len(base))
	var keys []string
	for _, k := range base {
		if _, ok := m[k]; ok {
			keys = append(keys, k)
			seen[k] = true
		}
	}
	var rest []string
	for k := range m {
		if !seen[k] {
			rest = append(rest, k)
		}
	}
	if len(rest) > 0 {
		sort.Strings(rest)
		keys = append(keys, rest...)
	}
	return keys
}

func saveLeaderboard(path string, entries []gridEntry) error {
	if len(entries) == 0 {
		return nil
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	w := csv.NewWriter(f)
	defer w.Flush()
	head := []string{"rank", "trend_gain", "mr_gain", "breakout_gain", "risk_target", "atr_stop_k", "atr_trail_k", "regime_multiplier", "cagr", "max_dd", "sharpe", "calmar", "final_equity"}
	if err := w.Write(head); err != nil {
		return err
	}
	for i, e := range entries {
		rec := []string{
			strconv.Itoa(i + 1),
			fmt.Sprintf("%.2f", e.TrendGain),
			fmt.Sprintf("%.2f", e.MRGain),
			fmt.Sprintf("%.2f", e.BreakoutGain),
			fmt.Sprintf("%.2f", e.RiskTarget),
			fmt.Sprintf("%.2f", e.ATRStop),
			fmt.Sprintf("%.2f", e.ATRTrail),
			fmt.Sprintf("%.2f", e.RegimeMul),
			fmt.Sprintf("%.4f", e.CAGR),
			fmt.Sprintf("%.4f", e.MaxDD),
			fmt.Sprintf("%.4f", e.Sharpe),
			fmt.Sprintf("%.4f", e.Calmar),
			fmt.Sprintf("%.4f", e.FinalEquity),
		}
		if err := w.Write(rec); err != nil {
			return err
		}
	}
	return w.Error()
}

func saveReport(path, body string) error {
	if strings.TrimSpace(body) == "" {
		return nil
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	return os.WriteFile(path, []byte(body), 0o644)
}

func logTrades(trades []backtest.Trade) {
	if len(trades) == 0 {
		log.Printf("No trades executed.")
		return
	}
	log.Printf("%-4s %-6s %-12s %-17s %-17s %-8s %-9s", "No.", "Dir", "Instrument", "Entry (UTC)", "Exit (UTC)", "Size", "Ret%")
	for i, tr := range trades {
		ret := computeTradeReturn(tr) * 100
		log.Printf("%-4d %-6s %-12s %-17s %-17s %-8.3f %-9.2f",
			i+1,
			strings.ToUpper(tr.Dir),
			tr.InstID,
			formatTimestamp(tr.EntryTime),
			formatTimestamp(tr.ExitTime),
			tr.Size,
			ret,
		)
	}
}

type attrAccumulator struct {
	trades    int
	wins      int
	winSum    float64
	lossSum   float64
	lossCount int
	total     float64
}

func summarizeAttribution(trades []backtest.Trade) map[string]AttributionStats {
	acc := map[string]*attrAccumulator{}
	for _, key := range []string{"trend", "mr", "breakout", "fallback"} {
		acc[key] = &attrAccumulator{}
	}
	for _, tr := range trades {
		key := strings.TrimSpace(tr.SubStrategy)
		if key == "" {
			key = "unknown"
		}
		bucket, ok := acc[key]
		if !ok {
			bucket = &attrAccumulator{}
			acc[key] = bucket
		}
		bucket.trades++
		bucket.total += tr.Return
		if tr.Return > 0 {
			bucket.wins++
			bucket.winSum += tr.Return
		} else if tr.Return < 0 {
			bucket.lossCount++
			bucket.lossSum += tr.Return
		}
	}
	stats := make(map[string]AttributionStats, len(acc))
	for key, bucket := range acc {
		out := AttributionStats{Trades: bucket.trades, TotalReturn: bucket.total}
		if bucket.trades > 0 {
			out.WinRate = float64(bucket.wins) / float64(bucket.trades)
		}
		if bucket.wins > 0 {
			out.AvgWin = bucket.winSum / float64(bucket.wins)
		}
		if bucket.lossCount > 0 {
			out.AvgLoss = bucket.lossSum / float64(bucket.lossCount)
		}
		stats[key] = out
	}
	return stats
}

func calcVolStats(curve []backtest.BarRecord, barMinutes int, target float64) VolTargetStats {
	vs := VolTargetStats{Target: target}
	if len(curve) == 0 {
		return vs
	}
	vals := make([]float64, len(curve))
	for i, b := range curve {
		vals[i] = b.Ret
	}
	vs.Actual = annualizeVol(stdDev(vals), barMinutes)
	return vs
}

func stdDev(vals []float64) float64 {
	if len(vals) == 0 {
		return 0
	}
	mean := 0.0
	for _, v := range vals {
		mean += v
	}
	mean /= float64(len(vals))
	varSum := 0.0
	for _, v := range vals {
		d := v - mean
		varSum += d * d
	}
	return math.Sqrt(varSum / math.Max(1, float64(len(vals)-1)))
}

func computeTradeReturn(tr backtest.Trade) float64 {
	if tr.EntryPrice > 0 && tr.ExitPrice > 0 {
		gross := (tr.ExitPrice / tr.EntryPrice) - 1
		if strings.ToLower(tr.Dir) == "short" {
			gross = -gross
		}
		return gross
	}
	return tr.Return
}

func formatTimestamp(ts int64) string {
	if ts <= 0 {
		return "-"
	}
	return time.UnixMilli(ts).UTC().Format("2006-01-02 15:04")
}

// ==================== Config I/O ====================

func loadConfig(path string) (BacktestConfig, error) {
	var cfg BacktestConfig
	if _, err := os.Stat(path); os.IsNotExist(err) {
		cfg = BacktestConfig{
			StartDate:          "2024-01-01",
			EndDate:            "2024-12-01",
			InitialCash:        10000,
			Instruments:        []string{"BTC-USDT-SWAP"},
			Timeframe:          "15m",
			DataSource:         "api",
			DataPath:           "./data/candles",
			AutoFetchIfMissing: true,
			UseRisk:            true,
			UsePortfolio:       false,
			BarsLimit:          2000,
			Strategy: StrategyConfig{
				TrendGain:    1.8,
				MRGain:       0.8,
				BreakoutGain: 1.0,
				Regime: RegimeConfig{
					Enable:         boolPtr(true),
					TrendAdxPeriod: 14,
					TrendAdxTh:     22,
					RangeBwPeriod:  20,
					RangeBwTh:      0.06,
				},
				MTF: MTFConfig{
					ConfirmEnable: boolPtr(true),
					HigherTF:      "1h",
					TrendAlign:    boolPtr(true),
				},
				Fallback: FallbackConfig{
					Enable:   boolPtr(true),
					Scale:    0.2,
					MAPeriod: 120,
				},
			},
			Risk: RiskConfig{
				RiskTarget:     0.55,
				ATRPeriod:      14,
				ATRStopK:       2.5,
				ATRTrailK:      3.0,
				MaxLeverage:    2.0,
				MaxAbsPosition: 1.5,
				DDCircuit: DDCircuitConfig{
					Enable:       boolPtr(true),
					Threshold:    0.15,
					CooldownBars: 96,
				},
			},
			Optimization: OptimizationConfig{
				Enable:     boolPtr(true),
				MaxSamples: 45,
				Seed:       42,
			},
			DebugFallbackMA:    true,
			DebugFallbackForce: false,
		}
		cfg.normalize()
		_ = saveJSON(path, cfg)
		log.Printf("Wrote default config to %s", path)
		return cfg, nil
	}
	b, err := os.ReadFile(path)
	if err != nil {
		return cfg, err
	}
	if err = json.Unmarshal(b, &cfg); err != nil {
		return cfg, err
	}
	cfg.normalize()
	return cfg, nil
}

func saveJSON(path string, v any) error {
	_ = os.MkdirAll(filepath.Dir(path), 0o755)
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	enc := json.NewEncoder(f)
	enc.SetIndent("", "  ")
	return enc.Encode(v)
}

// ==================== Utils ====================

func timeframeStepMS(tf string) int64 {
	switch strings.ToLower(strings.TrimSpace(tf)) {
	case "1m":
		return 60 * 1000
	case "5m":
		return 5 * 60 * 1000
	case "15m":
		return 15 * 60 * 1000
	case "30m":
		return 30 * 60 * 1000
	case "1h":
		return 60 * 60 * 1000
	case "4h":
		return 4 * 60 * 60 * 1000
	case "1d":
		return 24 * 60 * 60 * 1000
	default:
		return 15 * 60 * 1000
	}
}

func ensureAscUnique(xs []backtest.Candle, step int64) []backtest.Candle {
	if len(xs) == 0 {
		return xs
	}
	out := make([]backtest.Candle, 0, len(xs))
	var lastTs int64 = -1
	for _, k := range xs {
		if k.T <= lastTs {
			if k.T == lastTs {
				continue
			}
			if k.T < lastTs {
				continue
			}
		}
		out = append(out, k)
		lastTs = k.T
	}
	return out
}

func nonZeroOr(x, d int) int {
	if x > 0 {
		return x
	}
	return d
}

func nonZeroOrFloat(x, d float64) float64 {
	if x > 0 {
		return x
	}
	return d
}

func clamp(x, lo, hi float64) float64 {
	if x < lo {
		return lo
	}
	if x > hi {
		return hi
	}
	return x
}

func boolPtr(v bool) *bool { return &v }

func smaLast(a []float64, n int) float64 {
	if n <= 0 || len(a) < n {
		return a[len(a)-1]
	}
	sum := 0.0
	for i := len(a) - n; i < len(a); i++ {
		sum += a[i]
	}
	return sum / float64(n)
}

func stdLast(a []float64, n int) float64 {
	if n <= 1 || len(a) < n {
		return 0
	}
	sum := 0.0
	for i := len(a) - n; i < len(a); i++ {
		sum += a[i]
	}
	mean := sum / float64(n)
	acc := 0.0
	for i := len(a) - n; i < len(a); i++ {
		d := a[i] - mean
		acc += d * d
	}
	return math.Sqrt(acc / float64(n-1))
}

func maxLast(a []float64, n int) float64 {
	if len(a) == 0 {
		return 0
	}
	if n > len(a) {
		n = len(a)
	}
	mx := a[len(a)-n]
	for i := len(a) - n + 1; i < len(a); i++ {
		if a[i] > mx {
			mx = a[i]
		}
	}
	return mx
}

func minLast(a []float64, n int) float64 {
	if len(a) == 0 {
		return 0
	}
	if n > len(a) {
		n = len(a)
	}
	mn := a[len(a)-n]
	for i := len(a) - n + 1; i < len(a); i++ {
		if a[i] < mn {
			mn = a[i]
		}
	}
	return mn
}

func maxInts(vals ...int) int {
	if len(vals) == 0 {
		return 0
	}
	m := vals[0]
	for _, v := range vals[1:] {
		if v > m {
			m = v
		}
	}
	return m
}

func appendWithLimit(win []float64, v float64, limit int) []float64 {
	win = append(win, v)
	if limit > 0 && len(win) > limit {
		win = win[len(win)-limit:]
	}
	return win
}

func emaLast(vals []float64, period int) float64 {
	if len(vals) == 0 {
		return 0
	}
	if period <= 1 {
		return vals[len(vals)-1]
	}
	if period > len(vals) {
		period = len(vals)
	}
	alpha := 2.0 / (float64(period) + 1)
	ema := vals[len(vals)-period]
	for i := len(vals) - period + 1; i < len(vals); i++ {
		ema = alpha*vals[i] + (1-alpha)*ema
	}
	return ema
}

func softsign(x float64) float64 {
	if x == 0 {
		return 0
	}
	return x / (1 + math.Abs(x))
}

func rsiLast(vals []float64, period int) float64 {
	if period <= 1 || len(vals) <= period {
		return 50
	}
	gain := 0.0
	loss := 0.0
	for i := len(vals) - period; i < len(vals); i++ {
		diff := vals[i] - vals[i-1]
		if diff > 0 {
			gain += diff
		} else {
			loss -= diff
		}
	}
	if loss == 0 {
		return 100
	}
	rs := gain / (loss + 1e-9)
	return 100 - (100 / (1 + rs))
}

func trueRange(high, low, prevClose float64) float64 {
	rangeHL := high - low
	if rangeHL < 0 {
		rangeHL = 0
	}
	if prevClose <= 0 {
		return rangeHL
	}
	up := math.Abs(high - prevClose)
	down := math.Abs(low - prevClose)
	tr := rangeHL
	if up > tr {
		tr = up
	}
	if down > tr {
		tr = down
	}
	return tr
}

func annualizeVol(perBarStd float64, barMinutes int) float64 {
	if perBarStd <= 0 || barMinutes <= 0 {
		return 0
	}
	bars := (365.0 * 24 * 60) / float64(barMinutes)
	return perBarStd * math.Sqrt(bars)
}

// ==================== main ====================

func main() {
	runner, err := NewBacktestRunner("backtest_config.json")
	if err != nil {
		log.Fatal(err)
	}
	if err := runner.Run(); err != nil {
		log.Fatal(err)
	}
}
