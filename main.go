package main

import (
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math"
	"os"
	"path/filepath"
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
	UsePortfolio       bool     `json:"use_portfolio"`
	BarsLimit          int      `json:"bars_limit"`

	StrategyRiskTarget     float64 `json:"strategy_risk_target"`
	StrategyMaxAbsPosition float64 `json:"strategy_max_abs_position"`
	StrategyMaxLeverage    float64 `json:"strategy_max_leverage"`
	StrategyTrendGain      float64 `json:"strategy_trend_gain"`
	StrategyMRGain         float64 `json:"strategy_mr_gain"`
	StrategyBreakoutGain   float64 `json:"strategy_breakout_gain"`
	FallbackScale          float64 `json:"fallback_scale"`

	DebugFallbackMA    bool `json:"debug_fallback_ma"`
	DebugFallbackForce bool `json:"debug_fallback_force"`
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
}

// ==================== Strategy Adapter ====================

type StrategyAdapter struct {
	strategy *strategy.QuantMasterElite

	// Historical close price window for fallbacks
	hist          map[string][]float64
	peakPrice     map[string]float64
	highHist      map[string][]float64
	lowHist       map[string][]float64
	returnHist    map[string][]float64
	overlayATR    map[string]float64
	useMA         bool
	maFast        int
	maSlow        int
	useForceMom   bool
	fallbackScale float64 // scale for fallback targets in [-1, 1]

	fallbackMomentumLookback int
	fallbackMomentumThresh   float64
	minFallbackHoldBars      int
	lastFallbackTarget       map[string]float64
	fallbackHoldBars         map[string]int

	overlayBreakoutLookback int
	overlayVolLookback      int
	overlayTrendGain        float64
	overlayMomentumGain     float64
	overlayBreakoutGain     float64
	overlayDeadZone         float64
	overlayMeanRevGain      float64
	overlayEntryThreshold   float64
	overlayStopDrawdown     float64
	overlayTargetVol        float64
	overlayVolFloor         float64
	overlayMomShort         int
	overlayMomLong          int
	overlayMeanRevWindow    int
	overlayATRPeriod        int
	overlayHistLimit        int

	maxAbsTarget float64
	barMinutes   int

	signalLogger *strategy.SignalLogger
}

func (sa *StrategyAdapter) Name() string { return sa.strategy.Name() }

func (sa *StrategyAdapter) OnCandle(c backtest.Candle) []backtest.Signal {
	raw := sa.strategy.OnCandle(strategy.Candle{
		InstID: c.InstID, T: c.T, O: c.O, H: c.H, L: c.L, C: c.C, V: c.V,
	})

	out := make([]backtest.Signal, 0, 2)
	gotAbs := false

	// 1) Prefer absolute target from strategy meta (Meta.target) or by buy/sell/close
	for _, s := range raw {
		sa.logStrategySignal(s)
		tgt := 0.0
		if s.Meta != nil {
			if tv, ok := s.Meta["target"]; ok {
				switch vv := tv.(type) {
				case float64:
					tgt = vv
				case int:
					tgt = float64(vv)
				}
			}
		}
		if tgt == 0 {
			switch strings.ToLower(strings.TrimSpace(s.Side)) {
			case "buy":
				tgt = +s.Size
			case "sell":
				tgt = -s.Size
			case "close":
				tgt = 0
			default:
				continue
			}
		}
		maxAbs := sa.maxAbsTarget
		if maxAbs <= 0 {
			maxAbs = 1
		}
		tgt = clamp(tgt, -maxAbs, maxAbs)

		side := "close"
		size := 0.0
		if tgt > 0 {
			side, size = "buy", tgt
		} else if tgt < 0 {
			side, size = "sell", -tgt
		}
		out = append(out, backtest.Signal{
			InstID: s.InstID, Side: side, Size: size, Price: s.Price, Tag: "abs_target",
			Meta: s.Meta,
		})
		gotAbs = true
	}

	// 2) Fallbacks if no absolute target was produced
	if !gotAbs {
		// 2.1 Keep short history of closes
		if sa.hist == nil {
			sa.hist = make(map[string][]float64)
			sa.peakPrice = make(map[string]float64)
		}
		win := append(sa.hist[c.InstID], c.C)
		limit := sa.overlayHistLimit
		if limit <= 0 {
			limit = 2500
		}
		if len(win) > limit {
			win = win[len(win)-limit:]
		}
		sa.hist[c.InstID] = win

		if sa.highHist == nil {
			sa.highHist = make(map[string][]float64)
			sa.lowHist = make(map[string][]float64)
			sa.returnHist = make(map[string][]float64)
			sa.overlayATR = make(map[string]float64)
		}
		sa.highHist[c.InstID] = appendWithLimit(sa.highHist[c.InstID], c.H, limit)
		sa.lowHist[c.InstID] = appendWithLimit(sa.lowHist[c.InstID], c.L, limit)
		if len(win) >= 2 && win[len(win)-2] > 0 && c.C > 0 {
			ret := math.Log(c.C / win[len(win)-2])
			sa.returnHist[c.InstID] = appendWithLimit(sa.returnHist[c.InstID], ret, limit)
			prevClose := win[len(win)-2]
			tr := trueRange(c.H, c.L, prevClose)
			alpha := 2.0 / float64(maxInts(2, sa.overlayATRPeriod)+1)
			if sa.overlayATR[c.InstID] == 0 {
				sa.overlayATR[c.InstID] = tr
			} else {
				sa.overlayATR[c.InstID] = alpha*tr + (1-alpha)*sa.overlayATR[c.InstID]
			}
		}

		// 2.2 Adaptive overlay (trend + momentum + breakout mix)
		if sa.useMA {
			if tgt, ok := sa.computeOverlayTarget(c, win); ok {
				if final, changed := sa.handleFallbackTarget(c.InstID, tgt); changed {
					side, size := "close", 0.0
					if final > 0 {
						side, size = "buy", final
					} else if final < 0 {
						side, size = "sell", -final
					}
					meta := map[string]any{
						"target":  final,
						"fast":    sa.maFast,
						"slow":    sa.maSlow,
						"overlay": true,
					}
					sa.logAdapterSignal(c.InstID, side, size, c.C, "overlay_alpha", meta)
					out = append(out, backtest.Signal{
						InstID: c.InstID, Side: side, Size: size, Price: c.C, Tag: "overlay_alpha",
						Meta: meta,
					})
					gotAbs = true
				}
			}
		}

		// 2.3 Optional pure momentum fallback (kept for debugging parity)
		if !gotAbs && sa.useForceMom && len(win) > sa.fallbackMomentumLookback {
			ref := win[len(win)-sa.fallbackMomentumLookback-1]
			tgt := 0.0
			if ref > 0 {
				change := (c.C / ref) - 1
				switch {
				case change > sa.fallbackMomentumThresh:
					tgt = sa.fallbackScale
				case change < -sa.fallbackMomentumThresh:
					tgt = -sa.fallbackScale
				default:
					tgt = sa.lastFallbackTarget[c.InstID]
				}
			}
			if final, changed := sa.handleFallbackTarget(c.InstID, tgt); changed {
				side, size := "close", 0.0
				if final > 0 {
					side, size = "buy", final
				} else if final < 0 {
					side, size = "sell", -final
				}
				meta := map[string]any{"target": final}
				sa.logAdapterSignal(c.InstID, side, size, c.C, "fallback_momentum", meta)
				out = append(out, backtest.Signal{
					InstID: c.InstID, Side: side, Size: size, Price: c.C, Tag: "fallback_momentum",
					Meta: meta,
				})
				gotAbs = true
			}
		}
	}

	if len(out) == 0 {
		log.Printf("no signal for %s ts=%d", c.InstID, c.T)
	} else {
		log.Printf("emit %s ts=%d signals=%d tag=%s",
			c.InstID, c.T, len(out), out[0].Tag)
	}
	return out
}

func (sa *StrategyAdapter) OnTicker(t backtest.Ticker) []backtest.Signal {
	_ = sa.strategy.OnTicker(strategy.Ticker{
		InstID: t.InstID, Bid: t.Bid, Ask: t.Ask, BidSize: t.BidSize, AskSize: t.AskSize, Last: t.Last,
	})
	return nil
}

func (sa *StrategyAdapter) computeOverlayTarget(c backtest.Candle, win []float64) (float64, bool) {
	inst := c.InstID
	if len(win) == 0 {
		return 0, false
	}
	momShort := nonZeroOr(sa.overlayMomShort, 32)
	momLong := nonZeroOr(sa.overlayMomLong, 96)
	req := maxInts(sa.maSlow*2, sa.overlayVolLookback+8, sa.maFast+8, momLong+8)
	if len(win) < req {
		return 0, false
	}

	hi := sa.highHist[inst]
	lo := sa.lowHist[inst]
	rets := sa.returnHist[inst]
	if len(hi) == 0 || len(lo) == 0 || len(rets) < sa.overlayVolLookback {
		return 0, false
	}

	price := win[len(win)-1]
	fast := emaLast(win, sa.maFast)
	slow := emaLast(win, sa.maSlow)
	long := emaLast(win, sa.maSlow*2)
	if price <= 0 || fast <= 0 || slow <= 0 || long <= 0 {
		return 0, false
	}

	perBarVol := stdLast(rets, sa.overlayVolLookback)
	if perBarVol <= 0 {
		return 0, false
	}

	// Core signals
	if len(win) <= momLong {
		return 0, false
	}

	breakoutBias := 0.0
	breakoutLook := maxInts(sa.overlayBreakoutLookback, sa.maSlow)
	if breakoutLook > len(hi) {
		breakoutLook = len(hi)
	}
	if breakoutLook > 0 {
		hiRef := maxLast(hi, breakoutLook)
		loRef := minLast(lo, breakoutLook)
		if hiRef > 0 && price >= hiRef*(1-sa.overlayEntryThreshold) {
			breakoutBias = 1
		} else if loRef > 0 && price <= loRef*(1+sa.overlayEntryThreshold) {
			breakoutBias = -1
		}
	}

	mean := smaLast(win, sa.maSlow)
	std := stdLast(win, sa.maSlow)
	meanRev := 0.0
	if std > 0 {
		meanRev = (price - mean) / std
	}

	trendScore := softsign((fast - slow) / slow * sa.overlayTrendGain)
	macroScore := softsign((slow - long) / long * (sa.overlayTrendGain * 0.5))
	momentumShort := (price / win[len(win)-momShort]) - 1
	momentumLong := (price / win[len(win)-momLong]) - 1
	momentumScore := softsign(momentumShort*sa.overlayMomentumGain*1.2 + momentumLong*sa.overlayMomentumGain*0.4)
	breakoutScore := breakoutBias * sa.overlayBreakoutGain
	meanScore := softsign(meanRev * sa.overlayMeanRevGain)

	raw := trendScore*0.45 + macroScore*0.2 + momentumScore*0.35 + breakoutScore*0.4 - meanScore*0.2

	entryEdge := sa.overlayEntryThreshold
	if entryEdge <= 0 {
		entryEdge = 0.08
	}
	if math.Abs(raw) < entryEdge {
		prev := sa.lastFallbackTarget[inst]
		return prev, false
	}

	target := math.Tanh(raw) * sa.maxAbsTarget

	peak := sa.peakPrice[inst]
	if price > peak {
		peak = price
	}
	sa.peakPrice[inst] = peak
	stopDD := sa.overlayStopDrawdown
	if stopDD <= 0 {
		stopDD = 0.08
	}
	if peak > 0 {
		dd := (peak - price) / peak
		if dd > stopDD {
			target = 0
		}
	}

	// Volatility scaling
	annVol := annualizeVol(perBarVol, sa.barMinutes)
	targetVol := sa.overlayTargetVol
	if targetVol <= 0 {
		targetVol = 1.0
	}
	volFloor := sa.overlayVolFloor
	if volFloor <= 0 {
		volFloor = 0.25
	}
	annVol = math.Max(annVol, volFloor)
	volScale := clamp(targetVol/annVol, 0.25, 2.5)

	baseScale := clamp(sa.fallbackScale, 0.2, 1.25)
	target = clamp(target*baseScale*volScale, -sa.maxAbsTarget, sa.maxAbsTarget)

	prev, seen := sa.lastFallbackTarget[inst]
	if !seen {
		return target, true
	}
	if math.Abs(target-prev) < sa.overlayDeadZone {
		return prev, false
	}
	return target, true
}

func (sa *StrategyAdapter) handleFallbackTarget(inst string, proposed float64) (float64, bool) {
	prev := sa.lastFallbackTarget[inst]
	hold := sa.fallbackHoldBars[inst]
	if math.Abs(proposed-prev) < 1e-9 {
		sa.fallbackHoldBars[inst] = hold + 1
		return prev, false
	}
	if prev != 0 && proposed != prev && hold < sa.minFallbackHoldBars {
		sa.fallbackHoldBars[inst] = hold + 1
		return prev, false
	}
	sa.lastFallbackTarget[inst] = proposed
	sa.fallbackHoldBars[inst] = 0
	return proposed, true
}

func (sa *StrategyAdapter) logStrategySignal(sig strategy.Signal) {
	if sa.signalLogger == nil {
		return
	}
	if err := sa.signalLogger.LogSignal(sig); err != nil {
		log.Printf("signal log error: %v", err)
	}
}

func (sa *StrategyAdapter) logAdapterSignal(inst, side string, size, price float64, tag string, meta map[string]any) {
	if sa.signalLogger == nil {
		return
	}
	payload := map[string]any{}
	for k, v := range meta {
		payload[k] = v
	}
	sig := strategy.Signal{
		InstID: inst,
		Side:   side,
		Size:   size,
		Price:  price,
		Tag:    tag,
		Meta:   payload,
	}
	if err := sa.signalLogger.LogSignal(sig); err != nil {
		log.Printf("signal log error: %v", err)
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
	config     BacktestConfig
	strategy   *strategy.QuantMasterElite
	portfolio  *portfolio.Engine
	backtest   *backtest.Engine
	barMinutes int
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
	br.wirePortfolioLayer()

	result := br.backtest.Run(series)
	printResults(result)
	saveAll(result)

	log.Printf("Finished in %v", time.Since(start))
	return nil
}

// wireStrategyLayer connects the strategy implementation to the backtest engine.
func (br *BacktestRunner) wireStrategyLayer() {
	sa := &StrategyAdapter{
		strategy:                 br.strategy,
		useMA:                    br.config.DebugFallbackMA,
		maFast:                   24,
		maSlow:                   96,
		useForceMom:              br.config.DebugFallbackForce,
		fallbackScale:            nonZeroOrFloat(br.config.FallbackScale, 1.0),
		fallbackMomentumLookback: 48,
		fallbackMomentumThresh:   0.0015,
		minFallbackHoldBars:      6,
		lastFallbackTarget:       make(map[string]float64),
		fallbackHoldBars:         make(map[string]int),
		overlayBreakoutLookback:  144,
		overlayVolLookback:       96,
		overlayTrendGain:         14.0,
		overlayMomentumGain:      6.5,
		overlayBreakoutGain:      0.9,
		overlayDeadZone:          0.04,
		overlayMeanRevGain:       1.25,
		overlayEntryThreshold:    0.0008,
		overlayStopDrawdown:      0.08,
		overlayTargetVol:         nonZeroOrFloat(br.config.StrategyRiskTarget, 1.0),
		overlayVolFloor:          0.35,
		overlayMomShort:          32,
		overlayMomLong:           96,
		overlayMeanRevWindow:     96,
		overlayATRPeriod:         48,
		overlayHistLimit:         2500,
		maxAbsTarget:             nonZeroOrFloat(br.config.StrategyMaxAbsPosition, 1.0),
		barMinutes:               br.barMinutes,
		signalLogger:             strategy.DefaultSignalLogger(),
	}
	br.backtest.SetStrategy(sa)
}

// wirePortfolioLayer connects the portfolio engine if enabled.
func (br *BacktestRunner) wirePortfolioLayer() {
	if !br.config.UsePortfolio || br.portfolio == nil {
		return
	}
	pa := &PortfolioAdapter{portfolio: br.portfolio}
	br.backtest.SetPortfolio(pa)
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
		TrendGain:        nonZeroOrFloat(cfg.StrategyTrendGain, 3.0),
		MRWindows:        []int{10, 20},
		MRGain:           nonZeroOrFloat(cfg.StrategyMRGain, 0.30),
		BreakoutLookback: 20,
		BRGain:           nonZeroOrFloat(cfg.StrategyBreakoutGain, 1.0),

		VolWindow:          120,
		TargetVolAnnual:    nonZeroOrFloat(cfg.StrategyRiskTarget, 1.0),
		VolTargetSmoothing: 0.08,
		MinSigmaAnnual:     0.06,

		MaxAbsPosition:   nonZeroOrFloat(cfg.StrategyMaxAbsPosition, 2.0),
		MaxLeverage:      nonZeroOrFloat(cfg.StrategyMaxLeverage, 3.0),
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
		MaxAbsPosition:   nonZeroOrFloat(cfg.StrategyMaxAbsPosition, 1.0),
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

func printResults(r backtest.Result) {
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
	log.Printf("------------------------------------------------------------")
	log.Printf("Trades Detail")
	log.Printf("------------------------------------------------------------")
	logTrades(r.Trades)
	log.Printf("============================================================\n")
}

func saveAll(r backtest.Result) {
	_ = os.MkdirAll("./backtest_results", 0o755)
	_ = saveJSON("./backtest_results/stats.json", map[string]any{
		"final_equity": r.FinalEquity,
		"total_return": r.TotalRet,
		"cagr":         r.CAGR,
		"sharpe":       r.Sharpe,
		"max_dd":       r.MaxDD,
		"win_rate":     r.WinRate,
		"num_trades":   r.NumTrades,
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
	_ = w.Write([]string{"idx", "instrument", "dir", "entry_ts", "entry_utc", "entry_price", "exit_ts", "exit_utc", "exit_price", "size", "return_pct", "holding_minutes"})
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
		})
	}
	return nil
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
			StartDate:              "2024-01-01",
			EndDate:                "2024-12-01",
			InitialCash:            10000,
			Instruments:            []string{"BTC-USDT-SWAP"},
			Timeframe:              "15m",
			DataSource:             "api",
			DataPath:               "./data/candles",
			AutoFetchIfMissing:     true,
			UsePortfolio:           false,
			BarsLimit:              2000,
			StrategyRiskTarget:     1.2,
			StrategyMaxAbsPosition: 2.5,
			StrategyMaxLeverage:    4.0,
			StrategyTrendGain:      3.8,
			StrategyMRGain:         0.45,
			StrategyBreakoutGain:   1.4,
			FallbackScale:          1.8,
			DebugFallbackMA:        true,
			DebugFallbackForce:     false,
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
