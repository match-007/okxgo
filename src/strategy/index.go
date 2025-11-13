package strategy

import (
	"math"
	"math/rand"
	"sync"
	"time"
)

// ===============================================================================
// 常量：因子索引
// ===============================================================================
const (
	FTrend = iota
	FMR
	FBreakout
	FCarry
	FBasis
	FMicro
	FMomentum
	FVolRegime
	FNum // 因子总数
)

// 制度枚举（用于条件IC）
const (
	RegTrending = iota
	RegRanging
	RegVolatile
	RegNum
)

// ===============================================================================
// 核心参数定义
// ===============================================================================

type EliteParams struct {
	// === 因子配置 ===
	TrendWindows     []int
	TrendGain        float64
	MRWindows        []int
	MRGain           float64
	BreakoutLookback int
	BRGain           float64
	CarryHalfLife    int
	CarryGain        float64
	BasisHalfLife    int
	BasisGain        float64
	MicroHalfLife    int
	MicroGain        float64

	// === 智能学习 ===
	ICHalfLife    int
	ICMinSamples  int
	RegimeWindow  int
	MetaLearnRate float64

	// === 风控系统 ===
	VolWindow       int
	TargetVolAnnual float64
	MaxAbsPosition  float64
	MaxLeverage     float64
	StopLossATR     float64
	TrailingStopATR float64
	MaxDrawdownPct  float64

	// === 执行优化 ===
	TimeframeMinutes int
	EntryThreshold   float64
	ExitThreshold    float64
	CooldownBars     int
	MinPositionDelta float64

	// === 成本模型（Bps = 万分比） ===
	TakerFeeBps float64
	MakerFeeBps float64
	SlippageBps float64
	ImpactCoef  float64 // 线性影响系数 ~ size * (ATR/price)
	// UPDATED: 二次冲击项（非线性冲击）
	ImpactCoefQuad float64 // ~ (size)^2

	// === 交易所约束 ===
	LotSize            float64 // 最小下单量（合约张数/名义）
	TickSize           float64 // 最小报价跳动
	PriceStep          float64 // 部分交易所需要（可为 0 关闭）
	ContractMultiplier float64 // UPDATED: 合约乘数（每 1 合约、价格变动 1 单位的名义USD）

	// === 高级特性 ===
	UseAdaptiveStop  bool
	UseRegimeFilter  bool
	UseMetaLearning  bool
	UseLiquidityGate bool

	// === 性能追踪 ===
	PerformanceWindow int

	// === 期望-成本判定系数 ===
	EdgeCostCoef float64 // edge 必须大于 cost*coef 才执行

	// === 波动突变保护 ===
	VolTargetSmoothing float64 // 0~1，小到更稳
	MinSigmaAnnual     float64 // 年化波动率下限（避免超配）

	// === 可复现 ===
	Seed int64
}

func (p *EliteParams) withDefaults() EliteParams {
	q := *p

	// —— 仅对“窗口/必要阈值/硬性参数”提供默认 —— //
	if len(q.TrendWindows) == 0 {
		q.TrendWindows = []int{8, 16, 32, 64}
	}
	if len(q.MRWindows) == 0 {
		q.MRWindows = []int{10, 20, 40}
	}
	if q.BreakoutLookback == 0 {
		q.BreakoutLookback = 20
	}
	if q.CarryHalfLife == 0 {
		q.CarryHalfLife = 96
	}
	if q.BasisHalfLife == 0 {
		q.BasisHalfLife = 96
	}
	if q.MicroHalfLife == 0 {
		q.MicroHalfLife = 16
	}

	if q.ICHalfLife == 0 {
		q.ICHalfLife = 128
	}
	if q.ICMinSamples == 0 {
		q.ICMinSamples = 150
	}
	if q.RegimeWindow == 0 {
		q.RegimeWindow = 48
	}
	if q.MetaLearnRate == 0 {
		q.MetaLearnRate = 0.01
	}

	if q.VolWindow == 0 {
		q.VolWindow = 64
	}
	if q.TargetVolAnnual == 0 {
		q.TargetVolAnnual = 0.20
	}
	if q.MaxAbsPosition == 0 {
		q.MaxAbsPosition = 1.0
	}
	if q.MaxLeverage == 0 {
		q.MaxLeverage = 3.0
	}
	if q.StopLossATR == 0 {
		q.StopLossATR = 2.5
	}
	if q.TrailingStopATR == 0 {
		q.TrailingStopATR = 3.5
	}
	if q.MaxDrawdownPct == 0 {
		q.MaxDrawdownPct = 0.15
	}

	if q.TimeframeMinutes == 0 {
		q.TimeframeMinutes = 5
	}
	if q.EntryThreshold == 0 {
		q.EntryThreshold = 0.50
	}
	if q.ExitThreshold == 0 {
		q.ExitThreshold = 0.15
	}
	if q.CooldownBars == 0 {
		q.CooldownBars = 3
	}
	if q.MinPositionDelta == 0 {
		q.MinPositionDelta = 0.15
	}

	if q.PerformanceWindow == 0 {
		q.PerformanceWindow = 1000
	}

	// —— 加密默认成本/冲击参数 —— //
	if q.TakerFeeBps == 0 {
		q.TakerFeeBps = 5 // 0.05%
	}
	if q.MakerFeeBps == 0 {
		q.MakerFeeBps = 2 // 0.02%
	}
	if q.SlippageBps == 0 {
		q.SlippageBps = 2 // 0.02%
	}
	if q.ImpactCoef == 0 {
		q.ImpactCoef = 1.0 // 经验初值，请按品种标定
	}
	// UPDATED: 默认二次冲击项略小
	if q.ImpactCoefQuad == 0 {
		q.ImpactCoefQuad = 0.35
	}
	// UPDATED: 合约乘数默认 1（USDT 合约通常≈1）
	if q.ContractMultiplier == 0 {
		q.ContractMultiplier = 1.0
	}

	if q.EdgeCostCoef == 0 {
		q.EdgeCostCoef = 1.2
	}
	if q.VolTargetSmoothing == 0 {
		q.VolTargetSmoothing = 0.15 // 越小越稳
	}
	if q.MinSigmaAnnual == 0 {
		q.MinSigmaAnnual = 0.10 // 年化波动率下限
	}

	return q
}

// ===============================================================================
// 状态
// ===============================================================================

type eliteState struct {
	mu sync.Mutex

	// 基础数据
	closes, highs, lows *ringBuf
	volumes             *ringBuf
	atr                 *atrCalc
	retStd              *rollingStd
	longVol             *ewVar

	// 技术指标
	trendEMAs []*ema
	mrBands   []*bollingerBand
	rsi       *rsiCalc

	// 高级因子
	carryEMA *ema
	carryVar *ewVar
	basisEMA *ema
	basisVar *ewVar

	// 微结构
	microPressure    *ema // 订单流压力 EMA
	lastBid, lastAsk float64
	bidSize, askSize float64
	lastTrade        float64

	// 学习/正交
	factorIC []intelligentIC
	// UPDATED: 按制度维护条件IC
	factorICRegime [RegNum][]intelligentIC
	factorDecay    []float64
	factorCovs     []*ewCorr // 与趋势基因子协方差跟踪
	factorVars     []*ewVar  // 每个因子的均值/方差用于自标准化
	trendVar       *ewVar    // 趋势基因子方差
	prevFactors    []float64
	lastReturn     float64
	regimeState    *regimeDetector
	metaLearner    *metaOptimizer

	// 风控/绩效
	riskManager *advancedRisk
	perfTracker *performanceTracker

	// 执行状态
	position      float64 // 合约张数
	lastSignalPos float64
	entryPrice    float64 // 开仓加权均价（标的价格）
	entryBar      int
	maxFavorable  float64
	cooldown      int
	totalBars     int

	// 再入场缓冲
	reentryArm bool

	// 稳健化：目标仓位平滑
	targetPosEMA    float64
	targetPosInited bool

	// 波动目标突变保护
	lastSigmaAnn float64
}

// ===============================================================================
// 主体
// ===============================================================================

type QuantMasterElite struct {
	name   string
	params EliteParams
	// 全局
	statesMu   sync.RWMutex
	states     map[string]*eliteState
	globalPerf *performanceTracker
}

func NewQuantMasterElite(p EliteParams) *QuantMasterElite {
	pp := p.withDefaults()
	// 可复现随机种子
	if pp.Seed != 0 {
		rand.Seed(pp.Seed)
	} else {
		rand.Seed(time.Now().UnixNano())
	}
	return &QuantMasterElite{
		name:       "quantmaster_elite_v4.4_crypto", // UPDATED: 版本号
		params:     pp,
		states:     make(map[string]*eliteState),
		globalPerf: newPerformanceTracker(pp.PerformanceWindow, pp.TimeframeMinutes),
	}
}

func (qm *QuantMasterElite) Name() string { return qm.name }

// ===============================================================================
// OnCandle
// ===============================================================================

func (qm *QuantMasterElite) OnCandle(c Candle) []Signal {
	st := qm.getOrCreateState(c.InstID)
	st.mu.Lock()
	defer st.mu.Unlock()

	st.totalBars++

	// 数据推进
	st.closes.push(c.C)
	st.highs.push(c.H)
	st.lows.push(c.L)
	st.volumes.push(c.V)
	st.atr.push(c.H, c.L, c.C)

	// 标的 log-return
	prevClose := st.closes.getN(1)
	if !math.IsNaN(prevClose) && prevClose > 0 {
		ret := math.Log(c.C / prevClose)
		st.retStd.push(ret)
		st.longVol.push(ret)
		st.lastReturn = ret
	}

	for _, e := range st.trendEMAs {
		e.push(c.C)
	}
	for _, bb := range st.mrBands {
		bb.push(c.C)
	}
	st.rsi.push(c.C)

	if !qm.isReady(st) {
		return nil
	}

	// Regime：用 EMA 斜率强度（|fast-slow|/price）
	trendStrength := 0.0
	if len(st.trendEMAs) >= 2 && c.C > 0 {
		fast := st.trendEMAs[0].val()
		slow := st.trendEMAs[len(st.trendEMAs)-1].val()
		trendStrength = math.Abs(fast-slow) / c.C
	}
	if qm.params.UseRegimeFilter {
		st.regimeState.update(c.C, st.atr.val(), trendStrength)
	}

	// 因子
	rawF := qm.computeAllFactors(st, c)

	// 自标准化（各自 EW 均值/方差）
	normF := qm.intelligentNormalization(st, rawF)

	// 正交化：对 Trend 去投影
	orthoF := qm.dynamicOrthogonalization(st, normF)

	// 权重学习（条件IC + 元学习）
	weights := qm.learnFactorWeights(st, orthoF)

	// 制度驱动的因子缩放（保留趋势/突破在 volatile）
	if qm.params.UseRegimeFilter {
		weights = st.regimeState.adjustWeights(weights, orthoF)
	}

	// 合成信号
	raw := 0.0
	for i, f := range orthoF {
		raw += weights[i] * f
	}
	signal := tanhClamp(raw, 4.0)
	targetPos := signal * qm.params.MaxAbsPosition

	// 波动率目标化（含突变保护）
	targetPos = qm.volatilityScaling(st, targetPos)

	// 风控
	st.riskManager.currentDD = st.perfTracker.maxDrawdown()
	targetPos = st.riskManager.applyRiskLimits(targetPos, c.C, st.entryPrice)

	// 流动性门控
	if qm.params.UseLiquidityGate {
		targetPos = qm.liquidityGate(st, targetPos, c)
	}

	// 制度“软门”：按 regime/趋势强度缩放（UPDATED：volatile 设最小门控）
	gate := 1.0
	if qm.params.UseRegimeFilter {
		switch st.regimeState.currentRegime {
		case "trending":
			gate = 1.0
		case "ranging":
			lo, hi := 0.0001, 0.0007 // 加密 5m 更低阈
			x := (trendStrength - lo) / (hi - lo)
			if x < 0 {
				x = 0
			} else if x > 1 {
				x = 1
			}
			gate = 0.20 + 0.70*x // 0.20~0.90（略放松）
		case "volatile":
			gate = 0.30 // UPDATED: 不再清零，保留最小暴露
		}
	}
	targetPos *= gate

	// 目标仓位 EMA 平滑（制度自适应：trending 下更快）
	{
		posSmoothing := 0.12
		if st.regimeState.mode() == RegTrending {
			posSmoothing = 0.18
		} else if st.regimeState.mode() == RegVolatile {
			posSmoothing = 0.10
		}
		if !st.targetPosInited {
			st.targetPosEMA = targetPos
			st.targetPosInited = true
		} else {
			st.targetPosEMA = posSmoothing*targetPos + (1-posSmoothing)*st.targetPosEMA
		}
		targetPos = st.targetPosEMA
	}

	// 生成交易信号（Δ 下单，带再入场优化）
	signals := qm.generateTradeSignals(st, targetPos, c)

	// 绩效更新：返回本期策略收益供全局聚合
	rStrat := st.perfTracker.update(st.position, st.lastReturn)
	qm.globalPerf.updateStrat(rStrat)

	// 因子健康
	qm.monitorFactorHealth(st, orthoF)

	return signals
}

// ===============================================================================
// OnTicker
// ===============================================================================

func (qm *QuantMasterElite) OnTicker(t Ticker) []Signal {
	st := qm.getOrCreateState(t.InstID)
	st.mu.Lock()
	defer st.mu.Unlock()

	if t.Bid > 0 && t.Ask > 0 && t.Ask > t.Bid {
		st.lastBid, st.lastAsk = t.Bid, t.Ask
		st.bidSize, st.askSize = t.BidSize, t.AskSize
		total := st.askSize + st.bidSize
		if total > 0 && st.microPressure != nil {
			pressure := (st.askSize - st.bidSize) / total // 正=卖压
			st.microPressure.push(pressure)
		}
	}
	if t.Last > 0 {
		st.lastTrade = t.Last
	}
	return nil
}

// ===============================================================================
// 因子计算
// ===============================================================================

type rawFactor struct {
	name  string
	value float64
}

func (qm *QuantMasterElite) computeAllFactors(st *eliteState, c Candle) []rawFactor {
	f := make([]rawFactor, FNum)

	// Trend
	trendVal := qm.computeTrendComposite(st) * qm.params.TrendGain
	f[FTrend] = rawFactor{"trend", trendVal}

	// Mean Reversion
	mrVal := qm.computeMeanReversion(st, c.C) * qm.params.MRGain
	f[FMR] = rawFactor{"mr", mrVal}

	// Breakout
	brVal := qm.computeBreakout(st, c) * qm.params.BRGain
	f[FBreakout] = rawFactor{"breakout", brVal}

	// Carry（资金费率）
	if st.carryEMA != nil {
		f[FCarry] = rawFactor{"carry", st.carryEMA.val() * qm.params.CarryGain}
	}

	// Basis（期现基差，做反向）
	if st.basisEMA != nil {
		f[FBasis] = rawFactor{"basis", -st.basisEMA.val() * qm.params.BasisGain}
	}

	// Micro（反人性微回归）
	microVal := 0.0
	if st.microPressure != nil {
		microVal = -st.microPressure.val() * qm.params.MicroGain
	}
	f[FMicro] = rawFactor{"micro", microVal}

	// Momentum（加速度）
	mom := qm.computeMomentum(st)
	f[FMomentum] = rawFactor{"momentum", mom}

	// Volatility Regime（短/长波动比）
	volF := qm.computeVolatilityFactor(st)
	f[FVolRegime] = rawFactor{"vol_regime", volF}

	return f
}

// 趋势复合：多周期 EMA 斜率按 1/sqrt(window) 加权
func (qm *QuantMasterElite) computeTrendComposite(st *eliteState) float64 {
	n := len(st.trendEMAs)
	if n < 2 {
		return 0
	}
	sw, s := 0.0, 0.0
	for i := 1; i < n; i++ {
		fast := st.trendEMAs[i-1].val()
		slow := st.trendEMAs[i].val()
		slope := fast - slow
		w := 1.0 / math.Sqrt(float64(maxi(2, st.trendEMAs[i].window)))
		s += w * slope
		sw += w
	}
	if sw == 0 {
		return 0
	}
	return s / sw
}

func (qm *QuantMasterElite) computeMeanReversion(st *eliteState, price float64) float64 {
	if len(st.mrBands) == 0 {
		return 0
	}
	sum := 0.0
	for _, bb := range st.mrBands {
		sum += -bb.zscore(price)
	}
	return sum / float64(len(st.mrBands))
}

func (qm *QuantMasterElite) computeBreakout(st *eliteState, c Candle) float64 {
	lb := qm.params.BreakoutLookback
	if st.highs.count < lb || st.lows.count < lb {
		return 0
	}
	h := st.highs.max(lb)
	l := st.lows.min(lb)
	if h <= l {
		return 0
	}
	return 2.0*(c.C-l)/(h-l) - 1.0
}

func (qm *QuantMasterElite) computeMomentum(st *eliteState) float64 {
	if st.closes.count < 11 {
		return 0
	}
	ret5 := safeLog(st.closes.getN(0) / st.closes.getN(5))
	ret10 := safeLog(st.closes.getN(5) / st.closes.getN(10))
	return ret5 - ret10
}

func (qm *QuantMasterElite) computeVolatilityFactor(st *eliteState) float64 {
	recent := st.retStd.std()
	long := st.longVol.std()
	if long <= 0 {
		long = recent
	}
	if long <= 0 {
		return 0
	}
	return 1.0 - (recent / long)
}

// ===============================================================================
// 标准化 & 正交化
// ===============================================================================

func (qm *QuantMasterElite) intelligentNormalization(st *eliteState, factors []rawFactor) []float64 {
	norm := make([]float64, FNum)
	for i := 0; i < FNum; i++ {
		val := factors[i].value
		m := st.factorVars[i].meanVal()
		s := st.factorVars[i].std()
		if s < 1e-6 {
			s = 1e-6
		}
		z := (val - m) / s
		// winsor
		if z > 5 {
			z = 5
		} else if z < -5 {
			z = -5
		}
		norm[i] = z
	}
	// 推进历史
	for i := 0; i < FNum; i++ {
		st.factorVars[i].push(factors[i].value)
	}
	return norm
}

func (qm *QuantMasterElite) dynamicOrthogonalization(st *eliteState, f []float64) []float64 {
	if len(f) < 2 {
		return f
	}
	out := make([]float64, len(f))
	copy(out, f)

	// 基因子（趋势）的方差
	base := f[FTrend]
	st.trendVar.push(base)
	varBase := st.trendVar.std()
	varBase2 := varBase * varBase
	if varBase2 < 1e-9 {
		return out
	}

	// 维护与趋势的协方差并去投影
	for i := 0; i < len(out); i++ {
		if i == FTrend {
			continue
		}
		st.factorCovs[i].push(f[i], base)
		cov := st.factorCovs[i].cov
		beta := cov / varBase2
		out[i] = out[i] - beta*base
	}
	return out
}

// ===============================================================================
// 权重学习（UPDATED: 使用“条件IC”优先）
// ===============================================================================

func (qm *QuantMasterElite) learnFactorWeights(st *eliteState, f []float64) []float64 {
	n := len(f)
	w := make([]float64, n)

	// 更新 IC：上一期因子 vs 本期收益（全局 + 条件IC）
	if !math.IsNaN(st.lastReturn) && len(st.prevFactors) >= n {
		for i := 0; i < n; i++ {
			st.factorIC[i].update(st.prevFactors[i], st.lastReturn)
			mode := st.regimeState.mode()
			st.factorICRegime[mode][i].update(st.prevFactors[i], st.lastReturn)
		}
	}
	copy(st.prevFactors, f)

	// 取当前制度的 IC 为主，若样本不足则回落到全局IC
	mode := st.regimeState.mode()
	sufficient := st.factorICRegime[mode][FTrend].samples >= qm.params.ICMinSamples
	sourceIC := st.factorICRegime[mode]
	if !sufficient {
		sourceIC = st.factorIC
	}

	if sourceIC[FTrend].samples < qm.params.ICMinSamples {
		copy(w, qm.getExpertWeights(n))
		return w
	}

	sum := 0.0
	for i := 0; i < n; i++ {
		ic := sourceIC[i].getIC()
		dec := st.factorDecay[i]
		val := softplus(ic) * dec
		if val < 0 {
			val = 0
		}
		w[i] = val
		sum += val
	}
	if sum <= 0 {
		for i := range w {
			w[i] = 1 / float64(n)
		}
		return w
	}
	for i := range w {
		w[i] /= sum
	}

	// 元学习：轻度探索/利用 + 归一化
	if qm.params.UseMetaLearning && st.metaLearner != nil {
		w = st.metaLearner.optimize(w, st.perfTracker.sharpe())
		ws := 0.0
		for i := range w {
			if w[i] < 0 {
				w[i] = 0
			}
			ws += w[i]
		}
		if ws > 0 {
			for i := range w {
				w[i] /= ws
			}
		}
	}
	return w
}

func (qm *QuantMasterElite) getExpertWeights(n int) []float64 {
	// 加密合约：趋势/突破/动量偏重，MR 次之
	base := []float64{0.48, 0.12, 0.20, 0.00, 0.00, 0.05, 0.10, 0.05} // 微调

	w := make([]float64, n)
	sum := 0.0
	for i := 0; i < n && i < len(base); i++ {
		w[i] = base[i]
		sum += w[i]
	}
	if sum <= 0 {
		for i := range w {
			w[i] = 1 / float64(maxi(1, n))
		}
		return w
	}
	for i := range w {
		w[i] /= sum
	}
	return w
}

// ===============================================================================
// 缩放 / 门控
// ===============================================================================

func (qm *QuantMasterElite) volatilityScaling(st *eliteState, pos float64) float64 {
	ann := math.Sqrt((365 * 24 * 60) / float64(maxi(1, qm.params.TimeframeMinutes)))
	sigma := st.retStd.std()
	sigmaAnn := sigma * ann
	// 突变保护和平滑
	if st.lastSigmaAnn == 0 {
		st.lastSigmaAnn = sigmaAnn
	}
	// 下限保护
	if sigmaAnn < qm.params.MinSigmaAnnual {
		sigmaAnn = qm.params.MinSigmaAnnual
	}
	// 平滑（自适应：在 trending 放松平滑以更快响应）
	alpha := qm.params.VolTargetSmoothing
	if st.regimeState.mode() == RegTrending {
		alpha = math.Max(0.10, 0.7*alpha)
	}
	sig := alpha*sigmaAnn + (1-alpha)*st.lastSigmaAnn
	st.lastSigmaAnn = sig
	if sig <= 0 {
		sig = 1e-6
	}
	scaled := pos * (qm.params.TargetVolAnnual / sig)
	maxPos := qm.params.MaxAbsPosition * qm.params.MaxLeverage
	return clamp(scaled, -maxPos, maxPos)
}

func (qm *QuantMasterElite) liquidityGate(st *eliteState, pos float64, c Candle) float64 {
	// UPDATED: 用名义成交量门控（跨品种更稳）
	if c.V <= 0 || c.C <= 0 {
		return 0
	}
	mult := qm.params.ContractMultiplier
	nominalVol := c.V * c.C * mult
	avgNominal := st.volumes.mean(20) * c.C * mult
	if avgNominal <= 0 {
		return pos * 0.4
	}
	ratio := nominalVol / avgNominal
	if ratio < 0.5 {
		return pos * 0.35
	}
	if ratio < 0.8 {
		return pos * 0.75
	}
	return pos
}

// ===============================================================================
// 信号生成（Δ 下单）—— 成本一致化 & 交易所约束 + 再入场
// ===============================================================================

func (qm *QuantMasterElite) generateTradeSignals(st *eliteState, targetPos float64, c Candle) []Signal {
	var sigs []Signal
	cur := st.position
	delta := targetPos - cur

	// 单根 Δ 上限
	const maxDeltaPerBar = 0.45
	if delta > maxDeltaPerBar {
		delta = maxDeltaPerBar
	}
	if delta < -maxDeltaPerBar {
		delta = -maxDeltaPerBar
	}

	// 冷却
	if st.cooldown > 0 {
		st.cooldown--
		// 允许“再入场”在冷却期间放宽（轻微）
		if !st.reentryArm {
			return nil
		}
	}

	// 自适应止损（制度自适应阈值）
	if qm.params.UseAdaptiveStop {
		stopATR := qm.params.StopLossATR
		trailATR := qm.params.TrailingStopATR
		if st.regimeState.mode() == RegTrending {
			trailATR += 0.4
		} else if st.regimeState.mode() == RegRanging {
			stopATR -= 0.3
		}
		if stopATR < 1.5 {
			stopATR = 1.5
		}
		if trailATR < 2.5 {
			trailATR = 2.5
		}
		// 临时覆写
		origStop, origTrail := st.riskManager.params.StopLossATR, st.riskManager.params.TrailingStopATR
		st.riskManager.params.StopLossATR, st.riskManager.params.TrailingStopATR = stopATR, trailATR

		if stop := st.riskManager.checkAdaptiveStop(c.InstID, cur, c.C, st.entryPrice, st.maxFavorable, st.atr.val()); stop != nil {
			sigs = append(sigs, *stop)
			_ = getLogger().LogSignal(*stop)

			pnl := qm.estimatePnL(st, c.C)
			st.perfTracker.totalTrades++
			if pnl > 0 {
				st.perfTracker.winTrades++
			}
			st.position = 0
			st.lastSignalPos = 0
			st.entryPrice = 0
			st.maxFavorable = 0
			st.cooldown = qm.params.CooldownBars
			st.reentryArm = true // 止损后允许快速再入
			// 还原
			st.riskManager.params.StopLossATR, st.riskManager.params.TrailingStopATR = origStop, origTrail
			return sigs
		}
		// 还原
		st.riskManager.params.StopLossATR, st.riskManager.params.TrailingStopATR = origStop, origTrail
	}

	// 跟踪最有利价格
	if cur > 0 {
		if c.C > st.maxFavorable || st.maxFavorable == 0 {
			st.maxFavorable = c.C
		}
	} else if cur < 0 {
		if c.C < st.maxFavorable || st.maxFavorable == 0 {
			st.maxFavorable = c.C
		}
	}

	// —— 部分止盈（先落袋一半，制度自适应） —— //
	tpATR := 2.2
	if st.regimeState.mode() == RegRanging {
		tpATR = 1.8
	}
	if st.entryPrice > 0 && st.atr.val() > 0 && st.position != 0 {
		dist := st.atr.val() * tpATR
		if st.position > 0 && c.C >= st.entryPrice+dist {
			size := 0.5 * math.Abs(st.position)
			s := Signal{InstID: c.InstID, Side: "sell", Size: qm.roundSize(size), Price: qm.roundPrice(c.C), Tag: "tp_half", Meta: map[string]any{"reason": "take_profit_half"}}
			sigs = append(sigs, s)
			_ = getLogger().LogSignal(s)
			st.position -= size
			if st.position < 0 {
				st.position = 0
			}
			st.cooldown = maxi(1, qm.params.CooldownBars/2)
			st.reentryArm = true // 允许回撤后再入
		} else if st.position < 0 && c.C <= st.entryPrice-dist {
			size := 0.5 * math.Abs(st.position)
			s := Signal{InstID: c.InstID, Side: "buy", Size: qm.roundSize(size), Price: qm.roundPrice(c.C), Tag: "tp_half", Meta: map[string]any{"reason": "take_profit_half"}}
			sigs = append(sigs, s)
			_ = getLogger().LogSignal(s)
			st.position += size
			if st.position > 0 {
				st.position = 0
			}
			st.cooldown = maxi(1, qm.params.CooldownBars/2)
			st.reentryArm = true
		}
	}

	// 滞回：弱退（Exit）
	if math.Abs(targetPos) < qm.params.ExitThreshold && math.Abs(cur) >= qm.params.ExitThreshold {
		pnl := qm.estimatePnL(st, c.C)
		s := Signal{InstID: c.InstID, Side: "close", Size: qm.roundSize(math.Abs(cur)), Price: qm.roundPrice(c.C), Tag: qm.name, Meta: map[string]any{"reason": "exit_threshold", "pnl": pnl}}
		sigs = append(sigs, s)
		_ = getLogger().LogSignal(s)

		st.perfTracker.totalTrades++
		if pnl > 0 {
			st.perfTracker.winTrades++
		}
		st.position = 0
		st.lastSignalPos = 0
		st.entryPrice = 0
		st.maxFavorable = 0
		st.cooldown = qm.params.CooldownBars
		st.reentryArm = true
		return sigs
	}

	// —— 翻向惩罚：从多翻空/空翻多更难触发 —— //
	adjEntry := qm.params.EntryThreshold
	if cur*targetPos < 0 { // 翻向
		adjEntry += 0.06
	}

	// —— 再入场优化：趋势未破坏 + 回撤后再突破/跌破 —— //
	allowReentry := false
	if st.reentryArm && len(st.trendEMAs) >= 2 {
		fast := st.trendEMAs[0].val()
		slow := st.trendEMAs[len(st.trendEMAs)-1].val()
		if fast > slow && c.C > st.highs.max(5) { // 多头趋势再突破
			allowReentry = true
		}
		if fast < slow && c.C < st.lows.min(5) { // 空头趋势再跌破
			allowReentry = true
		}
	}
	minDelta := qm.params.MinPositionDelta
	if allowReentry {
		adjEntry = math.Max(0.10, adjEntry-0.12) // 降低入场门槛
		minDelta = math.Max(0.08, minDelta-0.07) // 降低最小Δ
	}

	// 小Δ忽略
	if math.Abs(delta) < minDelta {
		return nil
	}

	// === 成本一致化判定 ===
	// 期望边际收益：用下一 bar 的“典型振幅”近似，取当前 sigma（std of returns）
	sigma := st.retStd.std()
	if sigma <= 0 {
		sigma = 1e-4 // 极小噪声
	}

	// 先判断是否超过触发阈值（仓位维度），再比较边际收益与成本（收益维度）
	if math.Abs(delta) >= adjEntry {
		// 成本：fee + spread + slip + 价格冲击（线性+二次）
		cost := qm.estimateTradeCost(st, math.Abs(delta), c.C)

		execDelta := delta
		side := "buy"
		if execDelta < 0 {
			side = "sell"
		}

		edge := math.Abs(execDelta) * sigma
		if edge <= cost*qm.params.EdgeCostCoef {
			return nil
		}

		// 交易所约束：对 size 和 price 做 rounding
		px := qm.roundPrice(c.C)
		size := qm.roundSize(math.Abs(execDelta))
		if size <= 0 {
			return nil
		}

		s := Signal{InstID: c.InstID, Side: side, Size: size, Price: px, Tag: qm.name, Meta: map[string]any{
			"event":   "rebalance",
			"target":  targetPos,
			"delta":   execDelta,
			"cost":    cost,
			"edge":    edge,
			"sigma":   sigma,
			"sharpe":  st.perfTracker.sharpe(),
			"reentry": allowReentry,
		}}
		sigs = append(sigs, s)
		_ = getLogger().LogSignal(s)

		// === 用“实际执行的 Δ”更新仓位与加权均价 ===
		newPos := cur
		if side == "buy" {
			newPos = cur + size
		} else {
			newPos = cur - size
		}

		if newPos == 0 {
			st.entryPrice = 0
			st.maxFavorable = 0
		} else {
			// 加权均价
			turn := size
			if side == "sell" {
				turn = -size
			}
			st.entryPrice = weightedEntryPrice(cur, st.entryPrice, turn, px, newPos)
			// 更新最有利价
			if newPos > 0 {
				if st.maxFavorable == 0 || px > st.maxFavorable {
					st.maxFavorable = px
				}
			} else if newPos < 0 {
				if st.maxFavorable == 0 || px < st.maxFavorable {
					st.maxFavorable = px
				}
			}
		}

		st.position = newPos
		st.lastSignalPos = newPos
		if st.entryBar == 0 {
			st.entryBar = st.totalBars
		}
		st.cooldown = qm.params.CooldownBars
		st.reentryArm = false // 已再入则解除
		return sigs
	}

	return nil
}

// ===============================================================================
// 成本 / PnL / Rounding
// ===============================================================================

func (qm *QuantMasterElite) estimateTradeCost(st *eliteState, size float64, price float64) float64 {
	if price <= 0 {
		return 0
	}
	fee := (qm.params.TakerFeeBps / 10000.0)
	spread := 0.0
	if st.lastAsk > st.lastBid && st.lastBid > 0 {
		spread = (st.lastAsk - st.lastBid) / price
	}
	slip := (qm.params.SlippageBps / 10000.0)
	impactLin := qm.params.ImpactCoef * size * (st.atr.val() / price)
	impactQuad := qm.params.ImpactCoefQuad * size * size // UPDATED: 非线性冲击
	return fee + spread + slip + impactLin + impactQuad
}

func (qm *QuantMasterElite) estimatePnL(st *eliteState, px float64) float64 {
	// UPDATED: 统一口径：合约张数 * 合约乘数 * 价格差
	if st.entryPrice == 0 || st.position == 0 {
		return 0
	}
	return st.position * qm.params.ContractMultiplier * (px - st.entryPrice)
}

func (qm *QuantMasterElite) roundSize(size float64) float64 {
	if qm.params.LotSize <= 0 {
		return size
	}
	steps := math.Round(size / qm.params.LotSize)
	return steps * qm.params.LotSize
}

func (qm *QuantMasterElite) roundPrice(price float64) float64 {
	step := qm.params.TickSize
	if step <= 0 {
		step = qm.params.PriceStep
	}
	if step <= 0 {
		return price
	}
	steps := math.Round(price / step)
	return steps * step
}

func weightedEntryPrice(curPos, curAvg, turnDelta, turnPrice, newPos float64) float64 {
	// turnDelta：本次变化的仓位（买为 +size，卖为 -size）
	if newPos == 0 {
		return 0
	}
	return (curAvg*curPos + turnPrice*turnDelta) / newPos
}

// ===============================================================================
// 因子健康
// ===============================================================================

func (qm *QuantMasterElite) monitorFactorHealth(st *eliteState, factors []float64) {
	for i := range factors {
		if i >= len(st.factorIC) {
			continue
		}
		// 使用全局 IC 的健康度做衰减（保留简单性）
		ic := st.factorIC[i].getIC()
		samples := st.factorIC[i].samples
		if samples > qm.params.ICMinSamples*2 {
			if ic < -0.05 && st.factorIC[i].tstat() < -2.0 {
				st.factorDecay[i] *= 0.90
				if st.factorDecay[i] < 0.1 {
					st.factorDecay[i] = 0.1
				}
			}
			if ic > 0.02 {
				st.factorDecay[i] += 0.05
				if st.factorDecay[i] > 1.0 {
					st.factorDecay[i] = 1.0
				}
			}
		}
	}
}

// ===============================================================================
// 状态管理
// ===============================================================================

func (qm *QuantMasterElite) getOrCreateState(inst string) *eliteState {
	qm.statesMu.RLock()
	st, ok := qm.states[inst]
	qm.statesMu.RUnlock()
	if ok {
		return st
	}

	qm.statesMu.Lock()
	defer qm.statesMu.Unlock()
	if st, ok = qm.states[inst]; ok {
		return st
	}

	maxWindow := maxi(qm.params.VolWindow, qm.params.RegimeWindow)
	for _, w := range qm.params.TrendWindows {
		maxWindow = maxi(maxWindow, w)
	}
	for _, w := range qm.params.MRWindows {
		maxWindow = maxi(maxWindow, w)
	}

	buf := maxi(1024, maxWindow*4)
	st = &eliteState{
		closes: newRing(buf), highs: newRing(buf), lows: newRing(buf), volumes: newRing(buf),
		atr:     newATR(14),
		retStd:  newRollingStd(qm.params.VolWindow),
		longVol: newEWVar(halfLifeToAlpha(qm.params.VolWindow * 5)),

		rsi:       newRSI(14),
		trendEMAs: make([]*ema, len(qm.params.TrendWindows)),
		mrBands:   make([]*bollingerBand, len(qm.params.MRWindows)),

		microPressure: newEMA(maxi(2, qm.params.MicroHalfLife)),

		regimeState: newRegimeDetector(qm.params.RegimeWindow),
		metaLearner: newMetaOptimizer(qm.params.MetaLearnRate),

		riskManager: newAdvancedRisk(qm.params),
		perfTracker: newPerformanceTracker(qm.params.PerformanceWindow, qm.params.TimeframeMinutes),

		factorIC:    make([]intelligentIC, FNum),
		factorDecay: make([]float64, FNum),
		factorCovs:  make([]*ewCorr, FNum),
		factorVars:  make([]*ewVar, FNum),
		trendVar:    newEWVar(halfLifeToAlpha(qm.params.ICHalfLife)),

		prevFactors: make([]float64, FNum),
		lastReturn:  math.NaN(),
	}

	for i, w := range qm.params.TrendWindows {
		st.trendEMAs[i] = newEMA(w)
	}
	for i, w := range qm.params.MRWindows {
		st.mrBands[i] = newBollingerBand(w, 2.0)
	}

	alpha := halfLifeToAlpha(qm.params.ICHalfLife)
	for i := 0; i < FNum; i++ {
		st.factorIC[i] = newIntelligentIC(alpha)
		st.factorDecay[i] = 1.0
		c := newEWCorr(alpha)
		st.factorCovs[i] = &c
		st.factorVars[i] = newEWVar(alpha)
	}

	// UPDATED: 初始化“条件IC”容器
	for r := 0; r < RegNum; r++ {
		st.factorICRegime[r] = make([]intelligentIC, FNum)
		for i := 0; i < FNum; i++ {
			st.factorICRegime[r][i] = newIntelligentIC(alpha)
		}
	}

	qm.states[inst] = st
	return st
}

func (qm *QuantMasterElite) isReady(st *eliteState) bool {
	if st.atr == nil || st.retStd == nil || st.rsi == nil {
		return false
	}
	if st.atr.count < 14 || st.retStd.count < qm.params.VolWindow || !st.rsi.ready() {
		return false
	}
	for _, e := range st.trendEMAs {
		if e.count < 2 {
			return false
		}
	}
	for _, bb := range st.mrBands {
		if !bb.ready() {
			return false
		}
	}
	return true
}

// ===============================================================================
// 外部接口
// ===============================================================================

func (qm *QuantMasterElite) UpdateFunding(inst string, annualRate float64) {
	st := qm.getOrCreateState(inst)
	st.mu.Lock()
	defer st.mu.Unlock()
	if st.carryEMA == nil {
		st.carryEMA = newEMA(maxi(2, qm.params.CarryHalfLife))
		st.carryVar = newEWVar(halfLifeToAlpha(qm.params.CarryHalfLife))
	}
	st.carryEMA.push(annualRate)
	st.carryVar.push(annualRate)
}

func (qm *QuantMasterElite) UpdateBasis(inst string, basis float64) {
	st := qm.getOrCreateState(inst)
	st.mu.Lock()
	defer st.mu.Unlock()
	if st.basisEMA == nil {
		st.basisEMA = newEMA(maxi(2, qm.params.BasisHalfLife))
		st.basisVar = newEWVar(halfLifeToAlpha(qm.params.BasisHalfLife))
	}
	st.basisEMA.push(basis)
	st.basisVar.push(basis)
}

func (qm *QuantMasterElite) GetPerformance(inst string) map[string]float64 {
	st, ok := qm.states[inst]
	if !ok {
		return nil
	}
	st.mu.Lock()
	defer st.mu.Unlock()
	return map[string]float64{
		"sharpe":       st.perfTracker.sharpe(),
		"max_dd":       st.perfTracker.maxDrawdown(),
		"win_rate":     st.perfTracker.winRate(),
		"total_trades": float64(st.perfTracker.totalTrades),
	}
}

func (qm *QuantMasterElite) GetGlobalPerformance() map[string]float64 {
	return map[string]float64{
		"sharpe":   qm.globalPerf.sharpe(),
		"max_dd":   qm.globalPerf.maxDrawdown(),
		"win_rate": 0, // 全局胜率在组合层统计
	}
}

// ===============================================================================
// 高级组件
// ===============================================================================

// 智能 IC 追踪器

type intelligentIC struct {
	corr    ewCorr
	samples int
}

func newIntelligentIC(alpha float64) intelligentIC   { return intelligentIC{corr: newEWCorr(alpha)} }
func (ic *intelligentIC) update(factor, ret float64) { ic.corr.push(factor, ret); ic.samples++ }
func (ic *intelligentIC) getIC() float64             { return ic.corr.corr() }
func (ic *intelligentIC) tstat() float64 {
	if ic.samples < 30 {
		return 0
	}
	c := ic.getIC()
	n := float64(ic.samples)
	den := math.Sqrt(1 - c*c + 1e-9)
	return c * math.Sqrt(n-2) / den
}

// 市场制度检测器（使用趋势强度）

type regimeDetector struct {
	window             int
	volHist, trendHist *ringBuf
	currentRegime      string
}

func newRegimeDetector(window int) *regimeDetector {
	return &regimeDetector{window: window, volHist: newRing(window), trendHist: newRing(window), currentRegime: "ranging"}
}
func (rd *regimeDetector) update(price, atr, trendStrength float64) {
	if price <= 0 {
		return
	}
	rd.volHist.push(atr / price)
	rd.trendHist.push(trendStrength)
	if rd.volHist.count < rd.window {
		return
	}
	avgVol := rd.volHist.mean(rd.window)
	avgTrend := rd.trendHist.mean(rd.window)
	// 更宽松的 trending 判定；高波动更严格
	if avgVol > 0.015 { // 1.5%/bar 视为高波动
		rd.currentRegime = "volatile"
	} else if avgTrend > 0.0006 { // 更低门槛
		rd.currentRegime = "trending"
	} else {
		rd.currentRegime = "ranging"
	}
}
func (rd *regimeDetector) adjustWeights(w, factors []float64) []float64 {
	// UPDATED: 制度分因子缩放（volatile 保留趋势/突破/动量权重）
	out := make([]float64, len(w))
	copy(out, w)
	switch rd.currentRegime {
	case "trending":
		if len(out) > FTrend {
			out[FTrend] *= 1.25
		}
		if len(out) > FMR {
			out[FMR] *= 0.65
		}
		if len(out) > FMomentum {
			out[FMomentum] *= 1.15
		}
	case "ranging":
		if len(out) > FTrend {
			out[FTrend] *= 0.75
		}
		if len(out) > FMR {
			out[FMR] *= 1.35
		}
		if len(out) > FBreakout {
			out[FBreakout] *= 0.85
		}
	case "volatile":
		for i := range out {
			out[i] *= 0.8
		}
		if len(out) > FTrend {
			out[FTrend] *= 1.15
		}
		if len(out) > FBreakout {
			out[FBreakout] *= 1.15
		}
		if len(out) > FMicro {
			out[FMicro] *= 0.6
		}
		if len(out) > FMR {
			out[FMR] *= 0.6
		}
	}
	sum := 0.0
	for _, x := range out {
		sum += x
	}
	if sum > 0 {
		for i := range out {
			out[i] /= sum
		}
	}
	return out
}
func (rd *regimeDetector) mode() int {
	switch rd.currentRegime {
	case "trending":
		return RegTrending
	case "volatile":
		return RegVolatile
	default:
		return RegRanging
	}
}

// 元学习优化器

type metaOptimizer struct {
	learningRate float64
	momentum     []float64
	bestSharpe   float64
	once         sync.Once
}

func newMetaOptimizer(lr float64) *metaOptimizer {
	return &metaOptimizer{learningRate: lr, momentum: make([]float64, FNum), bestSharpe: -999}
}
func (mo *metaOptimizer) optimize(w []float64, sharpe float64) []float64 {
	mo.once.Do(func() { randSeedOnce() })
	if sharpe <= mo.bestSharpe {
		out := make([]float64, len(w))
		for i := range w {
			out[i] = w[i] * (1 + 0.05*randNormal())
		}
		return out
	}
	mo.bestSharpe = sharpe
	return w
}

// 风控

type advancedRisk struct {
	params     EliteParams
	peakEquity float64
	currentDD  float64
}

func newAdvancedRisk(p EliteParams) *advancedRisk { return &advancedRisk{params: p, peakEquity: 1.0} }
func (ar *advancedRisk) applyRiskLimits(pos, currentPrice, entryPrice float64) float64 {
	if ar.currentDD >= ar.params.MaxDrawdownPct {
		return pos * 0.5 // 紧急降仓
	}
	maxPos := ar.params.MaxAbsPosition * ar.params.MaxLeverage
	return clamp(pos, -maxPos, maxPos)
}
func (ar *advancedRisk) checkAdaptiveStop(inst string, pos, price, entry, maxFav, atr float64) *Signal {
	if pos == 0 || entry == 0 || atr <= 0 {
		return nil
	}
	dist := ar.params.StopLossATR * atr
	if pos > 0 && price <= entry-dist {
		return &Signal{InstID: inst, Side: "close", Size: math.Abs(pos), Price: price, Tag: "risk", Meta: map[string]any{"reason": "stop_loss"}}
	}
	if pos < 0 && price >= entry+dist {
		return &Signal{InstID: inst, Side: "close", Size: math.Abs(pos), Price: price, Tag: "risk", Meta: map[string]any{"reason": "stop_loss"}}
	}
	// 跟踪止损
	trail := ar.params.TrailingStopATR * atr
	if pos > 0 && maxFav > 0 && price <= maxFav-trail {
		return &Signal{InstID: inst, Side: "close", Size: math.Abs(pos), Price: price, Tag: "risk", Meta: map[string]any{"reason": "trailing_stop"}}
	}
	if pos < 0 && maxFav > 0 && price >= maxFav+trail {
		return &Signal{InstID: inst, Side: "close", Size: math.Abs(pos), Price: price, Tag: "risk", Meta: map[string]any{"reason": "trailing_stop"}}
	}
	return nil
}

// ===============================================================================
// 性能追踪器（策略收益 + log-equity 回撤）
// ===============================================================================

type performanceTracker struct {
	window       int
	barMinutes   int
	stratReturns *ringBuf // 策略收益序列（pos_{t-1} * ret_t）
	equityLog    float64  // log-equity
	peakLog      float64
	maxDD        float64
	lastPos      float64

	// 统计
	totalTrades, winTrades int
}

func newPerformanceTracker(window int, barMinutes int) *performanceTracker {
	if barMinutes <= 0 {
		barMinutes = 5
	}
	return &performanceTracker{
		window:       window,
		barMinutes:   barMinutes,
		stratReturns: newRing(window),
		equityLog:    0,
		peakLog:      0,
	}
}

// update 返回本期策略收益，供上层聚合
func (pt *performanceTracker) update(pos float64, ret float64) float64 {
	// 本期策略收益用上期仓位
	rStrat := ret * pt.lastPos
	pt.stratReturns.push(rStrat)

	// log-equity 累加与回撤
	pt.equityLog += rStrat
	if pt.equityLog > pt.peakLog {
		pt.peakLog = pt.equityLog
	}
	dd := 1 - math.Exp(pt.equityLog-pt.peakLog)
	if dd > pt.maxDD {
		pt.maxDD = dd
	}

	// 滚动 lastPos：用“已执行仓位”
	pt.lastPos = pos
	return rStrat
}

func (pt *performanceTracker) updateStrat(rStrat float64) {
	pt.stratReturns.push(rStrat)
	pt.equityLog += rStrat
	if pt.equityLog > pt.peakLog {
		pt.peakLog = pt.equityLog
	}
	dd := 1 - math.Exp(pt.equityLog-pt.peakLog)
	if dd > pt.maxDD {
		pt.maxDD = dd
	}
}

func (pt *performanceTracker) sharpe() float64 {
	if pt.stratReturns.count < 30 {
		return 0
	}
	mean := pt.stratReturns.mean(pt.stratReturns.count)
	std := pt.stratReturns.std(pt.stratReturns.count)
	if std == 0 {
		return 0
	}
	ann := math.Sqrt((365 * 24 * 60) / float64(maxi(1, pt.barMinutes)))
	return (mean / std) * ann
}
func (pt *performanceTracker) maxDrawdown() float64 { return pt.maxDD }
func (pt *performanceTracker) winRate() float64 {
	if pt.totalTrades == 0 {
		return 0
	}
	return float64(pt.winTrades) / float64(pt.totalTrades)
}

// ===============================================================================
// 指标组件
// ===============================================================================

type bollingerBand struct {
	ma    *rollingMean
	std   *rollingStd
	width float64
}

func newBollingerBand(window int, width float64) *bollingerBand {
	return &bollingerBand{ma: newRollingMean(window), std: newRollingStd(window), width: width}
}
func (bb *bollingerBand) push(v float64) { bb.ma.push(v); bb.std.push(v) }
func (bb *bollingerBand) ready() bool {
	return bb.ma.count >= bb.ma.window && bb.std.count >= bb.std.window
}
func (bb *bollingerBand) zscore(price float64) float64 {
	m := bb.ma.val()
	s := bb.std.std()
	if s == 0 {
		return 0
	}
	return (price - m) / s
}

// ===============================================================================
// 工具函数
// ===============================================================================

func softplus(x float64) float64 {
	if x > 20 {
		return x
	}
	return math.Log1p(math.Exp(x))
}
func tanhClamp(x, limit float64) float64 { return math.Tanh(x/limit) * limit }
func clamp(x, min, max float64) float64 {
	if x < min {
		return min
	}
	if x > max {
		return max
	}
	return x
}
func safeLog(x float64) float64 {
	if x <= 0 {
		return 0
	}
	return math.Log(x)
}
func maxi(a, b int) int {
	if a > b {
		return a
	}
	return b
}
func halfLifeToAlpha(hl int) float64 {
	if hl <= 1 {
		return 0.5
	}
	return 1 - math.Exp(-math.Log(2)/float64(hl))
}

var seedOnce sync.Once

func randSeedOnce()       { seedOnce.Do(func() { rand.Seed(time.Now().UnixNano()) }) }
func randNormal() float64 { return rand.NormFloat64() }

// ===============================================================================
// 基础数据结构（O(1) 统计）
// ===============================================================================

type ringBuf struct {
	data            []float64
	cap, count, idx int
	sum, sumSq      float64
}

func newRing(capacity int) *ringBuf { return &ringBuf{data: make([]float64, capacity), cap: capacity} }
func (r *ringBuf) push(val float64) {
	oldVal := 0.0
	if r.count >= r.cap {
		oldVal = r.data[r.idx]
		r.sum -= oldVal
		r.sumSq -= oldVal * oldVal
	}
	r.data[r.idx] = val
	r.sum += val
	r.sumSq += val * val
	if r.count < r.cap {
		r.count++
	}
	r.idx = (r.idx + 1) % r.cap
}
func (r *ringBuf) getN(n int) float64 {
	if n >= r.count {
		return math.NaN()
	}
	if r.count < r.cap {
		return r.data[r.count-1-n]
	}
	pos := (r.idx - 1 - n + r.cap) % r.cap
	return r.data[pos]
}
func (r *ringBuf) mean(n int) float64 {
	if n > r.count {
		n = r.count
	}
	if n == 0 {
		return 0
	}
	if n == r.count {
		return r.sum / float64(r.count)
	}
	// 部分窗口的回退计算
	s := 0.0
	for i := 0; i < n; i++ {
		s += r.getN(i)
	}
	return s / float64(n)
}
func (r *ringBuf) std(n int) float64 {
	if n > r.count {
		n = r.count
	}
	if n <= 1 {
		return 0
	}
	// 统一样本方差口径
	m := r.mean(n)
	s := 0.0
	for i := 0; i < n; i++ {
		d := r.getN(i) - m
		s += d * d
	}
	return math.Sqrt(s / float64(n-1))
}
func (r *ringBuf) max(n int) float64 {
	if n > r.count {
		n = r.count
	}
	if n == 0 {
		return math.NaN()
	}
	mx := r.getN(0)
	for i := 1; i < n; i++ {
		v := r.getN(i)
		if v > mx {
			mx = v
		}
	}
	return mx
}
func (r *ringBuf) min(n int) float64 {
	if n > r.count {
		n = r.count
	}
	if n == 0 {
		return math.NaN()
	}
	mn := r.getN(0)
	for i := 1; i < n; i++ {
		v := r.getN(i)
		if v < mn {
			mn = v
		}
	}
	return mn
}

// EMA

type ema struct {
	window       int
	alpha, value float64
	count        int
}

func newEMA(window int) *ema {
	if window < 2 {
		window = 2
	}
	return &ema{window: window, alpha: 2.0 / float64(window+1)}
}
func (e *ema) push(val float64) {
	if e.count == 0 {
		e.value = val
	} else {
		e.value = e.alpha*val + (1-e.alpha)*e.value
	}
	e.count++
}
func (e *ema) val() float64 { return e.value }

// ATR

type atrCalc struct {
	window    int
	atrEMA    *ema
	prevClose float64
	count     int
}

func newATR(window int) *atrCalc {
	if window < 2 {
		window = 14
	}
	return &atrCalc{window: window, atrEMA: newEMA(window)}
}
func (a *atrCalc) push(high, low, close float64) {
	tr := high - low
	if a.prevClose > 0 {
		tr = math.Max(tr, math.Abs(high-a.prevClose))
		tr = math.Max(tr, math.Abs(low-a.prevClose))
	}
	a.atrEMA.push(tr)
	a.prevClose = close
	a.count++
}
func (a *atrCalc) val() float64 { return a.atrEMA.val() }

// Rolling Mean / Std（容量=window，O(1) 生效）

type rollingMean struct {
	window int
	buffer *ringBuf
	count  int
}

func newRollingMean(window int) *rollingMean {
	return &rollingMean{window: window, buffer: newRing(window)}
}
func (rm *rollingMean) push(val float64) { rm.buffer.push(val); rm.count++ }
func (rm *rollingMean) val() float64 {
	n := rm.window
	if n > rm.buffer.count {
		n = rm.buffer.count
	}
	return rm.buffer.mean(n)
}

type rollingStd struct {
	window int
	buffer *ringBuf
	count  int
}

func newRollingStd(window int) *rollingStd {
	return &rollingStd{window: window, buffer: newRing(window)}
}
func (rs *rollingStd) push(val float64) { rs.buffer.push(val); rs.count++ }
func (rs *rollingStd) std() float64 {
	n := rs.window
	if n > rs.buffer.count {
		n = rs.buffer.count
	}
	return rs.buffer.std(n)
}

// RSI

type rsiCalc struct {
	window           int
	gainEMA, lossEMA *ema
	prevClose        float64
	count            int
}

func newRSI(window int) *rsiCalc {
	if window < 2 {
		window = 14
	}
	return &rsiCalc{window: window, gainEMA: newEMA(window), lossEMA: newEMA(window)}
}
func (r *rsiCalc) push(close float64) {
	if r.prevClose > 0 {
		d := close - r.prevClose
		if d > 0 {
			r.gainEMA.push(d)
			r.lossEMA.push(0)
		} else {
			r.gainEMA.push(0)
			r.lossEMA.push(-d)
		}
		r.count++
	}
	r.prevClose = close
}
func (r *rsiCalc) ready() bool { return r.count >= r.window }
func (r *rsiCalc) val() float64 {
	g := r.gainEMA.val()
	l := r.lossEMA.val()
	if l == 0 {
		return 100
	}
	rs := g / l
	return 100 - (100 / (1 + rs))
}

// EW Correlation

type ewCorr struct {
	alpha           float64
	meanX, meanY    float64
	varX, varY, cov float64
	inited          bool
}

func newEWCorr(alpha float64) ewCorr {
	if alpha <= 0 || alpha >= 1 {
		alpha = 0.94
	}
	return ewCorr{alpha: alpha}
}
func (e *ewCorr) push(x, y float64) {
	if !e.inited {
		e.meanX = x
		e.meanY = y
		e.inited = true
		return
	}
	ax := e.alpha
	one := 1 - ax
	dx := x - e.meanX
	dy := y - e.meanY
	e.meanX += ax * dx
	e.meanY += ax * dy
	e.varX = one*e.varX + ax*dx*dx
	e.varY = one*e.varY + ax*dy*dy
	e.cov = one*e.cov + ax*dx*dy
}
func (e *ewCorr) corr() float64 {
	if e.varX <= 0 || e.varY <= 0 {
		return 0
	}
	v := e.cov / (math.Sqrt(e.varX*e.varY) + 1e-12)
	if math.IsNaN(v) || math.IsInf(v, 0) {
		return 0
	}
	return v
}

// EW Variance

type ewVar struct {
	alpha   float64
	mean, v float64
	inited  bool
}

func newEWVar(alpha float64) *ewVar {
	if alpha <= 0 || alpha >= 1 {
		alpha = 0.94
	}
	return &ewVar{alpha: alpha}
}
func (e *ewVar) push(x float64) {
	if !e.inited {
		e.mean = x
		e.inited = true
		return
	}
	dx := x - e.mean
	e.mean += e.alpha * dx
	e.v = (1-e.alpha)*e.v + e.alpha*dx*dx
}
func (e *ewVar) std() float64 {
	if e.v < 0 {
		return 0
	}
	return math.Sqrt(e.v)
}
func (e *ewVar) meanVal() float64 { return e.mean }

// ===============================================================================
// 接口定义
// ===============================================================================

type Candle struct {
	InstID     string
	T          int64
	O, H, L, C float64
	V          float64 // 合约成交量（张数）；若是币/张混合，请在外部统一
}

type Ticker struct {
	InstID           string
	Bid, Ask         float64
	BidSize, AskSize float64
	Last             float64
}

type Signal struct {
	InstID string
	Side   string
	Size   float64 // 合约张数（下单单位）
	Price  float64
	Tag    string
	Meta   map[string]any
}

type Strategy interface {
	Name() string
	OnCandle(Candle) []Signal
	OnTicker(Ticker) []Signal
}

// 接口断言（编译期检查）
var _ Strategy = (*QuantMasterElite)(nil)

// ===== 需要你在工程里提供的日志器（保持与原实现一致）=====
type logger interface {
	LogSignal(Signal) error
}
