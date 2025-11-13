package portfolio

// Portfolio —— 组合层 / 仓位管理与资金分配（中文注释，零依赖，可直接嵌入）
// ==================================================================================
// 改动要点（与原版接口兼容）：
// 1) 新增 BeginBar(ts)/EndBar()：用“时间戳去重”的方式推进 bar 节拍，避免多品种重复+1；
// 2) SetStrategyTargets 语义调整为“覆盖整张策略目标表”（而非增量），防止旧目标残留；
// 3) Propose 结束后，冻结 lastExpo 用于下一根 StrategyLearn 的 paper PnL 统计；
// 4) vol/corr 年化与容错更稳：NaN/Inf/极小值护栏；
// 5) StrategyLearn 对小样本回落先验；
// 6) 其它细节：NaN 夹断、极小值地板、换手与节拍绑定等；
//
// 分层关系（建议）：
// Strategy → Portfolio（本文件）→ Risk（风控裁决）→ Execution（执行撮合）

import (
	"math"
	"sort"
)

// ===================== 对外轻量类型（避免循环依赖） =====================

type Candle struct {
	InstID     string
	T          int64
	O, H, L, C float64
}

// ===================== 配置 =====================

type Config struct {
	// 目标风险与约束
	TargetVolAnnual float64 // 组合目标年化波动，例如 0.20
	MaxLeverage     float64 // 杠杆上限（≈总绝对权重 Gross 上限）
	MaxGross        float64 // Gross（∑|w_i|）上限；若 0 则等于 MaxLeverage
	CashBufferPct   float64 // 预留“现金缓冲”（例如 0.05），降低满仓风险

	// 再平衡与交易成本近似控制
	RebalanceIntervalBars int     // 每隔多少“节拍”才考虑再平衡（按 BeginBar 的 ts 去重推进）
	DriftThreshold        float64 // 单品种目标偏离小于该阈值则不调（降低噪音）
	TurnoverCap           float64 // 每次再平衡允许的最大换手（∑|Δw|）

	// 因子/风险模型
	UseRiskParity   bool    // 使用风险平价（w_i ∝ |signal_i| / σ_i）
	UseMinVarApprox bool    // 使用极简“近似最小方差”（对角 + 平均相关）
	EWHalfLifeVol   int     // 波动率半衰期（bar）
	EWHalfLifeCorr  int     // 相关性半衰期（bar）
	VolFloor        float64 // 波动率地板（避免除零）
	BarMinutes      int     // K 线分钟数（年化用）

	// 策略聚合
	StrategyWeights map[string]float64 // 初始策略权重（缺省则均匀）
	StrategyLearn   bool               // 是否启用“按滚动 Sharpe”自适应策略权重

	// 约束
	PerInstrumentMax map[string]float64 // 单品种绝对权重上限（相对仓位）
}

func (c *Config) withDefaults() Config {
	q := *c
	if q.TargetVolAnnual == 0 {
		q.TargetVolAnnual = 0.20
	}
	if q.MaxLeverage == 0 {
		q.MaxLeverage = 3.0
	}
	if q.MaxGross == 0 {
		q.MaxGross = q.MaxLeverage
	}
	if q.CashBufferPct == 0 {
		q.CashBufferPct = 0.02
	}
	if q.DriftThreshold == 0 {
		q.DriftThreshold = 0.05
	}
	if q.TurnoverCap == 0 {
		q.TurnoverCap = 0.8
	}
	if q.EWHalfLifeVol == 0 {
		q.EWHalfLifeVol = 96
	}
	if q.EWHalfLifeCorr == 0 {
		q.EWHalfLifeCorr = 256
	}
	if q.VolFloor == 0 {
		q.VolFloor = 1e-4
	}
	if q.BarMinutes == 0 {
		q.BarMinutes = 5
	}
	if q.PerInstrumentMax == nil {
		q.PerInstrumentMax = map[string]float64{}
	}
	if q.StrategyWeights == nil {
		q.StrategyWeights = map[string]float64{}
	}
	return q
}

// ===================== 引擎 =====================

type Engine struct {
	cfg Config

	// —— 市场统计 —— //
	inst map[string]*instState
	corr map[[2]string]*ewCorr // 成对相关的 EW 估计

	// —— 策略输入缓存（仅当前 bar 生效）—— //
	// strategy -> (inst -> target in [-1,1])
	stratTargets map[string]map[string]float64

	// —— 输出持久化（用于门限/换手/学习）—— //
	lastTargets map[string]float64            // 上次输出（组合目标）
	lastExpo    map[string]map[string]float64 // 上一根策略曝光，用于下根学习

	// —— 策略绩效（学习权重）—— //
	stratPerf map[string]*sharpeTracker

	// —— 节拍控制 —— //
	lastBarTs int64 // 最近一次 BeginBar 的 ts
	barCount  int   // 仅在 BeginBar(ts) 且 ts 变化时 +1
}

func NewEngine(cfg Config) *Engine {
	c := cfg.withDefaults()
	return &Engine{
		cfg:          c,
		inst:         make(map[string]*instState),
		corr:         make(map[[2]string]*ewCorr),
		stratTargets: make(map[string]map[string]float64),
		lastTargets:  make(map[string]float64),
		stratPerf:    make(map[string]*sharpeTracker),
		lastExpo:     make(map[string]map[string]float64),
	}
}

// BeginBar —— 宣告新的一根开始（按时间戳去重推进节拍）
// 调用约定：在该“统一时间戳 ts”的所有 OnCandle 之前调用一次
func (e *Engine) BeginBar(ts int64) {
	if ts <= 0 {
		return
	}
	if e.lastBarTs != ts {
		e.lastBarTs = ts
		e.barCount++
	}
	// 清空本根的策略目标缓存（改为“覆盖式提交”）
	e.stratTargets = make(map[string]map[string]float64)
}

// EndBar —— 本根结束时的钩子（可留空，便于未来扩展）
func (e *Engine) EndBar() {}

// OnCandle —— 推入各品种收盘价，更新波动/相关/策略学习。
// 注意：请在同一个 ts 的所有 OnCandle 前后分别调用 BeginBar(ts)/EndBar()。
func (e *Engine) OnCandle(k Candle) {
	st := e.ensure(k.InstID)
	st.pushClose(k.C)

	// 更新相关矩阵（O(N^2)）
	for id, other := range e.inst {
		if id == k.InstID {
			continue
		}
		pair := key2(k.InstID, id)
		c := e.corr[pair]
		if c == nil {
			c = newEWCorr(alphaFromHL(e.cfg.EWHalfLifeCorr))
			e.corr[pair] = c
		}
		c.push(st.lastRet, other.lastRet)
	}

	// 依据上一根的策略曝光 lastExpo 计算 paper PnL（用于 StrategyLearn）
	if e.cfg.StrategyLearn && len(e.lastExpo) > 0 {
		for s, expo := range e.lastExpo {
			ret := 0.0
			for inst, w := range expo {
				if is, ok := e.inst[inst]; ok {
					ret += w * is.lastRet
				}
			}
			trk := e.ensureSharpe(s)
			trk.push(ret)
		}
	}
}

// SetStrategyTargets —— “覆盖式”提交某策略在本根的全量目标（-1..+1）。
// 约定：每根最多调用一次，若策略只覆盖了部分品种，其它品种默认目标 0。
func (e *Engine) SetStrategyTargets(strategy string, targets map[string]float64) {
	m := make(map[string]float64, len(targets))
	for inst, v := range targets {
		m[inst] = clamp(v, -1, 1)
	}
	e.stratTargets[strategy] = m
}

// Meta —— 输出组合诊断
type Meta struct {
	StrategyWeights map[string]float64
	PortfolioVolAnn float64
	Gross           float64
	Turnover        float64
	Scaler          float64
}

// Propose —— 聚合多策略目标，应用风险预算与约束，返回最终目标（-1..+1）
// 约定：在本根所有策略都完成 SetStrategyTargets 之后调用一次
func (e *Engine) Propose(mark map[string]float64) (map[string]float64, Meta) {
	if len(e.stratTargets) == 0 {
		return map[string]float64{}, Meta{}
	}

	// 1) 策略层权重（学习 or 固定）
	sw := e.strategyWeights()

	// 2) 聚合至单品种目标
	agg := make(map[string]float64)
	for s, m := range e.stratTargets {
		ws := sw[s]
		for inst, v := range m {
			agg[inst] += ws * v
		}
	}
	for inst, v := range agg {
		agg[inst] = clamp(safe(v), -1, 1)
	}
	if len(agg) == 0 {
		return map[string]float64{}, Meta{}
	}

	// 3) 风险预算（幅度）+ 信号方向（符号）
	w := e.allocateRisk(agg)
	if len(w) == 0 {
		return map[string]float64{}, Meta{}
	}

	// 4) 年化目标波动缩放
	scaler, sigmaAnn := e.volTargetScaler(w)
	for i := range w {
		w[i] = safe(w[i] * scaler)
	}

	// 5) 约束：单品种上限 / Gross / 杠杆 / 现金缓冲
	w = e.applyLimits(w)

	// 6) 再平衡门限 + 换手（仅在到达节拍时才执行）
	final, turnover := e.rebalanceGate(w)

	// 7) 冻结上一根的策略曝光（用于下一根学习）
	if e.cfg.StrategyLearn {
		e.lastExpo = make(map[string]map[string]float64)
		for s, m := range e.stratTargets {
			if e.lastExpo[s] == nil {
				e.lastExpo[s] = make(map[string]float64)
			}
			ws := sw[s]
			for inst, v := range m {
				e.lastExpo[s][inst] = ws * v
			}
		}
	}

	meta := Meta{
		StrategyWeights: sw,
		PortfolioVolAnn: sigmaAnn * scaler,
		Gross:           sumAbs(final),
		Turnover:        turnover,
		Scaler:          scaler,
	}
	return final, meta
}

// ===================== 内部：风险预算与波动目标 =====================

func (e *Engine) allocateRisk(agg map[string]float64) map[string]float64 {
	// 计算 σ_i 与信号强度 b_i = |s_i|
	sig := make(map[string]float64)
	sigma := make(map[string]float64)
	sumB := 0.0
	for inst, s := range agg {
		v := math.Abs(safe(s))
		if v < 1e-12 {
			continue
		}
		sig[inst] = v
		std := e.ensure(inst).vol()
		if math.IsNaN(std) || std < e.cfg.VolFloor {
			std = e.cfg.VolFloor
		}
		sigma[inst] = std
		sumB += v
	}
	if sumB == 0 {
		return nil
	}

	w := make(map[string]float64)
	if e.cfg.UseRiskParity || !e.cfg.UseMinVarApprox {
		// 风险平价：w_i ∝ b_i / σ_i（再按信号方向加符号）
		sum := 0.0
		for inst := range sig {
			sum += sig[inst] / sigma[inst]
		}
		if sum == 0 {
			return nil
		}
		for inst, b := range sig {
			w[inst] = sign(agg[inst]) * (b / sigma[inst]) / sum
		}
	} else {
		// 极简 Min-Var 近似：逆方差 + 信号幅度修正 + 归一
		sum := 0.0
		for inst := range sig {
			sum += 1.0 / (sigma[inst] * sigma[inst])
		}
		if sum == 0 {
			return nil
		}
		for inst := range sig {
			w[inst] = (1.0 / (sigma[inst] * sigma[inst])) / sum
		}
		for inst := range w {
			w[inst] *= sign(agg[inst]) * math.Max(0.2, sig[inst])
		}
		// 归一化
		s := sumAbs(w)
		if s > 0 {
			for k := range w {
				w[k] /= s
			}
		}
	}
	// 夹断 NaN/Inf
	for k := range w {
		w[k] = clamp(safe(w[k]), -1, 1)
	}
	return w
}

func (e *Engine) volTargetScaler(w map[string]float64) (scaler, sigmaAnn float64) {
	// Σ_ij ≈ σ_i σ_j ρ_ij（ρ_ij 若未知 => 0；对角 1）
	sigma := make(map[string]float64)
	list := make([]string, 0, len(w))
	for inst := range w {
		list = append(list, inst)
		std := e.ensure(inst).vol()
		if math.IsNaN(std) || std < e.cfg.VolFloor {
			std = e.cfg.VolFloor
		}
		sigma[inst] = std
	}

	var varP float64
	for i := 0; i < len(list); i++ {
		wi := safe(w[list[i]])
		si := sigma[list[i]]
		for j := 0; j < len(list); j++ {
			wj := safe(w[list[j]])
			sj := sigma[list[j]]
			rho := 0.0
			if i == j {
				rho = 1.0
			} else if c := e.corr[key2(list[i], list[j])]; c != nil {
				r := clamp(c.corr(), -0.99, 0.99)
				if !math.IsNaN(r) && !math.IsInf(r, 0) {
					rho = r
				}
			}
			varP += wi * wj * si * sj * rho
		}
	}
	if varP < 0 || math.IsNaN(varP) || math.IsInf(varP, 0) {
		varP = 0
	}
	barSigma := math.Sqrt(varP)
	ann := math.Sqrt((365 * 24 * 60) / float64(maxi(1, e.cfg.BarMinutes)))
	sigmaAnn = barSigma * ann
	if sigmaAnn <= 0 {
		return 1.0, 0
	}
	scaler = e.cfg.TargetVolAnnual / sigmaAnn
	if math.IsNaN(scaler) || math.IsInf(scaler, 0) {
		scaler = 1.0
	}
	return scaler, sigmaAnn
}

func (e *Engine) applyLimits(w map[string]float64) map[string]float64 {
	// 单品种上限
	for inst, v := range w {
		lim := 1.0
		if x, ok := e.cfg.PerInstrumentMax[inst]; ok && x > 0 {
			lim = x
		}
		w[inst] = clamp(safe(v), -lim, lim)
	}
	// 现金缓冲 + 杠杆/Gross
	targetGross := math.Min(e.cfg.MaxGross, e.cfg.MaxLeverage) * (1 - e.cfg.CashBufferPct)
	if targetGross <= 0 {
		return w
	}
	gross := sumAbs(w)
	if gross > targetGross {
		scale := targetGross / (gross + 1e-12)
		for k := range w {
			w[k] = safe(w[k] * scale)
		}
	}
	return w
}

func (e *Engine) rebalanceGate(w map[string]float64) (map[string]float64, float64) {
	// 只有在到达再平衡节拍时才考虑门限与换手
	if e.cfg.RebalanceIntervalBars > 0 && e.barCount%e.cfg.RebalanceIntervalBars != 0 {
		// 未到节拍：直接沿用上次
		return e.lastTargets, 0
	}

	// 单品种门限
	final := make(map[string]float64, len(w))
	for k, v := range w {
		prev := e.lastTargets[k]
		if math.Abs(v-prev) < e.cfg.DriftThreshold {
			final[k] = prev
		} else {
			final[k] = v
		}
	}
	// 统一衡量换手
	turnover := 0.0
	keys := unionKeys(w, e.lastTargets)
	for _, k := range keys {
		turnover += math.Abs(safe(final[k]) - safe(e.lastTargets[k]))
	}
	// 换手上限
	if e.cfg.TurnoverCap > 0 && turnover > e.cfg.TurnoverCap {
		s := e.cfg.TurnoverCap / (turnover + 1e-12)
		for _, k := range keys {
			final[k] = e.lastTargets[k] + s*(final[k]-e.lastTargets[k])
		}
		turnover = e.cfg.TurnoverCap
	}
	e.lastTargets = final
	return final, turnover
}

// ===================== 内部：策略学习权重 =====================

func (e *Engine) strategyWeights() map[string]float64 {
	// 未启用学习：按配置/均匀
	if !e.cfg.StrategyLearn {
		w := make(map[string]float64)
		sum := 0.0
		for strat := range e.stratTargets {
			v := e.cfg.StrategyWeights[strat]
			if v <= 0 {
				v = 1.0
			}
			w[strat] = v
			sum += v
		}
		if sum == 0 {
			for strat := range e.stratTargets {
				w[strat] = 1.0
				sum += 1.0
			}
		}
		for k := range w {
			w[k] /= sum
		}
		return w
	}

	// 启用学习：prior * softplus(Sharpe)，小样本回落先验
	const minSamples = 60
	w := make(map[string]float64)
	sum := 0.0
	for strat := range e.stratTargets {
		prior := e.cfg.StrategyWeights[strat]
		if prior <= 0 {
			prior = 1.0
		}
		trk := e.ensureSharpe(strat)
		val := prior
		if trk.count >= minSamples {
			val = prior * softplus(trk.sharpe())
		}
		w[strat] = val
		sum += val
	}
	if sum == 0 {
		for strat := range e.stratTargets {
			w[strat] = 1.0 / float64(len(e.stratTargets))
		}
	} else {
		for k := range w {
			w[k] /= sum
		}
	}
	return w
}

// ===================== 状态与工具 =====================

type instState struct {
	prevClose float64
	lastRet   float64
	volEW     *ewVar
}

func (e *Engine) ensure(inst string) *instState {
	if s, ok := e.inst[inst]; ok {
		return s
	}
	s := &instState{volEW: newEWVar(alphaFromHL(maxi(2, e.cfg.EWHalfLifeVol)))}
	e.inst[inst] = s
	return s
}

func (s *instState) pushClose(c float64) {
	if s.prevClose > 0 && c > 0 {
		r := math.Log(c / s.prevClose)
		if !math.IsNaN(r) && !math.IsInf(r, 0) {
			s.lastRet = r
			s.volEW.push(r)
		}
	}
	s.prevClose = c
}
func (s *instState) vol() float64 { return s.volEW.std() }

// 策略 Sharpe 的在线估计器（不做年化，年化系数在组合层统一处理）
type sharpeTracker struct {
	retEW, varEW *ewVar
	count        int
}

func newSharpeTracker(a float64) *sharpeTracker {
	return &sharpeTracker{retEW: newEWVar(a), varEW: newEWVar(a)}
}
func (s *sharpeTracker) push(r float64) {
	if math.IsNaN(r) || math.IsInf(r, 0) {
		return
	}
	s.retEW.push(r)
	s.varEW.push(r)
	s.count++
}
func (s *sharpeTracker) sharpe() float64 {
	if s.count < 30 {
		return 0
	}
	m := s.retEW.mean()
	v := s.varEW.variance()
	if v <= 0 {
		return 0
	}
	return m / math.Sqrt(v+1e-12)
}

func (e *Engine) ensureSharpe(name string) *sharpeTracker {
	if tr, ok := e.stratPerf[name]; ok {
		return tr
	}
	tr := newSharpeTracker(alphaFromHL(256))
	e.stratPerf[name] = tr
	return tr
}

// ===================== 统计组件 =====================

// 指数加权方差/均值
type ewVar struct {
	alpha  float64
	m      float64
	v      float64
	inited bool
}

func newEWVar(alpha float64) *ewVar {
	if alpha <= 0 || alpha >= 1 {
		alpha = 0.94
	}
	return &ewVar{alpha: alpha}
}
func (e *ewVar) push(x float64) {
	if !e.inited {
		e.m = x
		e.inited = true
		return
	}
	dx := x - e.m
	e.m += e.alpha * dx
	e.v = (1-e.alpha)*e.v + e.alpha*dx*dx
}
func (e *ewVar) std() float64 {
	if e.v < 0 {
		return 0
	}
	return math.Sqrt(e.v)
}
func (e *ewVar) mean() float64     { return e.m }
func (e *ewVar) variance() float64 { return e.v }

// 指数加权相关
type ewCorr struct {
	alpha       float64
	mx, my      float64
	vx, vy, cov float64
	inited      bool
}

func newEWCorr(alpha float64) *ewCorr {
	if alpha <= 0 || alpha >= 1 {
		alpha = 0.94
	}
	return &ewCorr{alpha: alpha}
}
func (e *ewCorr) push(x, y float64) {
	if !e.inited {
		e.mx = x
		e.my = y
		e.inited = true
		return
	}
	a := e.alpha
	b := 1 - a
	dx := x - e.mx
	dy := y - e.my
	e.mx += a * dx
	e.my += a * dy
	e.vx = b*e.vx + a*dx*dx
	e.vy = b*e.vy + a*dy*dy
	e.cov = b*e.cov + a*dx*dy
}
func (e *ewCorr) corr() float64 {
	if e.vx <= 0 || e.vy <= 0 {
		return 0
	}
	v := e.cov / (math.Sqrt(e.vx*e.vy) + 1e-12)
	if math.IsNaN(v) || math.IsInf(v, 0) {
		return 0
	}
	return v
}

// ===================== 小工具 =====================

func key2(a, b string) [2]string {
	if a < b {
		return [2]string{a, b}
	}
	return [2]string{b, a}
}
func alphaFromHL(hl int) float64 {
	if hl <= 1 {
		return 0.5
	}
	return 1 - math.Exp(-math.Log(2)/float64(hl))
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
func sign(x float64) float64 {
	if x < 0 {
		return -1
	}
	if x > 0 {
		return 1
	}
	return 0
}
func maxi(a, b int) int {
	if a > b {
		return a
	}
	return b
}
func sumAbs(m map[string]float64) float64 {
	s := 0.0
	for _, v := range m {
		s += math.Abs(v)
	}
	return s
}
func unionKeys(a, b map[string]float64) []string {
	ks := make(map[string]struct{})
	for k := range a {
		ks[k] = struct{}{}
	}
	for k := range b {
		ks[k] = struct{}{}
	}
	out := make([]string, 0, len(ks))
	for k := range ks {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
func softplus(x float64) float64 {
	if x > 20 {
		return x
	}
	return math.Log1p(math.Exp(x))
}
func safe(x float64) float64 {
	if math.IsNaN(x) || math.IsInf(x, 0) {
		return 0
	}
	return x
}
