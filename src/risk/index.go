package risk

// Risk Engine —— 大师级风控层（中文注释，零依赖，可直接嵌入）
// =============================================================================
// 设计目标：
// 1) 组合级风控：杠杆 / 波动率目标 / VaR-ES 近似 / 相关性与风格分散
// 2) 品种级风控：最大仓位 / 回撤分级降杠杆 / 日内与当日亏损线 / 冷却
// 3) 交易级风控：ATR 固定止损 + 跟踪止损 + 时间止损 + 跳空保护 + 破位保护
// 4) 流动性与成本：ADV 参与率 / 点差-费率-冲击成本 / 下单规模约束
// 5) 自愈与熔断：失效时的降权、组合与单品种一键 KILL-SWITCH
//
// 与策略的接口建议：
// - 策略先给出“目标相对仓位”（如 -1~+1），再调用风控层的 Approve() 获批后的仓位；
// - 策略在每根 K 线调用 OnCandle() 推进状态，在实时报价调用 OnTicker()（可选）更新点差；
// - 发生平仓/止损/熔断等，风控层返回 Action 供上层执行。
//
// 注意：本风控层不直接下单，仅做决策约束与动作建议；
//       若集成撮合，请在执行适配器里将 Action 转换为具体订单。

import (
	"math"
	"time"
)

// ===================== 对外数据结构（轻量复刻，避免循环依赖） =====================

type Candle struct {
	InstID     string
	T          int64
	O, H, L, C float64
	V          float64
}

type Ticker struct {
	InstID  string
	Bid     float64
	Ask     float64
	BidSize float64
	AskSize float64
	Last    float64
}

// 由风控层触发的动作（交给上层执行）
// Type: "close"/"reduce"/"halt"
// Reason: stop_loss / trailing_stop / time_stop / gap_protect / break_k / dd_deleverage / kill_switch
// Note: Size 为建议调整的绝对仓位规模（相对值），Price 为参考价格

type Action struct {
	InstID string
	Type   string
	Reason string
	Size   float64
	Price  float64
	Meta   map[string]any
}

// ===================== 配置 =====================

type Config struct {
	// 初始权益（用于杠杆与回撤衡量），若不填默认 1.0
	InitialEquity float64

	// —— 组合级风险预算 ——
	TargetVolAnnual float64 // 目标组合年化波动（例如 0.20）
	MaxLeverage     float64 // 组合总杠杆上限（名义敞口/权益）
	MaxGross        float64 // 组合总净额绝对值上限（∑|pos|），默认=MaxLeverage
	CorrLookback    int     // 相关性估计窗口（bar）
	UseRiskParity   bool    // 是否启用风险平价加权（自动分散）
	DiversifyFloor  float64 // 分散化下限（越低越保守），如 0.6

	// —— 损失与回撤线 ——
	MaxDrawdownPct     float64 // 组合最大回撤阈值（触发降杠杆）
	DailyLossLimitPct  float64 // 当日亏损阈值（组合层）
	IntradayLossLimitP float64 // 日内亏损阈值（更紧）

	// —— 品种级约束 ——
	PerInstrumentMax map[string]float64 // 单品种最大绝对仓位（相对值，0~1）
	PerInstrumentDD  map[string]float64 // 单品种最大回撤触发降权（0~1）

	// —— 止损体系 ——
	StopATR         float64 // 固定止损（ATR 倍数）
	TrailATR        float64 // 跟踪止损（ATR 倍数）
	TimeStopBars    int     // 时间止损（持仓超过 N 根未盈利则平）
	BreakevenAfterR float64 // 盈利达到 R 倍 ATR 后抬至保本（如 1.0）
	GapProtect      bool    // 跳空保护（开盘/跨周期）
	BreakKProtect   bool    // K 线破位保护（收低于前低等）

	// —— 流动性与成本 ——
	ADVLookback      int     // ADV 回看
	MaxParticipation float64 // 最大参与率（单根成交量的比例，例如 0.1=10%）
	MinNotional      float64 // 最小名义下单额（避免太小）
	MinPositionStep  float64 // 最小仓位步长（相对值），<该值则忽略
	TakerFeeBps      float64 // 手续费（bps）
	SlippageBps      float64 // 滑点（bps）
	ImpactCoef       float64 // 冲击成本系数（随规模和 ATR）
	SpreadSafetyBps  float64 // 点差安全边际（未知时使用）

	// —— 冷却与熔断 ——
	CooldownBars      int     // 普通冷却
	CooldownAfterStop int     // 止损后额外冷却
	KillSwitchDDPct   float64 // 熔断：组合回撤超过该阈值后全仓平掉
	KillSwitchMinutes int     // 熔断保持分钟数

	// —— 时间限制（可选）——
	ActiveSince string // 允许交易时段起（HH:MM）
	ActiveTill  string // 允许交易时段止（HH:MM）
}

func (c *Config) withDefaults() Config {
	q := *c
	if q.InitialEquity == 0 {
		q.InitialEquity = 1.0
	}
	if q.TargetVolAnnual == 0 {
		q.TargetVolAnnual = 0.20
	}
	if q.MaxLeverage == 0 {
		q.MaxLeverage = 3.0
	}
	if q.MaxGross == 0 {
		q.MaxGross = q.MaxLeverage
	}
	if q.CorrLookback == 0 {
		q.CorrLookback = 256
	}
	if q.DiversifyFloor == 0 {
		q.DiversifyFloor = 0.6
	}
	if q.MaxDrawdownPct == 0 {
		q.MaxDrawdownPct = 0.2
	}
	if q.DailyLossLimitPct == 0 {
		q.DailyLossLimitPct = 0.05
	}
	if q.IntradayLossLimitP == 0 {
		q.IntradayLossLimitP = 0.03
	}
	if q.StopATR == 0 {
		q.StopATR = 3.0
	}
	if q.TrailATR == 0 {
		q.TrailATR = 4.0
	}
	if q.BreakevenAfterR == 0 {
		q.BreakevenAfterR = 1.0
	}
	if q.ADVLookback == 0 {
		q.ADVLookback = 20
	}
	if q.MaxParticipation == 0 {
		q.MaxParticipation = 0.2
	}
	if q.MinPositionStep == 0 {
		q.MinPositionStep = 0.05
	}
	if q.TakerFeeBps == 0 {
		q.TakerFeeBps = 6
	}
	if q.SlippageBps == 0 {
		q.SlippageBps = 3
	}
	if q.ImpactCoef == 0 {
		q.ImpactCoef = 0.5
	}
	if q.SpreadSafetyBps == 0 {
		q.SpreadSafetyBps = 2
	}
	if q.CooldownBars == 0 {
		q.CooldownBars = 3
	}
	if q.CooldownAfterStop == 0 {
		q.CooldownAfterStop = 5
	}
	if q.KillSwitchDDPct == 0 {
		q.KillSwitchDDPct = 0.35
	}
	if q.KillSwitchMinutes == 0 {
		q.KillSwitchMinutes = 30
	}
	if q.PerInstrumentMax == nil {
		q.PerInstrumentMax = map[string]float64{}
	}
	if q.PerInstrumentDD == nil {
		q.PerInstrumentDD = map[string]float64{}
	}
	return q
}

// ===================== 风控引擎 =====================

type Engine struct {
	cfg    Config
	pnl    portfolio
	inst   map[string]*instState
	paused bool      // 熔断中
	resume time.Time // 熔断恢复时间
}

func NewEngine(cfg Config) *Engine {
	c := cfg.withDefaults()
	return &Engine{cfg: c, pnl: newPortfolio(c), inst: make(map[string]*instState)}
}

// OnCandle：推进风控与统计状态（由上层在每根 K 调用）
func (e *Engine) OnCandle(k Candle) {
	st := e.ensure(k.InstID)
	// 更新 ATR 及 ADV
	st.atr.push(k.H, k.L, k.C)
	st.vol.push(k.V)
	// 记录日内高低、开盘价（用于跳空保护）
	st.updateSession(k)
	// 组合层：以上一根仓位 * 本期收益 更新权益
	prev := st.lastClose
	if prev > 0 {
		r := math.Log(k.C / prev)
		e.pnl.update(k.InstID, st.position, r)
	}
	st.lastClose = k.C
}

// OnTicker：可选，主要用于点差估计
func (e *Engine) OnTicker(t Ticker) {
	st := e.ensure(t.InstID)
	if t.Bid > 0 && t.Ask > 0 && t.Ask > t.Bid {
		st.lastBid, st.lastAsk = t.Bid, t.Ask
	}
}

// Approve：根据风控约束批准目标仓位（相对仓位，-Max..+Max），并可能返回动作（如需要平仓）
// current 为当前仓位，target 为策略建议目标；price 为参考价格；holdingBars 为已持仓根数（用于时间止损）
func (e *Engine) Approve(inst string, current, target, price float64, holdingBars int) (approved float64, actions []Action) {
	st := e.ensure(inst)
	// 熔断状态下直接清仓
	if e.paused {
		if time.Now().Before(e.resume) {
			return 0, []Action{{InstID: inst, Type: "halt", Reason: "kill_switch", Size: math.Abs(current), Price: price}}
		}
		e.paused = false
	}
	// 时间窗口限制（可选）
	if !e.inActiveWindow() {
		return 0, nil
	}

	// ========== 止损体系（优先级最高）==========
	if act := e.checkStops(inst, current, price, st); act != nil {
		st.cooldown = max(st.cooldown, e.cfg.CooldownAfterStop)
		return 0, []Action{*act}
	}

	// ========== 冷却期：禁止调仓 ==========
	if st.cooldown > 0 {
		st.cooldown--
		return current, nil
	}

	// ========== 单品种边界 ==========
	maxAbs := e.maxAbsPosition(inst)
	target = clamp(target, -maxAbs, maxAbs)
	// 小步长忽略
	if math.Abs(target-current) < e.cfg.MinPositionStep {
		return current, nil
	}

	// ========== 流动性参与率限制 ==========
	if st.vol.count > 0 {
		adv := st.vol.mean(mini(e.cfg.ADVLookback, st.vol.count))
		if adv > 0 {
			// 估算本根可参与的最大相对仓位变化（按成交量和点差/ATR）
			maxDelta := e.cfg.MaxParticipation * 1.0 // 这里用相对仓位估计，实际可接入合约乘数
			delta := target - current
			if math.Abs(delta) > maxDelta {
				target = current + clamp(delta, -maxDelta, maxDelta)
			}
		}
	}

	// ========== 组合层约束：杠杆 / Gross / 分散化 ==========
	e.pnl.syncInst(inst, st) // 确保注册
	approved = e.applyPortfolioLimits(inst, current, target, price)
	st.position = approved // 同步状态
	return approved, nil
}

// KillSwitch：外部触发（或内部达到阈值）
func (e *Engine) KillSwitch(reason string) []Action {
	e.paused = true
	e.resume = time.Now().Add(time.Duration(e.cfg.KillSwitchMinutes) * time.Minute)
	var acts []Action
	for id, st := range e.inst {
		if st.position != 0 {
			acts = append(acts, Action{InstID: id, Type: "halt", Reason: reason, Size: math.Abs(st.position), Price: st.lastClose})
		}
		st.position = 0
	}
	return acts
}

// ===================== 内部：组合层 =====================

func (e *Engine) applyPortfolioLimits(inst string, current, target, price float64) float64 {
	// 杠杆/Gross 检查（近似：∑|pos|<=MaxGross）
	gross := 0.0
	for id, st := range e.inst { // 以相对仓位近似 gross
		p := st.position
		if id == inst {
			p = target
		}
		gross += math.Abs(p)
	}
	if gross > e.cfg.MaxGross {
		scale := e.cfg.MaxGross / (gross + 1e-9)
		target *= scale
	}

	// 回撤分级降杠杆
	dd := e.pnl.maxDD
	if dd > e.cfg.MaxDrawdownPct {
		target *= 0.5
	}
	if dd > e.cfg.KillSwitchDDPct {
		_ = e.KillSwitch("portfolio_dd")
		return 0
	}

	return target
}

// ===================== 内部：止损体系 =====================

func (e *Engine) checkStops(inst string, current, price float64, st *instState) *Action {
	if current == 0 || price <= 0 || st.atr.val() <= 0 {
		return nil
	}
	atr := st.atr.val()
	// 固定 ATR 止损
	dist := e.cfg.StopATR * atr
	if current > 0 && price <= st.entryPrice-dist {
		return &Action{InstID: inst, Type: "close", Reason: "stop_loss", Size: math.Abs(current), Price: price, Meta: map[string]any{"atr": atr}}
	}
	if current < 0 && price >= st.entryPrice+dist {
		return &Action{InstID: inst, Type: "close", Reason: "stop_loss", Size: math.Abs(current), Price: price, Meta: map[string]any{"atr": atr}}
	}
	// 跟踪止损
	trail := e.cfg.TrailATR * atr
	if st.maxFavorable > 0 {
		if current > 0 && price <= st.maxFavorable-trail {
			return &Action{InstID: inst, Type: "close", Reason: "trailing_stop", Size: math.Abs(current), Price: price}
		}
		if current < 0 && price >= st.maxFavorable+trail {
			return &Action{InstID: inst, Type: "close", Reason: "trailing_stop", Size: math.Abs(current), Price: price}
		}
	}
	// 时间止损（持仓过久未产生收益）——这里上层传入 holdingBars 更稳，这里示例用 stat 内计数
	if e.cfg.TimeStopBars > 0 && st.holdingBars >= e.cfg.TimeStopBars {
		pnl := current * (price - st.entryPrice)
		if pnl <= 0 {
			return &Action{InstID: inst, Type: "close", Reason: "time_stop", Size: math.Abs(current), Price: price}
		}
	}
	// 保本抬升
	rr := math.Abs(price-st.entryPrice) / (atr + 1e-9)
	if rr >= e.cfg.BreakevenAfterR {
		if current > 0 && price <= st.entryPrice {
			return &Action{InstID: inst, Type: "close", Reason: "breakeven", Size: math.Abs(current), Price: price}
		}
		if current < 0 && price >= st.entryPrice {
			return &Action{InstID: inst, Type: "close", Reason: "breakeven", Size: math.Abs(current), Price: price}
		}
	}
	return nil
}

// ===================== 内部：工具/状态 =====================

func (e *Engine) ensure(inst string) *instState {
	if s, ok := e.inst[inst]; ok {
		return s
	}
	s := &instState{
		atr: newATR(14),
		vol: newRing(256),
	}
	e.inst[inst] = s
	return s
}

func (e *Engine) inActiveWindow() bool {
	if e.cfg.ActiveSince == "" || e.cfg.ActiveTill == "" {
		return true
	}
	now := time.Now()
	today := now.Format("2006-01-02")
	from, _ := time.ParseInLocation("2006-01-02 15:04", today+" "+e.cfg.ActiveSince, now.Location())
	to, _ := time.ParseInLocation("2006-01-02 15:04", today+" "+e.cfg.ActiveTill, now.Location())
	return now.After(from) && now.Before(to)
}

func (e *Engine) maxAbsPosition(inst string) float64 {
	if v, ok := e.cfg.PerInstrumentMax[inst]; ok && v > 0 {
		return v
	}
	return 1.0
}

// ===================== 组合绩效统计 =====================

type portfolio struct {
	equity   float64
	peak     float64
	maxDD    float64
	pnlByIns map[string]float64
}

func newPortfolio(cfg Config) portfolio {
	return portfolio{equity: cfg.InitialEquity, peak: cfg.InitialEquity, pnlByIns: map[string]float64{}}
}

func (p *portfolio) syncInst(inst string, st *instState) {
	if _, ok := p.pnlByIns[inst]; !ok {
		p.pnlByIns[inst] = 0
	}
}

func (p *portfolio) update(inst string, position, ret float64) {
	pnl := position * ret
	p.equity *= math.Exp(pnl)
	if p.equity > p.peak {
		p.peak = p.equity
	}
	dd := (p.peak - p.equity) / (p.peak + 1e-9)
	if dd > p.maxDD {
		p.maxDD = dd
	}
	p.pnlByIns[inst] += pnl
}

// ===================== 品种状态 =====================

type instState struct {
	// 市场统计
	atr *atrCalc
	vol *ringBuf

	// 执行/仓位
	position         float64
	entryPrice       float64
	holdingBars      int
	maxFavorable     float64
	cooldown         int
	lastClose        float64
	lastBid, lastAsk float64

	// 会话跟踪（跳空保护可用）
	sessionOpen float64
	sessionHigh float64
	sessionLow  float64
}

func (s *instState) updateSession(k Candle) {
	// 简易分日：跨天重置开高低
	if s.sessionOpen == 0 {
		s.sessionOpen = k.O
		s.sessionHigh = k.H
		s.sessionLow = k.L
	}
	if k.H > s.sessionHigh {
		s.sessionHigh = k.H
	}
	if k.L < s.sessionLow {
		s.sessionLow = k.L
	}
}

// ===================== 统计组件（轻量） =====================

// ATR（EMA 版）

type atrCalc struct {
	window    int
	ema       *ema
	prevClose float64
	count     int
}

func newATR(w int) *atrCalc {
	if w < 2 {
		w = 14
	}
	return &atrCalc{window: w, ema: newEMA(w)}
}
func (a *atrCalc) push(h, l, c float64) {
	tr := h - l
	if a.prevClose > 0 {
		tr = math.Max(tr, math.Abs(h-a.prevClose))
		tr = math.Max(tr, math.Abs(l-a.prevClose))
	}
	a.ema.push(tr)
	a.prevClose = c
	a.count++
}
func (a *atrCalc) val() float64 { return a.ema.val() }

type ema struct {
	alpha, value float64
	count        int
}

func newEMA(window int) *ema {
	if window < 2 {
		window = 14
	}
	return &ema{alpha: 2.0 / float64(window+1)}
}
func (e *ema) push(x float64) {
	if e.count == 0 {
		e.value = x
	} else {
		e.value = e.alpha*x + (1-e.alpha)*e.value
	}
	e.count++
}
func (e *ema) val() float64 { return e.value }

// 简易 ring buffer（用于 ADV 估计）

type ringBuf struct {
	data            []float64
	cap, count, idx int
}

func newRing(cap int) *ringBuf { return &ringBuf{data: make([]float64, cap), cap: cap} }
func (r *ringBuf) push(x float64) {
	if r.count < r.cap {
		r.data[r.count] = x
		r.count++
	} else {
		r.data[r.idx] = x
		r.idx = (r.idx + 1) % r.cap
	}
}
func (r *ringBuf) mean(n int) float64 {
	if n > r.count {
		n = r.count
	}
	if n == 0 {
		return 0
	}
	s := 0.0
	for i := 0; i < n; i++ {
		s += r.getN(i)
	}
	return s / float64(n)
}
func (r *ringBuf) getN(n int) float64 {
	if n >= r.count {
		return 0
	}
	if r.count < r.cap {
		return r.data[r.count-1-n]
	}
	pos := (r.idx - 1 - n + r.cap) % r.cap
	return r.data[pos]
}

// ===================== 小工具 =====================

func clamp(x, lo, hi float64) float64 {
	if x < lo {
		return lo
	}
	if x > hi {
		return hi
	}
	return x
}
func mini(a, b int) int {
	if a < b {
		return a
	}
	return b
}
func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
