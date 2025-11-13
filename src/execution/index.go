package execution

import (
	"fmt"
	"math"
	"time"
)

type Side string

const (
	SideBuy  Side = "buy"
	SideSell Side = "sell"
)

type OrderType string

const (
	OrdLimit  OrderType = "limit"
	OrdMarket OrderType = "market"
)

type TIF string

const (
	GTC TIF = "GTC"
	IOC TIF = "IOC"
	FOK TIF = "FOK"
)

type OrderRequest struct {
	InstID      string
	Side        Side
	Type        OrderType
	Qty         float64
	Price       float64
	TimeInForce TIF
	PostOnly    bool
	ReduceOnly  bool
	ClientID    string
	Meta        map[string]any
}

type CancelRequest struct {
	InstID   string
	ClientID string
}

type Plan struct {
	Orders  []OrderRequest
	Cancels []CancelRequest
}

type OrderUpdate struct {
	InstID    string
	ClientID  string
	FilledQty float64
	Status    string // new/partially_filled/filled/canceled/rejected
	AvgPrice  float64
}

type Fill struct {
	InstID   string
	ClientID string
	Side     Side
	Qty      float64
	Price    float64
	Fee      float64
}

// ===================== 规格/配置 =====================

type InstrumentSpec struct {
	InstID        string
	TickSize      float64
	LotSize       float64
	ContractValue float64 // 每张名义（计价币），如 1 USDT/张
	MinNotional   float64 // 最小名义（计价币）
	MinQty        float64 // 最小下单张数
}

type Config struct {
	AccountEquity float64
	LeverageCap   float64

	MaxChildSlices   int
	SliceInterval    int
	MaxParticipation float64
	ChildMinQty      float64
	ChildMaxQty      float64

	PassivePriceOffsetTicks int
	AggressiveSlippageBps   float64

	PreferPassive bool
	UseIceberg    bool

	CancelStaleAfterMs int
	MaxRetries         int
}

func (c *Config) withDefaults() Config {
	q := *c
	if q.AccountEquity <= 0 {
		q.AccountEquity = 10000
	}
	if q.LeverageCap <= 0 {
		q.LeverageCap = 3
	}
	if q.MaxChildSlices <= 0 {
		q.MaxChildSlices = 4
	}
	if q.SliceInterval <= 0 {
		q.SliceInterval = 500
	}
	if q.MaxParticipation <= 0 {
		q.MaxParticipation = 0.05
	}
	if q.ChildMinQty <= 0 {
		q.ChildMinQty = 1
	}
	if q.ChildMaxQty <= 0 || q.ChildMaxQty < q.ChildMinQty {
		q.ChildMaxQty = 100
	}
	if q.PassivePriceOffsetTicks <= 0 {
		q.PassivePriceOffsetTicks = 1
	}
	if q.AggressiveSlippageBps <= 0 {
		q.AggressiveSlippageBps = 10
	}
	if q.CancelStaleAfterMs <= 0 {
		q.CancelStaleAfterMs = 3000
	}
	if q.MaxRetries <= 0 {
		q.MaxRetries = 2
	}
	return q
}

// ===================== 执行器主体 =====================

type Executor struct {
	cfg   Config
	specs map[string]InstrumentSpec
	ins   map[string]*state
}

func NewExecutor(cfg Config) *Executor {
	c := cfg.withDefaults()
	return &Executor{
		cfg:   c,
		specs: make(map[string]InstrumentSpec),
		ins:   make(map[string]*state),
	}
}

func (ex *Executor) RegisterInstrument(spec InstrumentSpec) { ex.specs[spec.InstID] = spec }

// Step —— 给定“批准后的相对仓位目标”，生成本时刻的下/撤建议
// approvedPosRel：-1..+1（或上层定义的 MaxAbs）
// markPrice：标记价（计价币）
// adv：近似可成交“张数”的日内/区间均值（用于参与率）
func (ex *Executor) Step(inst string, approvedPosRel float64, markPrice float64, adv float64) Plan {
	sp, ok := ex.specs[inst]
	if !ok || markPrice <= 0 || sp.ContractValue <= 0 {
		return Plan{}
	}
	st := ex.ensure(inst)

	// 先处理超时挂单的撤单（重定价交给下一拍）
	cancels := ex.findStaleCancels(inst, st)

	// 目标张数（向下取整到 lot；不足最小名义/最小张数则返回 0）
	target := ex.targetContracts(sp, approvedPosRel, markPrice)

	// 当前净仓位 + 净在途（同向）≈“有效仓位”
	outstanding := st.netOutstanding() // 同向为正、反向为负
	// 需要的“净增量”
	rawDelta := target - (st.position + outstanding)

	// 极小变化忽略（避免抖动）
	if nearlyZero(rawDelta) {
		return Plan{Cancels: cancels}
	}

	orders := make([]OrderRequest, 0, 2)

	// —— 若方向发生切换：先 ReduceOnly 将仓位减到 0 —— //
	if st.position*rawDelta < 0 && !nearlyZero(st.position) {
		needReduce := math.Min(math.Abs(st.position), math.Abs(rawDelta))
		child := ex.childQty(sp, needReduce, adv)
		if child > 0 {
			px := ex.priceAggressive(markPrice, sideOpposite(st.position), sp) // 受控滑点
			ord := ex.makeOrderIOC(inst, sideOpposite(st.position), child, px, true /*RO*/)
			orders = append(orders, ord)
			st.addOpen(ord.ClientID, ord.Side, ord.Qty, ord.Price)
			// 先把减仓发出去，下一拍再继续（避免一次做两件事）
			return Plan{Orders: orders, Cancels: cancels}
		}
	}

	// —— 同向调仓或已完成减仓 —— //
	if !nearlyZero(rawDelta) {
		child := ex.childQty(sp, math.Abs(rawDelta), adv)
		if child > 0 {
			sd := sideOf(rawDelta)
			if ex.cfg.PreferPassive {
				px := ex.pricePassive(markPrice, sd, sp)
				ord := ex.makeOrderLimit(inst, sd, child, px, true /*post-only*/, false /*RO*/)
				orders = append(orders, ord)
				st.addOpen(ord.ClientID, ord.Side, ord.Qty, ord.Price)
			} else {
				px := ex.priceAggressive(markPrice, sd, sp)
				ord := ex.makeOrderIOC(inst, sd, child, px, false /*RO*/)
				orders = append(orders, ord)
				st.addOpen(ord.ClientID, ord.Side, ord.Qty, ord.Price)
			}
		}
	}

	return Plan{Orders: orders, Cancels: cancels}
}

// —— 订单/成交回写 —— //

func (ex *Executor) OnOrderUpdate(u OrderUpdate) {
	st := ex.ensure(u.InstID)
	if _, ok := st.open[u.ClientID]; !ok {
		return
	}
	switch u.Status {
	case "canceled", "rejected":
		delete(st.open, u.ClientID)
	case "filled":
		delete(st.open, u.ClientID)
	}
}

func (ex *Executor) OnFill(f Fill) {
	st := ex.ensure(f.InstID)
	dir := 1.0
	if f.Side == SideSell {
		dir = -1
	}
	st.position += dir * f.Qty

	// 扣减 open 剩余
	if o, ok := st.open[f.ClientID]; ok {
		o.Remaining -= f.Qty
		if o.Remaining <= lotEps {
			delete(st.open, f.ClientID)
		} else {
			st.open[f.ClientID] = o
		}
	}
}

// ===================== 内部状态与工具 =====================

type state struct {
	position float64              // 实际持仓（张）
	open     map[string]openOrder // 还在路上的单（用于在途净Δ）
}

type openOrder struct {
	Side      Side
	Qty       float64
	Price     float64
	Remaining float64
	Ts        time.Time
}

func (s *state) addOpen(id string, side Side, qty, price float64) {
	if s.open == nil {
		s.open = make(map[string]openOrder)
	}
	s.open[id] = openOrder{
		Side:      side,
		Qty:       qty,
		Price:     price,
		Remaining: qty,
		Ts:        time.Now(),
	}
}

// 净在途Δ：买单剩余为 +Remaining，卖单为 -Remaining
func (s *state) netOutstanding() float64 {
	sum := 0.0
	for _, o := range s.open {
		if o.Side == SideBuy {
			sum += o.Remaining
		} else {
			sum -= o.Remaining
		}
	}
	return sum
}

func (ex *Executor) ensure(inst string) *state {
	if s, ok := ex.ins[inst]; ok {
		return s
	}
	s := &state{open: make(map[string]openOrder)}
	ex.ins[inst] = s
	return s
}

// 目标张数换算（稳健版）
func (ex *Executor) targetContracts(sp InstrumentSpec, approvedPosRel float64, mark float64) float64 {
	r := clamp(approvedPosRel, -1, 1)
	if mark <= 0 || sp.ContractValue <= 0 {
		return 0
	}
	// 目标名义（计价币）
	notional := r * ex.cfg.AccountEquity * ex.cfg.LeverageCap
	if nearlyZero(notional) {
		return 0
	}
	// 原始张数
	raw := notional / sp.ContractValue
	// 先满足最小名义/最小张数
	minByNotional := 0.0
	if sp.MinNotional > 0 {
		minByNotional = sp.MinNotional / sp.ContractValue
	}
	minReq := math.Max(sp.MinQty, minByNotional)
	absRaw := math.Abs(raw)
	if absRaw < minReq {
		// 小目标直接忽略，避免被 lot 四舍五入成假单
		return 0
	}
	// 向“有利于不超”的方向取整到 lot
	q := roundDownToLot(absRaw, sp.LotSize)
	if q <= 0 {
		return 0
	}
	return signed(q, sign(raw))
}

// 子单 sizing（参与率优先→再夹断到[min,max]→向下取整到 lot）
func (ex *Executor) childQty(sp InstrumentSpec, need float64, adv float64) float64 {
	if need <= 0 {
		return 0
	}
	// 参与率上限对应的张数
	capAdv := math.Max(0, adv) * ex.cfg.MaxParticipation
	// 先按参与率截断
	q := math.Min(need, capAdv)
	// 再夹断到 [ChildMinQty, ChildMaxQty]
	q = clamp(q, ex.cfg.ChildMinQty, ex.cfg.ChildMaxQty)
	// 向下取整到 lot（绝不超过 need）
	q = roundDownToLot(q, sp.LotSize)
	if q <= 0 {
		return 0
	}
	// 最后再确保不超过 need
	if q > need {
		q = roundDownToLot(need, sp.LotSize)
	}
	if q <= 0 {
		return 0
	}
	return q
}

// 被动价：买 = mark - ticks，卖 = mark + ticks；严格对齐 tick，不越界
func (ex *Executor) pricePassive(mark float64, side Side, sp InstrumentSpec) float64 {
	ticks := float64(ex.cfg.PassivePriceOffsetTicks)
	offs := ticks * sp.TickSize
	if side == SideBuy {
		return roundToTick(maxFloat(mark-offs, sp.TickSize), sp.TickSize)
	}
	return roundToTick(mark+offs, sp.TickSize)
}

// 主动价：受控滑点 bps + tick 对齐
func (ex *Executor) priceAggressive(mark float64, side Side, sp InstrumentSpec) float64 {
	lim := math.Abs(ex.cfg.AggressiveSlippageBps) / 10000.0
	if lim < 0 {
		lim = 0
	}
	var px float64
	if side == SideBuy {
		px = mark * (1 + lim)
	} else {
		px = mark * (1 - lim)
	}
	px = roundToTick(px, sp.TickSize)
	if px <= 0 {
		px = sp.TickSize
	}
	return px
}

// 造单（Limit GTC / PostOnly 可选）
func (ex *Executor) makeOrderLimit(inst string, side Side, qty float64, price float64, postOnly bool, reduceOnly bool) OrderRequest {
	id := ex.cid(inst)
	return OrderRequest{
		InstID:      inst,
		Side:        side,
		Type:        OrdLimit,
		Qty:         qty,
		Price:       price,
		TimeInForce: GTC,
		PostOnly:    postOnly,
		ReduceOnly:  reduceOnly,
		ClientID:    id,
	}
}

// 造单（IOC + 可选 ReduceOnly）——用于减仓或强制推进
func (ex *Executor) makeOrderIOC(inst string, side Side, qty float64, price float64, reduceOnly bool) OrderRequest {
	id := ex.cid(inst)
	return OrderRequest{
		InstID:      inst,
		Side:        side,
		Type:        OrdLimit,
		Qty:         qty,
		Price:       price,
		TimeInForce: IOC,
		PostOnly:    false,
		ReduceOnly:  reduceOnly,
		ClientID:    id,
	}
}

// 撤单（超时即撤，重定价交回下一拍）
func (ex *Executor) findStaleCancels(inst string, st *state) []CancelRequest {
	if len(st.open) == 0 || ex.cfg.CancelStaleAfterMs <= 0 {
		return nil
	}
	now := time.Now()
	var out []CancelRequest
	exp := time.Duration(ex.cfg.CancelStaleAfterMs) * time.Millisecond
	for cid, o := range st.open {
		if now.Sub(o.Ts) >= exp {
			out = append(out, CancelRequest{InstID: inst, ClientID: cid})
		}
	}
	return out
}

// ===================== 工具 =====================

const lotEps = 1e-9

func roundToTick(px, tick float64) float64 {
	if tick <= 0 {
		return px
	}
	steps := math.Round(px / tick)
	return steps * tick
}

// “向下”取整到 lot：确保不会因为四舍五入超出 need
func roundDownToLot(qty, lot float64) float64 {
	if lot <= 0 {
		return qty
	}
	steps := math.Floor(qty/lot + 1e-12)
	return steps * lot
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
func signed(x, s float64) float64 { return x * s }
func nearlyZero(x float64) bool   { return math.Abs(x) < 1e-9 }

func sideOf(delta float64) Side {
	if delta >= 0 {
		return SideBuy
	}
	return SideSell
}
func sideOpposite(pos float64) Side {
	if pos > 0 {
		return SideSell
	}
	return SideBuy
}

func maxFloat(a, b float64) float64 {
	if a > b {
		return a
	}
	return b
}

func (ex *Executor) cid(inst string) string { return fmt.Sprintf("%s-%d", inst, time.Now().UnixNano()) }

// 适配器接口（与你原版一致）
type ExchangeAdapter interface {
	SendOrders([]OrderRequest) error
	CancelOrders([]CancelRequest) error
}
