package backtest

// Backtest 鈥旓拷?绂荤嚎鍥炴祴 / 鍘嗗彶澶嶇洏锛堜腑鏂囨敞閲婏紝闆剁涓夋柟渚濊禆锟?
// 璁捐鐩爣锟?
// 1) 椹卞姩閾捐矾锛歋trategy 锟?(Portfolio) 锟?Risk 锟?Execution锛堢畝鍖栨垚浜ゆā鍨嬶級锟?缁勫悎鏉冪泭锟?
// 2) 澶氬搧绉嶅悓姝ユ帹杩涳細鎸夋椂闂村綊骞舵墍鏈夊搧锟?K 绾匡紝鈥滈€愭椂鍒烩€濈粺涓€瑙﹀彂绛栫暐涓庡啀骞宠　锟?
// 3) 鎴愭湰涓庢粦鐐癸細鎸夋崲鎵嬫敹鍙栬垂锟?婊戠偣锛坆ps锛夛紝鏀寔鈥滄湰 bar 鏀剁洏鎴愪氦鈥濇垨鈥滀笅涓€ bar 寮€鐩樻垚浜も€濓紱
// 4) 鎸囨爣浜у嚭锛氭€绘敹鐩娿€佸勾鍖栥€丼harpe銆佹渶澶у洖鎾ゃ€佽儨鐜囥€佷氦鏄撴槑缁嗐€侀€愭棩/锟?bar 鏉冪泭鏇茬嚎锟?
// 5) 瑙ｈ€︼細鐢ㄨ交閲忔帴鍙ｅ鎺ヤ綘锟?Strategy / Risk / Portfolio 灞傦紝閬垮厤 import 寰幆锟?
// 澶囨敞锛氭湰鐗堝皢鈥滃悓涓€鏃堕棿鎴斥€濈殑澶氬搧绉嶆敹鐩婂厛鍚堝苟锛屽啀璁颁竴绗旂粍鍚堟敹鐩婏紙Sharpe 骞村寲鏇存纭級锟?

import (
	"encoding/json"
	"math"
	"sort"
)

// ===================== 杞婚噺鍏叡绫诲瀷锛堥伩鍏嶅惊鐜緷璧栵級 =====================

type Candle struct {
	InstID     string
	T          int64 // Unix 姣
	O, H, L, C float64
	V          float64
}

type Ticker struct { // 棰勭暀锛堜竴鑸洖娴嬩笉鐢級
	InstID  string
	Bid     float64
	Ask     float64
	BidSize float64
	AskSize float64
	Last    float64
}

type Signal struct { // 鏉ヨ嚜绛栫暐鐨勪俊鍙凤紙鐩稿浠撲綅鎴栧姩浣滐級
	InstID string
	Side   string  // "buy"/"sell"/"close"
	Size   float64 // 鐩稿浠撲綅锟?..1锛夛紝姝ｅ璐熺┖锛沜lose 蹇界暐 Size
	Price  float64 // 鍙€夛細鍙傝€冧环锛堜笉鐢ㄤ簬鍥炴祴鎴愪氦锟?
	Tag    string
	Meta   map[string]any
}

// Risk 鍔ㄤ綔锛堜笌 risk 灞傚搴旂殑杞婚噺鍖栧鍒伙級

type Action struct {
	InstID string
	Type   string // close/reduce/halt
	Reason string
	Size   float64 // 寤鸿璋冩暣鐨勭粷瀵逛粨浣嶈妯★紙鐩稿鍊硷級
	Price  float64
	Meta   map[string]any
}

// ===================== 澶栭儴鎺ュ彛锛堢敱浣犵殑鍏跺畠灞傞€傞厤锟?=====================

type Strategy interface {
	Name() string
	OnCandle(Candle) []Signal
	OnTicker(Ticker) []Signal
}

type Risk interface {
	OnCandle(Candle)
	OnTicker(Ticker)
	// current / target / price 涓虹浉瀵逛粨浣嶄笌鍙傝€冧环锛涜繑鍥炴壒鍑嗗悗鐨勪粨浣嶄笌闇€瑕佺珛鍗虫墽琛岀殑鍔ㄤ綔
	Approve(inst string, current, target, price float64, holdingBars int) (approved float64, actions []Action)
}

type Portfolio interface {
	// Propose 鎺ュ彈鍚勫搧绉嶇殑鈥滅瓥鐣ヤ俊鍙风洰鏍団€濓紙-1..+1锛夛紝杩斿洖缁忛闄╅锟?绾︽潫鍚庣殑鈥滅粍鍚堢洰鏍囷拷?
	Propose(mark map[string]float64) (map[string]float64, map[string]any)
	// SetStrategyTargets 鐢ㄤ簬鎻愪氦鏌愮瓥鐣ュ鍚勫搧绉嶇殑鐩爣锟?1..+1锟?
	SetStrategyTargets(strategy string, targets map[string]float64)
	OnCandle(Candle)
}

// 鑷畾涔夋垚锟?Hook锛堝彲閫夛級锛氬湪榛樿妯″瀷涔嬪墠/涔嬪悗骞查鎴愪氦浠锋垨鎴愭湰锛堢敤浜庡拰 Execution 鑱斿姩锟?
type FillHook func(inst string, side string, delta float64, refPrice float64) (fillPrice float64, extraCostBps float64)

// ===================== 鍥炴祴閰嶇疆涓庣粨锟?=====================

type Config struct {
	InitialEquity float64 // 鍒濆鏉冪泭锟?.0 琛ㄧず 1 鍗曚綅锟?
	BarMinutes    int     // bar 鍛ㄦ湡锛堢敤浜庡勾鍖栵級

	// 鎴愪氦涓庢垚锟?
	TradeOnNextBar bool    // true: 锟?bar 鍐崇瓥锛屼笅涓€ bar 寮€鐩樻垚浜わ紱false: 锟?bar 鏀剁洏鎴愪氦
	TakerFeeBps    float64 // 璐圭巼锛坆ps锟?
	MakerFeeBps    float64 // maker 璐圭巼锛坆ps锛夛紝UseMaker=true 鏃朵娇锟?
	SlippageBps    float64 // 婊戠偣锛坆ps锛夛紝鍙岃竟
	UseMaker       bool    // 鍋忓悜琚姩鎴愪氦锛堝洖娴嬪亣璁捐兘鎸傚埌锟?

	// 杩囨护涓庨棬锟?
	MinRebalanceStep float64 // 鏈€灏忚皟浠撴闀匡紙鐩稿浠撲綅锛夛紝灏忎簬鍒欏拷锟?
	MaxAbsPosition   float64 // 组合绝对权重上限（默认 1）

	// Hook
	BeforeFill FillHook // 鎴愪氦鍓嶅洖璋冿紙鍙皟鏁存垚浜や环/闄勫姞鎴愭湰锟?
	AfterFill  FillHook // 鎴愪氦鍚庡洖璋冿紙鍙褰曟垨杩藉姞鎴愭湰锟?
}

func (c *Config) withDefaults() Config {
	q := *c
	if q.InitialEquity == 0 {
		q.InitialEquity = 1.0
	}
	if q.BarMinutes == 0 {
		q.BarMinutes = 5
	}
	if q.TakerFeeBps == 0 {
		q.TakerFeeBps = 6
	}
	if q.SlippageBps == 0 {
		q.SlippageBps = 3
	}
	if q.MinRebalanceStep == 0 {
		q.MinRebalanceStep = 0.05
	}
	if q.MaxAbsPosition == 0 {
		q.MaxAbsPosition = 1.0
	}
	return q
}

// 鍗曠瑪浜ゆ槗锛堝紑鈫掑钩锛夎锟?

type Trade struct {
	InstID     string
	Dir        string // long/short
	EntryTime  int64
	EntryPrice float64
	ExitTime   int64
	ExitPrice  float64
	Size       float64 // 鎸佷粨鐩稿瑙勬ā锛堢粷瀵瑰€硷紝0..1锟?
	Return     float64 // 瀵瑰簲鏈熼棿鐨勫鏁版敹鐩婅础鐚紙绾︼級
}

// 锟?bar 璁板綍锛堝彲閫夊鍑虹粯鍥撅級

type BarRecord struct {
	Ts       int64
	Equity   float64
	Ret      float64
	Drawdown float64
}

// 缁撴灉

type Result struct {
	EquityCurve []BarRecord
	Trades      []Trade

	FinalEquity float64
	TotalRet    float64
	CAGR        float64
	Sharpe      float64
	MaxDD       float64
	WinRate     float64
	NumTrades   int
}

func (r Result) Summary() string {
	b, _ := json.MarshalIndent(map[string]any{
		"final_equity": r.FinalEquity,
		"total_return": r.TotalRet,
		"cagr":         r.CAGR,
		"sharpe":       r.Sharpe,
		"max_dd":       r.MaxDD,
		"win_rate":     r.WinRate,
		"num_trades":   r.NumTrades,
	}, "", "  ")
	return string(b)
}

// ===================== 寮曟搸 =====================

type Engine struct {
	cfg Config

	strategy  Strategy
	portfolio Portfolio // 鍙拷?
	risk      Risk      // 鍙拷?

	before FillHook
	after  FillHook
}

func New(cfg Config) *Engine {
	c := cfg.withDefaults()
	return &Engine{cfg: c, before: cfg.BeforeFill, after: cfg.AfterFill}
}

func (e *Engine) SetStrategy(s Strategy)   { e.strategy = s }
func (e *Engine) SetRisk(r Risk)           { e.risk = r }
func (e *Engine) SetPortfolio(p Portfolio) { e.portfolio = p }

// Series 杈撳叆锛氭瘡涓搧绉嶄竴锟?K 绾匡紙闇€鏃堕棿鍗囧簭锛涜嫢鏃犲垯鑷鎺掑簭锟?
type Series map[string][]Candle

// 鈥旓拷?鍐呴儴鎸佷粨涓庡緟鎵ц鐩爣锛堝懡鍚嶇被鍨嬶紝閬垮厤鍖垮悕缁撴瀯瀵艰嚧鐨勪笉鍏煎锟?鈥旓拷?

type instState struct {
	lastClose     float64
	pos           float64
	entryPx       float64
	entryTs       int64
	holding       int
	pendingTarget *pending
}

type pending struct {
	target  float64
	applyAt int64
}

// Run 鈥旓拷?鎵ц鏁存鍥炴祴
func (e *Engine) Run(series Series) Result {
	// 0) 棰勫鐞嗭細鎺掑簭 & 鐢熸垚缁熶竴鎷嶆墎搴忓垪
	all := flatten(series)
	if len(all) == 0 || e.strategy == nil {
		return Result{}
	}

	// 1) 鐘讹拷?
	eq := e.cfg.InitialEquity
	peak := eq
	var maxDD float64
	barAnn := math.Sqrt((365 * 24 * 60) / float64(maxi(1, e.cfg.BarMinutes)))

	// 姣忎釜鍝佺鐘讹拷?
	states := map[string]*instState{}
	for inst := range series {
		states[inst] = &instState{}
	}

	// 杈撳嚭
	var curve []BarRecord
	var trades []Trade
	var aggRets []float64

	// 2) 涓诲惊鐜細鎸夆€滄椂闂存埑鍒嗙粍鈥濇帹杩涳紙鍚屼竴鏃跺埢鍚堝苟鏀剁泭 锟?鏇村噯纭殑 Sharpe锟?
	i := 0
	for i < len(all) {
		ts := all[i].T
		// 2.0) 鏀堕泦璇ユ椂闂存埑鐨勬墍锟?K 绾匡紙宸茬ǔ瀹氭帓搴忥細锟?ts 鏃舵寜 InstID锟?
		j := i
		for j < len(all) && all[j].T == ts {
			j++
		}
		group := all[i:j] // [i, j)

		// 2.1) 鍏堟寜鈥滀笂涓€鏃跺埢浠撲綅鈥濊绠楁湰鏃跺埢鐨勭粍鍚堟敹鐩婂閲忥紙鍚堝苟鍚勫搧绉嶏級
		sumRet := 0.0
		for _, k := range group {
			st := states[k.InstID]
			if st != nil && st.lastClose > 0 && st.pos != 0 {
				r := math.Log(k.C / st.lastClose)
				sumRet += st.pos * r
			}
		}

		// 2.2) 鎺ㄨ繘 椋庢帶/缁勫悎 & 绛栫暐锛屾敹闆嗙洰锟?
		if e.risk != nil || e.portfolio != nil {
			for _, k := range group {
				if e.risk != nil {
					e.risk.OnCandle(k)
				}
				if e.portfolio != nil {
					e.portfolio.OnCandle(k)
				}
			}
		}

		// 绛栫暐淇″彿锛堟湰鏃跺埢姣忎釜鍝佺涓€鏉℃垨澶氭潯锟?
		var sigs []Signal
		for _, k := range group {
			sigs = append(sigs, e.strategy.OnCandle(k)...)
		}

		// 2.3) 缁勫悎鑱氬悎锛堣嫢锟?Portfolio锛夛紝鍚﹀垯鐩存帴鎶婄瓥鐣ヤ俊鍙疯浆涓虹洰锟?
		targets := map[string]float64{}
		if e.portfolio != nil {
			// mark 浼犲綋鍓嶆敹鐩橈紙鎴栧彲鐢ㄤ腑浠凤級
			mark := map[string]float64{}
			for _, k := range group {
				mark[k.InstID] = k.C
			}
			// 绛栫暐渚х粰锟?[-1,1] 鐨勭洰锟?
			want := map[string]float64{}
			for _, s := range sigs {
				v := s.Size
				switch s.Side {
				case "buy":
					v = math.Max(v, 0.0)
				case "sell":
					v = -math.Max(v, 0.0)
				case "close":
					v = 0
				}
				want[s.InstID] = clamp(v, -e.cfg.MaxAbsPosition, e.cfg.MaxAbsPosition)
			}
			e.portfolio.SetStrategyTargets(e.strategy.Name(), want)
			agg, _ := e.portfolio.Propose(mark)
			for k2, v := range agg {
				targets[k2] = v
			}
		} else {
			for _, s := range sigs {
				v := s.Size
				switch s.Side {
				case "buy":
					v = math.Max(v, 0.0)
				case "sell":
					v = -math.Max(v, 0.0)
				case "close":
					v = 0
				}
				targets[s.InstID] = clamp(v, -e.cfg.MaxAbsPosition, e.cfg.MaxAbsPosition)
			}
		}

		// 2.4) 灏嗙洰鏍囦氦锟?Risk 瀹℃壒 & 鐢熸垚鍔ㄤ綔锛堝彧瀹夋帓锛屼笉绔嬪嵆搴旂敤锟?
		for inst, tgt := range targets {
			ss := states[inst]
			if ss == nil {
				ss = &instState{}
				states[inst] = ss
			}
			cur := ss.pos
			if math.Abs(tgt-cur) < e.cfg.MinRebalanceStep {
				continue
			}
			approved := tgt
			var acts []Action
			if e.risk != nil {
				// 鍙傝€冧环锛氳嫢宸叉湁 lastClose 鐢ㄤ箣锛屽惁鍒欑敤褰撳墠 k.C
				ref := ss.lastClose
				if ref == 0 {
					if k := findInGroup(group, inst); k != nil {
						ref = k.C
					}
				}
				approved, acts = e.risk.Approve(inst, cur, tgt, ref, ss.holding)
			}
			// 椋庢帶鍔ㄤ綔浼樺厛锛坰top/halt 锟?鐩存帴娓呴浂锟?
			for _, a := range acts {
				if a.Type == "close" || a.Type == "halt" {
					ss.pendingTarget = &pending{target: 0, applyAt: decideApplyTs(ts, e.cfg.TradeOnNextBar, e.cfg.BarMinutes)}
				}
			}
			// 姝ｅ父璋冧粨瀹夋帓
			if ss.pendingTarget == nil {
				ss.pendingTarget = &pending{target: approved, applyAt: decideApplyTs(ts, e.cfg.TradeOnNextBar, e.cfg.BarMinutes)}
			}
		}

		// 2.5) 搴旂敤缁勫悎鏀剁泭锛堜竴娆℃€у悎骞讹級锛屽苟璁板綍
		if sumRet != 0 {
			eq *= math.Exp(sumRet)
		}
		aggRets = append(aggRets, sumRet)

		// 2.6) 鍦ㄢ€滆鏃堕棿鎴斥€濈粺涓€鎵ц鎵€鏈夎揪鍒版墽琛岀偣锟?pending锛堢敤鍚勮嚜鍝佺鏈椂鍒荤殑浠锋牸锟?
		for _, k := range group {
			st := states[k.InstID]
			if st != nil && st.pendingTarget != nil && ts >= st.pendingTarget.applyAt {
				price := refPriceForFill(e.cfg.TradeOnNextBar, k)
				e.applyFill(states, k.InstID, st.pendingTarget.target, price, ts, &trades, &eq)
				st.pendingTarget = nil
			}
		}

		// 2.7) 鏇存柊浠锋牸/鎸佷粨鍛ㄦ湡
		for _, k := range group {
			st := states[k.InstID]
			if st == nil {
				st = &instState{}
				states[k.InstID] = st
			}
			st.lastClose = k.C
			if st.pos != 0 {
				st.holding++
			} else {
				st.holding = 0
			}
		}

		// 2.8) 鍥炴挙 & 鏇茬嚎锛堟瘡涓椂闂存埑鍙涓€绗旓級
		if eq > peak {
			peak = eq
		}
		dd := (peak - eq) / (peak + 1e-12)
		if dd > maxDD {
			maxDD = dd
		}
		curve = append(curve, BarRecord{Ts: ts, Equity: eq, Ret: sumRet, Drawdown: dd})

		// 涓嬩竴锟?
		i = j
	}

	// 3) 姹囨€绘寚锟?
	res := Result{}
	res.EquityCurve = curve
	res.Trades = trades
	res.FinalEquity = eq
	res.TotalRet = eq/e.cfg.InitialEquity - 1
	years := float64(len(curve)) / barsPerYear(e.cfg.BarMinutes)
	if years > 0 {
		res.CAGR = math.Pow(eq/e.cfg.InitialEquity, 1/years) - 1
	}
	res.Sharpe = sharpe(aggRets, barAnn) // 娉ㄦ剰锛歛ggRets 鏄€滄瘡鏃堕棿姝モ€濈殑缁勫悎鏀剁泭
	res.MaxDD = maxDD
	wins := 0
	for _, tr := range trades {
		if tr.Return > 0 {
			wins++
		}
	}
	if len(trades) > 0 {
		res.WinRate = float64(wins) / float64(len(trades))
		res.NumTrades = len(trades)
	}
	return res
}

// ===================== 鍐呴儴锛氳皟浠撲笌鎴愪氦 =====================

func (e *Engine) applyFill(states map[string]*instState, inst string, target float64, refPx float64, ts int64, trades *[]Trade, eq *float64) {
	s := states[inst]
	if s == nil {
		return
	}
	cur := s.pos
	if math.Abs(target-cur) < 1e-9 {
		return
	}
	fee := e.cfg.TakerFeeBps
	if e.cfg.UseMaker {
		fee = e.cfg.MakerFeeBps
	}
	costBps := fee + e.cfg.SlippageBps

	if e.before != nil {
		px, extra := e.before(inst, sideOf(target-cur), math.Abs(target-cur), refPx)
		if px > 0 {
			refPx = px
		}
		costBps += extra
	}

	turnover := math.Abs(target - cur)
	*eq *= (1 - (turnover * costBps / 10000.0))

	closing := cur != 0 && (target == 0 || sign(target) != sign(cur))
	opening := cur == 0 && target != 0

	s.pos = target

	if closing {
		ret := sign(cur) * math.Log(refPx/(s.entryPx+1e-12)) * math.Abs(cur)
		*trades = append(*trades, Trade{
			InstID:     inst,
			Dir:        dirName(cur),
			EntryTime:  s.entryTs,
			EntryPrice: s.entryPx,
			ExitTime:   ts,
			ExitPrice:  refPx,
			Size:       math.Abs(cur),
			Return:     ret,
		})

		if target != 0 {
			s.entryPx = refPx
			s.entryTs = ts
		} else {
			s.entryPx = 0
			s.entryTs = 0
		}
	} else if opening {
		s.entryPx = refPx
		s.entryTs = ts
	}

	if e.after != nil {
		_, extraBps := e.after(inst, sideOf(target-cur), math.Abs(target-cur), refPx)
		if extraBps != 0 {
			*eq *= (1 - (turnover * extraBps / 10000.0))
		}
	}
}

func refPriceForFill(next bool, k Candle) float64 {
	if next {
		return k.O
	}
	return k.C
}
func dirName(pos float64) string {
	if pos > 0 {
		return "long"
	}
	return "short"
}
func sideOf(delta float64) string {
	if delta >= 0 {
		return "buy"
	}
	return "sell"
}

// ===================== 宸ュ叿锛氭椂闂磋酱褰掑苟涓庢寚锟?=====================

// 灏嗗搴忓垪鎷嶅钩涓烘椂闂村崌搴忥紙绋冲畾鎺掑簭锛夛紝鍚屼竴鏃堕棿鎴虫寜 InstID 鎺掑簭
func flatten(s Series) []Candle {
	var out []Candle
	for inst, arr := range s {
		// 纭繚鍗囧簭
		sorted := make([]Candle, len(arr))
		copy(sorted, arr)
		sort.SliceStable(sorted, func(i, j int) bool {
			if sorted[i].T == sorted[j].T {
				return sorted[i].InstID < sorted[j].InstID
			}
			return sorted[i].T < sorted[j].T
		})
		for i := range sorted {
			if sorted[i].InstID == "" {
				sorted[i].InstID = inst
			}
			out = append(out, sorted[i])
		}
	}
	sort.SliceStable(out, func(i, j int) bool {
		if out[i].T == out[j].T {
			return out[i].InstID < out[j].InstID
		}
		return out[i].T < out[j].T
	})
	return out
}

func findInGroup(group []Candle, inst string) *Candle {
	for i := range group {
		if group[i].InstID == inst {
			return &group[i]
		}
	}
	return nil
}

func sharpe(rets []float64, ann float64) float64 {
	if len(rets) < 30 {
		return 0
	}
	m := mean(rets)
	v := variance(rets, m)
	if v <= 0 {
		return 0
	}
	return (m / math.Sqrt(v)) * ann
}

func mean(a []float64) float64 {
	if len(a) == 0 {
		return 0
	}
	s := 0.0
	for _, x := range a {
		s += x
	}
	return s / float64(len(a))
}
func variance(a []float64, m float64) float64 {
	if len(a) <= 1 {
		return 0
	}
	s := 0.0
	for _, x := range a {
		d := x - m
		s += d * d
	}
	return s / float64(len(a)-1)
}
func barsPerYear(barMin int) float64 { return (365 * 24 * 60) / float64(maxi(1, barMin)) }
func maxi(a, b int) int {
	if a > b {
		return a
	}
	return b
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
func last(a []float64) float64 {
	if len(a) == 0 {
		return 0
	}
	return a[len(a)-1]
}

// 璁＄畻涓嬩竴鏍瑰紑鐩樻椂闂达紙浠呯敤浜庣畝鍗曟棩鍐呭洖娴嬶紱璺ㄤ氦鏄撴椂娈佃鑷畾涔夛級
func nextBarTs(ts int64, barMin int) int64 { return ts + int64(barMin)*60*1000 }
func decideApplyTs(ts int64, next bool, barMin int) int64 {
	if next {
		return nextBarTs(ts, barMin)
	}
	return ts
}
