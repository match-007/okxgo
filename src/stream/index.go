package stream

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

//////////////////////////////////////////////////////////////////////
// ============================ 数据结构 ============================ //
//////////////////////////////////////////////////////////////////////

// Candle —— K线（升序：旧->新）
type Candle struct {
	Timestamp int64   `json:"ts"`     // Unix ms
	Open      float64 `json:"open"`   // 开盘
	High      float64 `json:"high"`   // 最高
	Low       float64 `json:"low"`    // 最低
	Close     float64 `json:"close"`  // 收盘
	Volume    float64 `json:"volume"` // 成交量（张/币，OKX原样）
	InstID    string  `json:"instId"` // 品种
	TF        string  `json:"tf"`     // 周期：5m/15m/1h/4h
}

// WSMessage —— OKX 公共WS响应
type WSMessage struct {
	Event string          `json:"event,omitempty"`
	Arg   *WSArg          `json:"arg,omitempty"`
	Data  json.RawMessage `json:"data,omitempty"`
	Code  string          `json:"code,omitempty"`
	Msg   string          `json:"msg,omitempty"`
}

// WSArg —— 订阅参数
type WSArg struct {
	Channel string `json:"channel"`
	InstID  string `json:"instId"`
}

// TickerData —— ticker
type TickerData struct {
	InstID    string `json:"instId"`
	Last      string `json:"last"`
	BidPx     string `json:"bidPx"`
	AskPx     string `json:"askPx"`
	High24h   string `json:"high24h"`
	Low24h    string `json:"low24h"`
	Vol24h    string `json:"vol24h"`
	VolCcy24h string `json:"volCcy24h"`
	Ts        string `json:"ts"`
}

// TradeData —— 成交
type TradeData struct {
	InstID  string `json:"instId"`
	TradeID string `json:"tradeId"`
	Px      string `json:"px"`
	Sz      string `json:"sz"`
	Side    string `json:"side"`
	Ts      string `json:"ts"`
}

// BookData —— 简化行情簿
type BookData struct {
	Asks [][]string `json:"asks"`
	Bids [][]string `json:"bids"`
	Ts   string     `json:"ts"`
}

//////////////////////////////////////////////////////////////////////
// ========================= Hybrid 客户端 ========================== //
//////////////////////////////////////////////////////////////////////

type HybridClient struct {
	// ---------- WS ----------
	wsConn           *websocket.Conn
	wsURL            string
	wsSubscriptions  map[string]bool // key: channel+":"+instID（含 candle 频道）
	wsRunning        bool
	wsReconnecting   bool
	wsReconnectCount int
	maxReconnect     int

	wsWriteMu sync.Mutex
	wsCloseCh chan struct{}

	// Worker：避免在读循环里做重活
	wsMsgCh   chan []byte
	wsWorkers int
	workersWg sync.WaitGroup

	// ---------- HTTP ----------
	httpClient  *http.Client
	httpBaseURL string
	httpTimeout time.Duration

	// ---------- 缓存 ----------
	// tickerCache/tradeCache/bookCache：保持你现有接口不变
	tickerCache sync.Map
	tradeCache  sync.Map
	bookCache   sync.Map

	// candleCache：升序（旧->新），key = instID+"_"+tf
	candleCache sync.Map

	// 二级索引：最后一根闭合K的 ts，避免重复下发；key 同上
	lastClosedTs sync.Map // key: instID+"_"+tf -> int64(ts)

	// ---------- 回调 ----------
	tickerHandlers []func([]TickerData)
	tradeHandlers  []func([]TradeData)
	bookHandlers   []func(map[string][]BookData)
	candleHandlers []func([]Candle)

	// ---------- 控制 ----------
	mu   sync.RWMutex
	done chan struct{}

	// ---------- 选项 ----------
	enableCache     bool
	cacheExpiration time.Duration
	fallbackToHTTP  bool // WS不可用时，用HTTP轮询
	pollingInterval time.Duration
}

// NewHybridClient —— 复用全局默认传输栈（兼容 netboot.Init）
func NewHybridClient() *HybridClient {
	var hc *http.Client
	if dt, ok := http.DefaultTransport.(*http.Transport); ok {
		tr := dt.Clone()
		hc = &http.Client{Timeout: 10 * time.Second, Transport: tr}
	} else {
		hc = http.DefaultClient
	}

	return &HybridClient{
		// OKX 公共地址
		wsURL:        "wss://ws.okx.com:8443/ws/v5/public",
		httpBaseURL:  "https://www.okx.com",
		httpClient:   hc,
		httpTimeout:  10 * time.Second,
		wsWorkers:    4,
		maxReconnect: 10,

		wsSubscriptions: make(map[string]bool),
		done:            make(chan struct{}),

		// 缓存策略：微缓存（防止同一秒连打HTTP）
		enableCache:     true,
		cacheExpiration: 5 * time.Second,

		// 降级轮询
		fallbackToHTTP:  true,
		pollingInterval: 5 * time.Second,
	}
}

//////////////////////////////////////////////////////////////////////
// ======================== WebSocket 管理 ========================= //
//////////////////////////////////////////////////////////////////////

// ConnectWebSocket —— 幂等
func (c *HybridClient) ConnectWebSocket() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.wsRunning && c.wsConn != nil {
		return nil
	}

	dialer := *websocket.DefaultDialer // 复用全局默认（可能含代理/自定义解析）
	log.Printf("📡 连接 WebSocket: %s", c.wsURL)
	conn, _, err := dialer.Dial(c.wsURL, nil)
	if err != nil {
		return fmt.Errorf("WebSocket 连接失败: %v", err)
	}

	// 旧连接的关闭信号
	if c.wsCloseCh != nil {
		select {
		case <-c.wsCloseCh:
		default:
			close(c.wsCloseCh)
		}
	}
	c.wsCloseCh = make(chan struct{})

	c.wsConn = conn
	c.wsRunning = true
	c.wsReconnectCount = 0

	// 心跳超时刷新
	conn.SetReadDeadline(time.Now().Add(60 * time.Second))
	conn.SetPongHandler(func(string) error {
		conn.SetReadDeadline(time.Now().Add(60 * time.Second))
		return nil
	})

	// 启动 worker
	if c.wsMsgCh == nil {
		c.wsMsgCh = make(chan []byte, 1024)
		for i := 0; i < c.wsWorkers; i++ {
			c.workersWg.Add(1)
			go c.wsWorker()
		}
	}

	// 读循环 & 心跳
	go c.readWSLoop(c.wsCloseCh)
	go c.keepWSAlive(c.wsCloseCh)

	log.Println("✅ WebSocket 连接成功")
	return nil
}

func (c *HybridClient) readWSLoop(closeCh <-chan struct{}) {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("❌ WS消息处理panic: %v", r)
		}
	}()

	for {
		c.mu.RLock()
		conn := c.wsConn
		running := c.wsRunning
		c.mu.RUnlock()
		if !running || conn == nil {
			return
		}

		conn.SetReadDeadline(time.Now().Add(60 * time.Second))
		_, msg, err := conn.ReadMessage()
		if err != nil {
			// 关闭旧连接并尝试重连
			c.mu.Lock()
			if c.wsConn != nil {
				_ = c.wsConn.Close()
				c.wsConn = nil
			}
			c.mu.Unlock()

			if c.wsRunning {
				go c.reconnectWS()
			}
			time.Sleep(time.Second)
			continue
		}

		select {
		case c.wsMsgCh <- msg:
		default:
			log.Printf("⚠️ WS消息队列已满，丢弃一条")
		}

		select {
		case <-closeCh:
			return
		default:
		}
	}
}

func (c *HybridClient) wsWorker() {
	defer c.workersWg.Done()
	for msg := range c.wsMsgCh {
		var m WSMessage
		if err := json.Unmarshal(msg, &m); err != nil {
			continue
		}
		// 订阅结果
		if m.Event == "subscribe" && m.Code == "0" {
			log.Printf("✅ WS订阅成功: %+v", m.Arg)
			continue
		}
		// 数据分发
		if m.Arg != nil && m.Data != nil {
			c.handleWSData(m.Arg.Channel, m.Arg.InstID, m.Data)
		}
	}
}

func (c *HybridClient) keepWSAlive(closeCh <-chan struct{}) {
	ticker := time.NewTicker(20 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			c.mu.RLock()
			conn := c.wsConn
			running := c.wsRunning
			c.mu.RUnlock()
			if !running || conn == nil {
				return
			}
			deadline := time.Now().Add(5 * time.Second)
			c.wsWriteMu.Lock()
			err := conn.WriteControl(websocket.PingMessage, nil, deadline)
			c.wsWriteMu.Unlock()
			if err != nil {
				log.Printf("⚠️ WS心跳失败: %v", err)
				go c.reconnectWS()
			}
		case <-closeCh:
			return
		}
	}
}

// 重连 + 恢复所有订阅
func (c *HybridClient) reconnectWS() {
	c.mu.Lock()
	if !c.wsRunning {
		c.mu.Unlock()
		return
	}
	if c.wsReconnecting || c.wsReconnectCount >= c.maxReconnect {
		c.mu.Unlock()
		return
	}
	c.wsReconnecting = true
	c.wsReconnectCount++

	// 拷贝当前订阅
	subs := make([]string, 0, len(c.wsSubscriptions))
	for k := range c.wsSubscriptions {
		subs = append(subs, k)
	}
	c.mu.Unlock()

	delay := time.Duration(c.wsReconnectCount) * 2 * time.Second
	if delay > 30*time.Second {
		delay = 30 * time.Second
	}
	log.Printf("🔄 %d秒后重连WebSocket (第%d次)...", delay/time.Second, c.wsReconnectCount)
	time.Sleep(delay)

	if err := c.ConnectWebSocket(); err != nil {
		log.Printf("❌ 重连失败: %v", err)
		c.mu.Lock()
		c.wsReconnecting = false
		c.mu.Unlock()
		return
	}

	// 恢复订阅
	for _, key := range subs {
		parts := strings.SplitN(key, ":", 2)
		if len(parts) != 2 {
			continue
		}
		channel, instID := parts[0], parts[1]
		_ = c.subscribeWS(channel, []string{instID})
	}

	c.mu.Lock()
	c.wsReconnecting = false
	c.mu.Unlock()
}

//////////////////////////////////////////////////////////////////////
// =========================== WS 订阅封装 ========================== //
//////////////////////////////////////////////////////////////////////

func (c *HybridClient) SubscribeTickers(instIDs []string) error {
	return c.subscribeWS("tickers", instIDs)
}
func (c *HybridClient) SubscribeTrades(instIDs []string) error {
	return c.subscribeWS("trades", instIDs)
}

// 订阅 WS 蜡烛：channel = candle5m/candle15m/candle1H/candle4H
func (c *HybridClient) SubscribeCandlesWS(instIDs []string, timeframe string) error {
	ch := c.tfToCandleChannel(timeframe)
	return c.subscribeWS(ch, instIDs)
}

func (c *HybridClient) UnsubscribeTickers(instIDs []string) error {
	return c.unsubscribeWS("tickers", instIDs)
}
func (c *HybridClient) UnsubscribeTrades(instIDs []string) error {
	return c.unsubscribeWS("trades", instIDs)
}
func (c *HybridClient) UnsubscribeCandlesWS(instIDs []string, timeframe string) error {
	ch := c.tfToCandleChannel(timeframe)
	return c.unsubscribeWS(ch, instIDs)
}

func (c *HybridClient) ClearSubscriptions() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.wsSubscriptions = make(map[string]bool)
}

func (c *HybridClient) subscribeWS(channel string, instIDs []string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.wsConn == nil {
		return fmt.Errorf("WebSocket未连接")
	}
	args := make([]WSArg, 0, len(instIDs))
	for _, id := range instIDs {
		args = append(args, WSArg{Channel: channel, InstID: id})
		c.wsSubscriptions[channel+":"+id] = true
	}
	msg := map[string]any{"op": "subscribe", "args": args}
	return c.writeJSON(msg)
}

func (c *HybridClient) unsubscribeWS(channel string, instIDs []string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.wsConn == nil {
		return fmt.Errorf("WebSocket未连接")
	}
	args := make([]WSArg, 0, len(instIDs))
	for _, id := range instIDs {
		args = append(args, WSArg{Channel: channel, InstID: id})
		delete(c.wsSubscriptions, channel+":"+id)
	}
	msg := map[string]any{"op": "unsubscribe", "args": args}
	return c.writeJSON(msg)
}

func (c *HybridClient) writeJSON(v any) error {
	c.wsWriteMu.Lock()
	defer c.wsWriteMu.Unlock()
	if c.wsConn == nil {
		return fmt.Errorf("ws nil")
	}
	return c.wsConn.WriteJSON(v)
}

//////////////////////////////////////////////////////////////////////
// ============================ WS 数据路由 ========================= //
//////////////////////////////////////////////////////////////////////

func (c *HybridClient) handleWSData(channel, instID string, data json.RawMessage) {
	switch {
	case channel == "tickers":
		var arr []TickerData
		if err := json.Unmarshal(data, &arr); err != nil {
			return
		}
		for _, t := range arr {
			c.tickerCache.Store(t.InstID, t)
		}
		c.dispatchTicker(arr)

	case channel == "trades":
		var arr []TradeData
		if err := json.Unmarshal(data, &arr); err != nil {
			return
		}
		c.dispatchTrade(arr)

	case strings.HasPrefix(channel, "books"):
		var arr []BookData
		if err := json.Unmarshal(data, &arr); err != nil {
			return
		}
		c.dispatchBook(map[string][]BookData{"default": arr})

	case strings.HasPrefix(channel, "candle"):
		// OKX candle: data: [[ts, o, h, l, c, vol, volCcy, volCcyQuote, confirm], ...]
		var rows [][]string
		if err := json.Unmarshal(data, &rows); err != nil {
			return
		}
		tf := c.channelToTF(channel)
		if tf == "" {
			return
		}
		c.onCandleWS(instID, tf, rows)
	}
}

//////////////////////////////////////////////////////////////////////
// ============================ HTTP 接口 =========================== //
//////////////////////////////////////////////////////////////////////

// GetCandles —— HTTP拉取并升序返回“最新 limit 根”（自动分页 & 去重）
func (c *HybridClient) GetCandles(instID, timeframe string, limit int) ([]Candle, error) {
	if limit <= 0 {
		limit = 300
	}
	cacheKey := instID + "_" + timeframe

	// 缓存命中（足量才直接返回）
	if c.enableCache {
		if v, ok := c.candleCache.Load(cacheKey); ok {
			cached := v.([]Candle) // 升序
			if len(cached) >= limit {
				return cached[len(cached)-limit:], nil
			}
		}
	}

	// 分页
	bar := c.tfToBarParam(timeframe)
	const (
		maxPerCandles   = 300
		maxTotalCandles = 1440
		maxPerHistory   = 100
	)

	var (
		allRows [][]string // 原始OKX行，均为新->旧
		after   string
	)

	// 先 market/candles（最近）
	wantRecent := limit
	if wantRecent > maxTotalCandles {
		wantRecent = maxTotalCandles
	}
	for len(allRows) < wantRecent {
		per := maxPerCandles
		if need := wantRecent - len(allRows); need < per {
			per = need
		}
		api := fmt.Sprintf("%s/api/v5/market/candles?instId=%s&bar=%s&limit=%d",
			c.httpBaseURL, instID, bar, per)
		if after != "" {
			api += "&after=" + after
		}
		rows, err := c.doOKXCandlesRequest(api)
		if err != nil {
			return nil, err
		}
		if len(rows) == 0 {
			break
		}
		allRows = append(allRows, rows...)
		after = rows[len(rows)-1][0]
		if len(rows) < per {
			break
		}
		time.Sleep(120 * time.Millisecond)
	}

	// 不够则 history-candles
	for len(allRows) < limit {
		per := maxPerHistory
		if need := limit - len(allRows); need < per {
			per = need
		}
		api := fmt.Sprintf("%s/api/v5/market/history-candles?instId=%s&bar=%s&limit=%d",
			c.httpBaseURL, instID, bar, per)
		if after != "" {
			api += "&after=" + after
		}
		rows, err := c.doOKXCandlesRequest(api)
		if err != nil {
			return nil, err
		}
		if len(rows) == 0 {
			break
		}
		allRows = append(allRows, rows...)
		after = rows[len(rows)-1][0]
		if len(rows) < per {
			break
		}
		time.Sleep(120 * time.Millisecond)
	}

	if len(allRows) == 0 {
		return nil, fmt.Errorf("返回数据为空：%s %s", instID, timeframe)
	}

	// 解析 + 升序 + 去重（同 ts 后写覆盖前写）
	candles := parseOKXRowsToCandlesAsc(allRows, instID, timeframe)

	// 仅保留最后 limit 根（升序尾部）
	if len(candles) > limit {
		candles = candles[len(candles)-limit:]
	}

	// 写缓存（微缓存）
	if c.enableCache {
		c.candleCache.Store(cacheKey, candles)
		time.AfterFunc(c.cacheExpiration, func() { c.candleCache.Delete(cacheKey) })
	}

	return candles, nil
}

func (c *HybridClient) doOKXCandlesRequest(apiURL string) ([][]string, error) {
	type okxResp struct {
		Code string     `json:"code"`
		Msg  string     `json:"msg"`
		Data [][]string `json:"data"`
	}
	ctx, cancel := context.WithTimeout(context.Background(), c.httpTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, apiURL, nil)
	if err != nil {
		return nil, err
	}
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("HTTP请求失败: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return nil, fmt.Errorf("HTTP状态码=%d, body=%s", resp.StatusCode, string(b))
	}
	var result okxResp
	b, _ := io.ReadAll(resp.Body)
	if err := json.Unmarshal(b, &result); err != nil {
		return nil, fmt.Errorf("解析JSON失败: %v", err)
	}
	if result.Code != "0" {
		return nil, fmt.Errorf("API错误: code=%s, msg=%s", result.Code, result.Msg)
	}
	return result.Data, nil
}

//////////////////////////////////////////////////////////////////////
// ============================= 业务层 ============================ //
//////////////////////////////////////////////////////////////////////

// SubscribeCandles —— “历史回补 + WS闭合增量” 的统一入口
// 说明：为了上层简单，仍保留原方法签名
func (c *HybridClient) SubscribeCandles(instIDs []string, timeframe string) error {
	// 1) 回补：先拉一段历史，初始化 UI/策略
	backfill := 300
	if timeframe == "1h" || timeframe == "4h" {
		backfill = 400 // 较长周期多回一点
	}
	for _, id := range instIDs {
		rows, err := c.GetCandles(id, timeframe, backfill)
		if err != nil {
			return err
		}
		cacheKey := id + "_" + timeframe
		c.candleCache.Store(cacheKey, rows)

		// 记住最后一根闭合 ts（升序尾部）
		if len(rows) > 0 {
			c.lastClosedTs.Store(cacheKey, rows[len(rows)-1].Timestamp)
			// 首次回调：把历史发出去（可选：仅发尾部几根）
			c.dispatchCandle(rows)
		}
	}

	// 2) 订阅 WS 蜡烛（闭合增量）
	if err := c.SubscribeCandlesWS(instIDs, timeframe); err != nil {
		log.Printf("⚠️ WS订阅失败，启动HTTP轮询模式: %v", err)
		if c.fallbackToHTTP {
			go c.startHTTPPolling(instIDs, timeframe)
			return nil
		}
		return err
	}

	return nil
}

// HTTP 降级轮询：定期拉最后 N 根，做去重 + 增量下发
func (c *HybridClient) startHTTPPolling(instIDs []string, timeframe string) {
	t := time.NewTicker(c.pollingInterval)
	defer t.Stop()
	log.Printf("🔄 启动HTTP轮询：%s %s, 间隔%v", strings.Join(instIDs, ","), timeframe, c.pollingInterval)
	for {
		select {
		case <-t.C:
			for _, id := range instIDs {
				rows, err := c.GetCandles(id, timeframe, 20)
				if err != nil || len(rows) == 0 {
					continue
				}
				c.mergeAndDispatch(id, timeframe, rows)
			}
		case <-c.done:
			return
		}
	}
}

// WS蜡烛处理：仅在 confirm=="1" 时合入缓存并下发
func (c *HybridClient) onCandleWS(instID, timeframe string, rows [][]string) {
	if len(rows) == 0 {
		return
	}
	cacheKey := instID + "_" + timeframe

	// 把 confirm==1 的行转 Candle（单批可能有多根）
	closed := make([]Candle, 0, len(rows))
	for _, it := range rows {
		// 严格校验长度：OKX 至少 [ts,o,h,l,c,vol,volCcy,volQuote,confirm]
		if len(it) < 6 {
			continue
		}
		confirm := lastOr(it, len(it)-1)
		if confirm != "1" {
			continue // 只处理闭合K
		}
		ts, _ := strconv.ParseInt(it[0], 10, 64)
		o, _ := strconv.ParseFloat(it[1], 64)
		h, _ := strconv.ParseFloat(it[2], 64)
		l, _ := strconv.ParseFloat(it[3], 64)
		cx, _ := strconv.ParseFloat(it[4], 64)
		vol, _ := strconv.ParseFloat(it[5], 64)

		closed = append(closed, Candle{
			Timestamp: ts, Open: o, High: h, Low: l, Close: cx, Volume: vol, InstID: instID, TF: timeframe,
		})
	}
	if len(closed) == 0 {
		return
	}

	// 升序合并 + 去重 + 增量下发
	// 1) 取已有缓存
	var base []Candle
	if v, ok := c.candleCache.Load(cacheKey); ok {
		base = v.([]Candle)
	}
	// 2) 合并：把 closed 逐个覆盖（按 ts）
	merged := mergeCandlesAsc(base, closed)

	// 3) 找出“真正新闭合”的增量（基于 lastClosedTs）
	lastTs := int64(0)
	if v, ok := c.lastClosedTs.Load(cacheKey); ok {
		lastTs, _ = v.(int64)
	}
	incr := tailAfterTs(merged, lastTs)

	// 4) 落缓存 + 下发
	if len(merged) > 0 {
		c.candleCache.Store(cacheKey, merged)
		c.lastClosedTs.Store(cacheKey, merged[len(merged)-1].Timestamp)
	}
	if len(incr) > 0 {
		c.dispatchCandle(incr)
	}
}

// 合并并下发（用于 HTTP 轮询增量）
func (c *HybridClient) mergeAndDispatch(instID, timeframe string, fresh []Candle) {
	if len(fresh) == 0 {
		return
	}
	cacheKey := instID + "_" + timeframe

	// 现有缓存
	var base []Candle
	if v, ok := c.candleCache.Load(cacheKey); ok {
		base = v.([]Candle)
	}

	// 覆盖合并（按 ts）
	merged := mergeCandlesAsc(base, fresh)

	// 增量
	lastTs := int64(0)
	if v, ok := c.lastClosedTs.Load(cacheKey); ok {
		lastTs, _ = v.(int64)
	}
	incr := tailAfterTs(merged, lastTs)

	// 更新并下发
	if len(merged) > 0 {
		c.candleCache.Store(cacheKey, merged)
		c.lastClosedTs.Store(cacheKey, merged[len(merged)-1].Timestamp)
	}
	if len(incr) > 0 {
		c.dispatchCandle(incr)
	}
}

//////////////////////////////////////////////////////////////////////
// ============================== 回调 ============================= //
//////////////////////////////////////////////////////////////////////

func (c *HybridClient) OnTicker(handler func([]TickerData)) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.tickerHandlers = append(c.tickerHandlers, handler)
}
func (c *HybridClient) OnTrade(handler func([]TradeData)) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.tradeHandlers = append(c.tradeHandlers, handler)
}
func (c *HybridClient) OnBook(handler func(map[string][]BookData)) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.bookHandlers = append(c.bookHandlers, handler)
}
func (c *HybridClient) OnCandle(handler func([]Candle)) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.candleHandlers = append(c.candleHandlers, handler)
}

func (c *HybridClient) dispatchTicker(arr []TickerData) {
	c.mu.RLock()
	hs := append([]func([]TickerData){}, c.tickerHandlers...)
	c.mu.RUnlock()
	for _, h := range hs {
		h(arr)
	}
}
func (c *HybridClient) dispatchTrade(arr []TradeData) {
	c.mu.RLock()
	hs := append([]func([]TradeData){}, c.tradeHandlers...)
	c.mu.RUnlock()
	for _, h := range hs {
		h(arr)
	}
}
func (c *HybridClient) dispatchBook(m map[string][]BookData) {
	c.mu.RLock()
	hs := append([]func(map[string][]BookData){}, c.bookHandlers...)
	c.mu.RUnlock()
	for _, h := range hs {
		h(m)
	}
}
func (c *HybridClient) dispatchCandle(arr []Candle) {
	c.mu.RLock()
	hs := append([]func([]Candle){}, c.candleHandlers...)
	c.mu.RUnlock()
	for _, h := range hs {
		h(arr)
	}
}

//////////////////////////////////////////////////////////////////////
// ============================== 工具 ============================= //
//////////////////////////////////////////////////////////////////////

// 时间框转换
func (c *HybridClient) tfToBarParam(tf string) string {
	switch tf {
	case "1m":
		return "1m"
	case "5m":
		return "5m"
	case "15m":
		return "15m"
	case "30m":
		return "30m"
	case "1h":
		return "1H"
	case "4h":
		return "4H"
	case "1d":
		return "1D"
	default:
		return "5m"
	}
}
func (c *HybridClient) tfToCandleChannel(tf string) string {
	switch tf {
	case "1m":
		return "candle1m"
	case "5m":
		return "candle5m"
	case "15m":
		return "candle15m"
	case "30m":
		return "candle30m"
	case "1h":
		return "candle1H"
	case "4h":
		return "candle4H"
	case "1d":
		return "candle1D"
	default:
		return "candle5m"
	}
}
func (c *HybridClient) channelToTF(ch string) string {
	switch ch {
	case "candle1m":
		return "1m"
	case "candle5m":
		return "5m"
	case "candle15m":
		return "15m"
	case "candle30m":
		return "30m"
	case "candle1H":
		return "1h"
	case "candle4H":
		return "4h"
	case "candle1D":
		return "1d"
	default:
		return ""
	}
}

// 解析 OKX rows（新->旧）为升序 Candle（旧->新）
func parseOKXRowsToCandlesAsc(rows [][]string, instID, tf string) []Candle {
	out := make([]Candle, 0, len(rows))
	seen := make(map[int64]struct{}, len(rows))
	for i := len(rows) - 1; i >= 0; i-- {
		it := rows[i]
		if len(it) < 6 {
			continue
		}
		ts, _ := strconv.ParseInt(it[0], 10, 64)
		if _, ok := seen[ts]; ok {
			continue
		}
		o, _ := strconv.ParseFloat(it[1], 64)
		h, _ := strconv.ParseFloat(it[2], 64)
		l, _ := strconv.ParseFloat(it[3], 64)
		cx, _ := strconv.ParseFloat(it[4], 64)
		vol, _ := strconv.ParseFloat(it[5], 64)
		out = append(out, Candle{
			Timestamp: ts, Open: o, High: h, Low: l, Close: cx, Volume: vol, InstID: instID, TF: tf,
		})
		seen[ts] = struct{}{}
	}
	return out
}

// 合并两个升序切片（b 覆盖 a，按 ts 覆盖），返回升序
func mergeCandlesAsc(a, b []Candle) []Candle {
	if len(a) == 0 {
		// 确保 b 是升序（调用方保证；保险起见可以再排一下）
		return dedupAsc(b)
	}
	if len(b) == 0 {
		return a
	}
	// 建索引（a）
	idx := make(map[int64]int, len(a))
	for i, k := range a {
		idx[k.Timestamp] = i
	}
	out := make([]Candle, 0, len(a)+len(b))
	out = append(out, a...)

	// 覆盖或追加 b
	for _, k := range b {
		if pos, ok := idx[k.Timestamp]; ok {
			out[pos] = k
		} else {
			out = append(out, k)
		}
	}
	// 最后做一次去重+升序（基于 ts）
	return dedupAsc(out)
}

// 去重并按 ts 升序（稳定）
func dedupAsc(in []Candle) []Candle {
	if len(in) <= 1 {
		return in
	}
	// 简洁：先按 ts 排，再覆盖去重
	type kv struct {
		ts int64
		i  int
	}
	tmp := make([]kv, len(in))
	for i := range in {
		tmp[i] = kv{ts: in[i].Timestamp, i: i}
	}
	// 简单插入排序（N 通常不大；若担心可换 sort.Slice）
	for i := 1; i < len(tmp); i++ {
		j := i
		for j > 0 && tmp[j-1].ts > tmp[j].ts {
			tmp[j-1], tmp[j] = tmp[j], tmp[j-1]
			j--
		}
	}
	out := make([]Candle, 0, len(in))
	var lastTs int64 = -1
	for _, p := range tmp {
		k := in[p.i]
		if k.Timestamp == lastTs {
			// 同 ts 后写覆盖前写：保留最后遇到的
			out[len(out)-1] = k
			continue
		}
		out = append(out, k)
		lastTs = k.Timestamp
	}
	return out
}

// 取 merged 中“最后一个 > lastTs”的尾部增量（升序）
func tailAfterTs(merged []Candle, lastTs int64) []Candle {
	if len(merged) == 0 {
		return nil
	}
	lo, hi := 0, len(merged)-1
	pos := len(merged) // 默认全不大于
	for lo <= hi {
		m := (lo + hi) >> 1
		if merged[m].Timestamp > lastTs {
			pos = m
			hi = m - 1
		} else {
			lo = m + 1
		}
	}
	if pos >= 0 && pos < len(merged) {
		return merged[pos:]
	}
	return nil
}

func lastOr(a []string, i int) string {
	if i >= 0 && i < len(a) {
		return strings.TrimSpace(a[i])
	}
	return ""
}

//////////////////////////////////////////////////////////////////////
// ============================ 运行控制 ============================ //
//////////////////////////////////////////////////////////////////////

// IsConnected —— WS是否连接
func (c *HybridClient) IsConnected() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.wsRunning && c.wsConn != nil
}

// Close —— 释放
func (c *HybridClient) Close() {
	c.mu.Lock()
	if !c.wsRunning {
		c.mu.Unlock()
		return
	}
	c.wsRunning = false
	if c.wsCloseCh != nil {
		select {
		case <-c.wsCloseCh:
		default:
			close(c.wsCloseCh)
		}
	}
	if c.wsConn != nil {
		_ = c.wsConn.Close()
		c.wsConn = nil
	}
	if c.done != nil {
		select {
		case <-c.done:
		default:
			close(c.done)
		}
	}
	// 关闭 worker
	if c.wsMsgCh != nil {
		close(c.wsMsgCh)
		c.wsMsgCh = nil
	}
	c.mu.Unlock()

	c.workersWg.Wait()
	log.Println("👋 混合客户端已关闭")
}
