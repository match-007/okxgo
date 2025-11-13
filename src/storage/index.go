package storage

// Storage —— 数据存储层（K 线缓存 / 交易日志）
// =============================================================================
// 1) K线内存缓存：按 品种 × 周期 管理环形缓冲，线程安全，支持快照/恢复；
// 2) 交易日志：结构化 JSON Lines（.jsonl）与按日/按大小滚动；
// 3) 零第三方依赖，轻耦合轻类型，避免与上层循环依赖。

import (
	"bufio"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"
	"time"
)

// ===================== 轻量公共类型 =====================

// Candle —— 基础 K 线结构（避免与其它层循环依赖）
type Candle struct {
	InstID string  `json:"instId"`
	TF     string  `json:"tf"` // 1m/5m/15m/1h/4h/1d
	T      int64   `json:"t"`  // Unix 毫秒
	O      float64 `json:"o"`
	H      float64 `json:"h"`
	L      float64 `json:"l"`
	C      float64 `json:"c"`
	V      float64 `json:"v"`
}

// ===================== 引擎总控 =====================

type Config struct {
	DataDir string // 数据目录（日志与快照）

	// Candle 缓存设置
	MaxBarsPerSeries int           // 单序列最大缓存根数（环形缓冲容量）
	AutoPersist      bool          // 是否自动周期性落盘 CSV（快照）
	PersistInterval  time.Duration // 自动快照的间隔

	// 日志滚动设置
	LogFilename       string // 基础文件名，默认 trade.jsonl
	LogRotateDaily    bool   // 是否按日滚动（推荐：true）
	LogRotateMaxBytes int64  // 单文件最大字节，超过即 size 滚动（0 表示不限制）
}

func (c *Config) withDefaults() Config {
	q := *c
	if q.DataDir == "" {
		q.DataDir = "./data"
	}
	if q.MaxBarsPerSeries == 0 {
		q.MaxBarsPerSeries = 5000
	}
	if q.PersistInterval == 0 {
		q.PersistInterval = 5 * time.Minute
	}
	if q.LogFilename == "" {
		q.LogFilename = "trade.jsonl"
	}
	// 按注释默认开启按日滚动
	if !q.LogRotateDaily {
		q.LogRotateDaily = true
	}
	return q
}

// Engine —— 对外聚合对象：Candles + Log
type Engine struct {
	Candles *CandleStore
	Log     *TradeLogger

	wg   sync.WaitGroup
	quit chan struct{}
}

func NewEngine(cfg Config) *Engine {
	c := cfg.withDefaults()
	_ = os.MkdirAll(c.DataDir, 0o755)
	eng := &Engine{
		Candles: NewCandleStore(c.DataDir, c.MaxBarsPerSeries, c.AutoPersist, c.PersistInterval),
		Log:     NewTradeLogger(c.DataDir, c.LogFilename, c.LogRotateDaily, c.LogRotateMaxBytes),
		quit:    make(chan struct{}),
	}
	return eng
}

func (e *Engine) Close() error {
	close(e.quit)
	err1 := e.Candles.Close()
	err2 := e.Log.Close()
	if err1 != nil {
		return err1
	}
	return err2
}

// ===================== Candle 内存缓存（环形，多键） =====================

type CandleStore struct {
	mu     sync.RWMutex
	series map[string]*cSeries // key: instID|tf

	persist struct {
		dir      string
		enabled  bool
		interval time.Duration
		wg       sync.WaitGroup
		quit     chan struct{}
	}

	capacity int
}

func NewCandleStore(dir string, maxBars int, autoPersist bool, interval time.Duration) *CandleStore {
	cs := &CandleStore{
		series:   make(map[string]*cSeries),
		capacity: maxBars,
	}
	cs.persist.dir = filepath.Join(dir, "candles")
	cs.persist.enabled = autoPersist
	cs.persist.interval = interval
	cs.persist.quit = make(chan struct{})
	_ = os.MkdirAll(cs.persist.dir, 0o755)
	if autoPersist {
		cs.startAutoPersist()
	}
	return cs
}

func (s *CandleStore) key(instID, tf string) string { return instID + "|" + tf }

// Append —— 追加一根 K 线（同时间戳覆盖“最新一根”）
func (s *CandleStore) Append(k Candle) {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := s.key(k.InstID, k.TF)
	seq := s.series[key]
	if seq == nil {
		seq = newCSeries(s.capacity)
		s.series[key] = seq
	}
	seq.push(k)
}

// AppendBatch —— 批量追加（按时间升序更好）
func (s *CandleStore) AppendBatch(arr []Candle) {
	for _, k := range arr {
		s.Append(k)
	}
}

// Get —— 取最近 n 根（不足则全量），返回“时间升序”
func (s *CandleStore) Get(instID, tf string, n int) []Candle {
	s.mu.RLock()
	seq := s.series[s.key(instID, tf)]
	s.mu.RUnlock()
	if seq == nil || seq.count == 0 {
		return nil
	}
	seq.mu.RLock()
	defer seq.mu.RUnlock()
	if n <= 0 || n > seq.count {
		n = seq.count
	}
	out := make([]Candle, n)
	start := seq.count - n
	for i := 0; i < n; i++ {
		out[i] = seq.getAscUnsafe(start + i) // 在已持有锁的情况下使用 unsafe 版本
	}
	return out
}

// Last —— 取最新一根（存在返回 true）
func (s *CandleStore) Last(instID, tf string) (Candle, bool) {
	s.mu.RLock()
	seq := s.series[s.key(instID, tf)]
	s.mu.RUnlock()
	if seq == nil || seq.count == 0 {
		return Candle{}, false
	}
	seq.mu.RLock()
	defer seq.mu.RUnlock()
	return seq.getLastUnsafe(), true
}

// SnapshotCSV —— 将某序列快照到 CSV（覆盖写），含表头：instId,tf,t,o,h,l,c,v
func (s *CandleStore) SnapshotCSV(instID, tf, path string) error {
	s.mu.RLock()
	seq := s.series[s.key(instID, tf)]
	s.mu.RUnlock()
	if seq == nil || seq.count == 0 {
		return errors.New("no data")
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
	_ = w.Write([]string{"instId", "tf", "t", "o", "h", "l", "c", "v"})
	// 顺序输出（升序）
	seq.mu.RLock()
	defer seq.mu.RUnlock()
	for i := 0; i < seq.count; i++ {
		c := seq.getAscUnsafe(i)
		_ = w.Write([]string{
			c.InstID, c.TF, fmt.Sprintf("%d", c.T), fmt.Sprintf("%f", c.O), fmt.Sprintf("%f", c.H),
			fmt.Sprintf("%f", c.L), fmt.Sprintf("%f", c.C), fmt.Sprintf("%f", c.V),
		})
	}
	return nil
}

// LoadCSV —— 读取 CSV（与 SnapshotCSV 对应），并覆盖现有序列
func (s *CandleStore) LoadCSV(instID, tf, path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	r := csv.NewReader(f)
	// 略过表头
	_, _ = r.Read()
	var rows []Candle
	for {
		rec, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		if len(rec) < 8 {
			continue
		}
		var k Candle
		k.InstID = strings.TrimSpace(rec[0])
		k.TF = strings.TrimSpace(rec[1])
		fmt.Sscan(rec[2], &k.T)
		fmt.Sscan(rec[3], &k.O)
		fmt.Sscan(rec[4], &k.H)
		fmt.Sscan(rec[5], &k.L)
		fmt.Sscan(rec[6], &k.C)
		fmt.Sscan(rec[7], &k.V)
		rows = append(rows, k)
	}
	// 覆盖写入
	s.mu.Lock()
	defer s.mu.Unlock()
	seq := newCSeries(s.capacity)
	for _, k := range rows {
		seq.push(k)
	}
	s.series[s.key(instID, tf)] = seq
	return nil
}

func (s *CandleStore) startAutoPersist() {
	s.persist.wg.Add(1)
	go func() {
		defer s.persist.wg.Done()
		t := time.NewTicker(s.persist.interval)
		defer t.Stop()
		for {
			select {
			case <-s.persist.quit:
				return
			case <-t.C:
				s.mu.RLock()
				keys := make([]string, 0, len(s.series))
				for k := range s.series {
					keys = append(keys, k)
				}
				s.mu.RUnlock()
				s.persistAll(keys)
			}
		}
	}()
}

func (s *CandleStore) persistAll(keys []string) {
	for _, k := range keys {
		parts := strings.Split(k, "|")
		if len(parts) != 2 {
			continue
		}
		inst, tf := parts[0], parts[1]
		path := filepath.Join(s.persist.dir, fmt.Sprintf("%s_%s.csv", sanitize(inst), tf))
		_ = s.SnapshotCSV(inst, tf, path)
	}
}

func (s *CandleStore) Close() error {
	if s.persist.enabled {
		close(s.persist.quit)
		s.persist.wg.Wait()
	}
	return nil
}

// —— 单序列：环形缓冲 ——

// cSeries 内部必须自行加锁；提供 Unsafe 只在外层已持锁时使用。
type cSeries struct {
	mu     sync.RWMutex
	data   []Candle
	cap    int
	count  int
	idx    int
	lastTS int64 // 最新一根的时间戳（覆盖同 ts）
}

func newCSeries(capacity int) *cSeries {
	if capacity < 128 {
		capacity = 128
	}
	return &cSeries{data: make([]Candle, capacity), cap: capacity}
}

func (s *cSeries) push(k Candle) {
	s.mu.Lock()
	defer s.mu.Unlock()
	// 同时间戳：覆盖“最后一根”
	if s.count > 0 && s.lastTS == k.T {
		pos := s.writePos(0) // 修正：0 表示最后一根
		s.data[pos] = k
		return
	}
	// 正常写入
	if s.count < s.cap {
		s.data[s.count] = k
		s.count++
	} else {
		s.data[s.idx] = k
		s.idx = (s.idx + 1) % s.cap
	}
	s.lastTS = k.T
}

// writePos(offsetFromLast)：0=最后一根，-1=倒数第二根……（环形安全计算）
func (s *cSeries) writePos(offsetFromLast int) int {
	if s.count == 0 {
		return 0
	}
	if s.count < s.cap {
		return (s.count - 1) + offsetFromLast
	}
	// 环形：最后一根在 idx-1
	last := (s.idx - 1 + s.cap) % s.cap
	return (last + offsetFromLast + s.cap) % s.cap
}

func (s *cSeries) getLastUnsafe() Candle { return s.getAscUnsafe(s.count - 1) }

// getAscUnsafe(i) —— 第 i 条（从 0 最旧到 count-1 最新）；调用方需持锁
func (s *cSeries) getAscUnsafe(i int) Candle {
	if s.count == 0 {
		return Candle{}
	}
	if i < 0 {
		i = 0
	}
	if i >= s.count {
		i = s.count - 1
	}
	if s.count < s.cap {
		return s.data[i]
	}
	pos := (s.idx + i) % s.cap
	return s.data[pos]
}

// ===================== 交易日志（JSON Lines + 滚动） =====================

type TradeLogger struct {
	mu        sync.Mutex
	dir       string
	baseName  string
	file      *os.File
	writer    *bufio.Writer
	bytes     int64
	dayMark   string
	rotateDay bool
	maxBytes  int64
}

func NewTradeLogger(dir, filename string, rotateDaily bool, maxBytes int64) *TradeLogger {
	if filename == "" {
		filename = "trade.jsonl"
	}
	_ = os.MkdirAll(dir, 0o755)
	lg := &TradeLogger{dir: dir, baseName: filename, rotateDay: rotateDaily, maxBytes: maxBytes}
	_ = lg.rotateIfNeeded(true)
	return lg
}

func (t *TradeLogger) Close() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.writer != nil {
		_ = t.writer.Flush()
	}
	if t.file != nil {
		return t.file.Close()
	}
	return nil
}

// —— 统一写入入口 ——
func (t *TradeLogger) write(obj any) error {
	b, err := json.Marshal(obj)
	if err != nil {
		return err
	}
	b = append(b, '\n')
	t.mu.Lock()
	defer t.mu.Unlock()
	if err := t.rotateIfNeeded(false); err != nil {
		return err
	}
	if t.writer == nil {
		return errors.New("logger not opened")
	}
	if _, err := t.writer.Write(b); err != nil {
		return err
	}
	t.bytes += int64(len(b))
	return t.writer.Flush()
}

// —— 日志结构（可按需扩展）——

type base struct {
	TS   string `json:"ts"`
	Host string `json:"host"`
	Cat  string `json:"cat"` // 分类：signal/order/fill/action/risk/exec/info/error
}

func nowISO() string { return time.Now().UTC().Format(time.RFC3339Nano) }
func hostName() string {
	h, _ := os.Hostname()
	if h == "" {
		h = runtime.GOOS
	}
	return h
}

// 信号日志（可从策略或组合层写入）
type LogSignal struct {
	base
	InstID string         `json:"instId"`
	Tag    string         `json:"tag"`
	Dir    string         `json:"dir"` // buy/sell/close
	Size   float64        `json:"size"`
	Price  float64        `json:"price"`
	Meta   map[string]any `json:"meta,omitempty"`
}

// 风控动作日志
type LogAction struct {
	base
	InstID string         `json:"instId"`
	Type   string         `json:"type"`   // close/reduce/halt
	Reason string         `json:"reason"` // stop_loss/trailing_stop/kill_switch...
	Size   float64        `json:"size"`
	Price  float64        `json:"price"`
	Meta   map[string]any `json:"meta,omitempty"`
}

// 下单与成交
type LogOrder struct {
	base
	InstID, ClientID, Side, OrdType string
	Qty, Price                      float64
	TIF                             string
	PostOnly, ReduceOnly            bool
	Meta                            map[string]any
}

type LogFill struct {
	base
	InstID, ClientID, Side string
	Qty, Price, Fee        float64
	Meta                   map[string]any
}

// 其它信息/错误
type LogInfo struct {
	base
	Scope  string
	Fields map[string]any
}

type LogError struct {
	base
	Scope, Message string
	Fields         map[string]any
}

// —— API ——
func (t *TradeLogger) Signal(in LogSignal) error {
	in.base = base{TS: nowISO(), Host: hostName(), Cat: "signal"}
	return t.write(in)
}
func (t *TradeLogger) Action(in LogAction) error {
	in.base = base{TS: nowISO(), Host: hostName(), Cat: "action"}
	return t.write(in)
}
func (t *TradeLogger) Order(in LogOrder) error {
	in.base = base{TS: nowISO(), Host: hostName(), Cat: "order"}
	return t.write(in)
}
func (t *TradeLogger) Fill(in LogFill) error {
	in.base = base{TS: nowISO(), Host: hostName(), Cat: "fill"}
	return t.write(in)
}
func (t *TradeLogger) Info(scope string, fields map[string]any) error {
	return t.write(LogInfo{base: base{TS: nowISO(), Host: hostName(), Cat: "info"}, Scope: scope, Fields: fields})
}
func (t *TradeLogger) Error(scope, msg string, fields map[string]any) error {
	return t.write(LogError{base: base{TS: nowISO(), Host: hostName(), Cat: "error"}, Scope: scope, Message: msg, Fields: fields})
}

// Tail —— 读取最近 N 行日志（从当前活跃文件）
// 说明：实现简洁，适合中小文件；如需大文件高效 tail，可改为反向 seek 扫描。
func (t *TradeLogger) Tail(n int) ([]json.RawMessage, error) {
	if n <= 0 {
		n = 50
	}
	path := filepath.Join(t.dir, t.activeNameForDay(time.Now().Format("20060102")))
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	b, err := io.ReadAll(f)
	if err != nil {
		return nil, err
	}
	lines := strings.Split(strings.TrimSpace(string(b)), "\n")
	if len(lines) > n {
		lines = lines[len(lines)-n:]
	}
	out := make([]json.RawMessage, 0, len(lines))
	for _, ln := range lines {
		if strings.TrimSpace(ln) != "" {
			out = append(out, json.RawMessage(ln))
		}
	}
	return out, nil
}

// —— 滚动与文件管理 ——

// activeNameForDay：给定 yyyymmdd 返回活跃文件名
func (t *TradeLogger) activeNameForDay(day string) string {
	if !t.rotateDay {
		return t.baseName
	}
	base := strings.TrimSuffix(t.baseName, filepath.Ext(t.baseName))
	ext := filepath.Ext(t.baseName)
	return fmt.Sprintf("%s_%s%s", base, day, ext)
}

func (t *TradeLogger) activeName() string {
	return t.activeNameForDay(time.Now().Format("20060102"))
}

func (t *TradeLogger) rotateIfNeeded(force bool) error {
	nowDay := time.Now().Format("20060102")
	needRotate := force || t.file == nil || (t.rotateDay && nowDay != t.dayMark) || (t.maxBytes > 0 && t.bytes > t.maxBytes)

	if !needRotate {
		return nil
	}

	// 1) 若存在旧文件，必要时先对“旧日文件”执行 size 滚动
	if t.file != nil {
		// 旧文件路径使用 t.dayMark（而不是今天）
		oldPath := filepath.Join(t.dir, t.activeNameForDay(t.dayMark))
		// 关闭旧 writer/file
		if t.writer != nil {
			_ = t.writer.Flush()
		}
		_ = t.file.Close()
		t.writer = nil
		t.file = nil

		// 仅在“同一日”且达到 size 时做 size 滚动；
		// 若是跨日触发，不做旧日文件的 size 滚动（通常没必要）
		if t.maxBytes > 0 && t.bytes > t.maxBytes && nowDay == t.dayMark {
			for i := 1; ; i++ {
				cand := fmt.Sprintf("%s.%d", oldPath, i)
				if _, err := os.Stat(cand); errors.Is(err, os.ErrNotExist) {
					_ = os.Rename(oldPath, cand)
					break
				}
			}
		}
	}

	// 2) 打开“今日活跃文件”
	newPath := filepath.Join(t.dir, t.activeNameForDay(nowDay))
	if err := os.MkdirAll(t.dir, 0o755); err != nil {
		return err
	}
	f, err := os.OpenFile(newPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	t.file = f
	t.writer = bufio.NewWriterSize(f, 64*1024)
	t.bytes = fileSize(newPath)
	t.dayMark = nowDay
	return nil
}

func fileSize(path string) int64 {
	fi, err := os.Stat(path)
	if err != nil {
		return 0
	}
	return fi.Size()
}

// ===================== 工具 =====================

func sanitize(name string) string {
	repl := []string{"/", "_", "\\", "_", ":", "-", "*", "-", "?", "-", "\"", "-", "<", "-", ">", "-", "|", "-", " ", "_"}
	for i := 0; i < len(repl); i += 2 {
		name = strings.ReplaceAll(name, repl[i], repl[i+1])
	}
	return name
}

// =============== 额外：将多序列摘要导出为元信息（非必须） ===============

type SeriesMeta struct {
	InstID, TF string
	Bars       int
	From, To   time.Time
}

// Summary —— 提取内存里所有序列的简要信息（便于可视化/诊断）
func (s *CandleStore) Summary() []SeriesMeta {
	s.mu.RLock()
	defer s.mu.RUnlock()
	keys := make([]string, 0, len(s.series))
	for k := range s.series {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	out := make([]SeriesMeta, 0, len(keys))
	for _, k := range keys {
		seq := s.series[k]
		if seq == nil || seq.count == 0 {
			continue
		}
		parts := strings.Split(k, "|")
		if len(parts) != 2 {
			continue
		}
		seq.mu.RLock()
		first := seq.getAscUnsafe(0)
		last := seq.getAscUnsafe(seq.count - 1)
		seq.mu.RUnlock()
		out = append(out, SeriesMeta{
			InstID: parts[0],
			TF:     parts[1],
			Bars:   seq.count,
			From:   time.UnixMilli(first.T),
			To:     time.UnixMilli(last.T),
		})
	}
	return out
}
