package strategy

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"
)

// SignalLogger 把交易信号写入到 “策略日志/币种_YYYY-MM-DD.log”
//   - 多行格式（含小图标），如：
//     🕒 时间:2025-10-27T12:34:56.789Z
//     🪙 币种:BTCUSDT
//     🧭 方向:买入
//     📦 数量:0.50
//     💵 价格:65000.1
//     🏷 标签:止盈一半(tp_half)
//     📋 细节:
//     原因:止盈一半
//     目标仓位:0.80
//     增量Δ:0.30
//     预计成本:0.0006
//     夏普比:1.23
//     （条目间以空行分隔）
//
// - 每天自动换新文件（按本地日期）
// - 并发安全
type SignalLogger struct {
	baseDir     string
	mu          sync.Mutex
	files       map[string]*os.File // key: InstID
	paths       map[string]string   // key: InstID -> current file path
	currentDate string              // YYYY-MM-DD
}

func NewSignalLogger(baseDir string) *SignalLogger {
	if baseDir == "" {
		baseDir = "策略日志"
	}
	return &SignalLogger{
		baseDir:     baseDir,
		files:       make(map[string]*os.File),
		paths:       make(map[string]string),
		currentDate: time.Now().Format("2006-01-02"),
	}
}

func (l *SignalLogger) rotateIfNeeded(now time.Time) {
	date := now.Format("2006-01-02")
	if date == l.currentDate {
		return
	}
	// 日期变更 -> 关闭所有已打开文件
	for inst, f := range l.files {
		_ = f.Close()
		delete(l.files, inst)
		delete(l.paths, inst)
	}
	l.currentDate = date
}

func (l *SignalLogger) fileFor(inst string, now time.Time) (*os.File, error) {
	if err := os.MkdirAll(l.baseDir, 0o755); err != nil {
		return nil, err
	}
	fileName := fmt.Sprintf("%s_%s.log", inst, now.Format("2006-01-02"))
	path := filepath.Join(l.baseDir, fileName)

	// 同一币种且同一日期直接复用文件句柄
	if f, ok := l.files[inst]; ok && l.paths[inst] == path {
		return f, nil
	}
	// 关闭旧文件
	if f, ok := l.files[inst]; ok {
		_ = f.Close()
		delete(l.files, inst)
		delete(l.paths, inst)
	}

	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, err
	}
	l.files[inst] = f
	l.paths[inst] = path
	return f, nil
}

// 单例
var (
	loggerOnce    sync.Once
	defaultLogger *SignalLogger
)

func getLogger() *SignalLogger {
	loggerOnce.Do(func() {
		defaultLogger = NewSignalLogger("策略日志")
	})
	return defaultLogger
}

// DefaultSignalLogger exposes the shared logger for external packages.
func DefaultSignalLogger() *SignalLogger { return getLogger() }

// LogSignal is a helper for logging without manually grabbing the logger.
func LogSignal(sig Signal) error { return getLogger().LogSignal(sig) }

// LogSignal 写入一条「多行文本」日志（含小图标）
func (l *SignalLogger) LogSignal(sig Signal) error {
	now := time.Now()
	l.mu.Lock()
	defer l.mu.Unlock()

	l.rotateIfNeeded(now)
	f, err := l.fileFor(sig.InstID, now)
	if err != nil {
		return err
	}

	block := l.formatSignal(now, sig)
	_, err = f.WriteString(block)
	return err
}

// Close 可选：进程退出时调用
func (l *SignalLogger) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	var err error
	for inst, f := range l.files {
		if e := f.Close(); e != nil {
			err = e
		}
		delete(l.files, inst)
		delete(l.paths, inst)
	}
	return err
}

// ===== 中文化 / 展示工具 =====

func (l *SignalLogger) formatSignal(now time.Time, sig Signal) string {
	// 头部主键：不平铺，逐行“键:值”，加 emoji
	var b bytes.Buffer
	timeStr := now.Format(time.RFC3339Nano)

	tagCN := tagToCN(sig.Tag)
	tagLine := tagCN
	if tagCN != sig.Tag && sig.Tag != "" {
		tagLine = fmt.Sprintf("%s(%s)", tagCN, sig.Tag)
	}

	fmt.Fprintf(&b, "🕒 时间:%s\n", timeStr)
	fmt.Fprintf(&b, "🪙 币种:%s\n", sig.InstID)
	fmt.Fprintf(&b, "🧭 方向:%s\n", sideToCN(sig.Side))
	fmt.Fprintf(&b, "📦 数量:%g\n", sig.Size)
	fmt.Fprintf(&b, "💵 价格:%g\n", sig.Price)
	fmt.Fprintf(&b, "🏷 标签:%s\n", tagLine)

	// 细节段：先打印标题行“📋 细节:”，下一行起逐条 key:value
	metaCN := metaToCN(sig.Meta, sig.Tag, sig.Side)

	fmt.Fprintf(&b, "📋 细节:\n")

	// 固定顺序优先
	order := []string{"原因", "事件", "目标仓位", "增量Δ", "预计成本", "夏普比", "平仓盈亏"}
	already := map[string]bool{}
	for _, k := range order {
		if v, ok := metaCN[k]; ok {
			fmt.Fprintf(&b, "%s:%v\n", k, v)
			already[k] = true
		}
	}
	// 其余键按字典序
	rest := make([]string, 0, len(metaCN))
	for k := range metaCN {
		if !already[k] {
			rest = append(rest, k)
		}
	}
	sort.Strings(rest)
	for _, k := range rest {
		fmt.Fprintf(&b, "%s:%v\n", k, metaCN[k])
	}

	// 分隔空行
	b.WriteString("\n")
	return b.String()
}

func sideToCN(s string) string {
	switch s {
	case "buy":
		return "买入"
	case "sell":
		return "卖出"
	case "close":
		return "平仓"
	default:
		return s
	}
}

func tagToCN(tag string) string {
	switch tag {
	case "tp_half":
		return "止盈一半"
	case "risk":
		return "风控"
	case "quantmaster_elite_v4.3":
		return "量化大师v4.3"
	default:
		return tag
	}
}

func reasonToCN(v any) any {
	str, ok := v.(string)
	if !ok {
		return v
	}
	switch str {
	case "take_profit_half":
		return "止盈一半"
	case "exit_threshold":
		return "阈值退出"
	case "stop_loss":
		return "止损"
	case "trailing_stop":
		return "跟踪止损"
	default:
		return str
	}
}

func eventToCN(v any) any {
	str, ok := v.(string)
	if !ok {
		return v
	}
	switch str {
	case "rebalance":
		return "再平衡"
	default:
		return str
	}
}

// 把 Meta 的键名/常见值中文化；其余键原样透传
func metaToCN(meta map[string]any, tag string, side string) map[string]any {
	if meta == nil {
		return map[string]any{}
	}
	out := make(map[string]any, len(meta)+2)
	for k, v := range meta {
		switch k {
		case "event":
			out["事件"] = eventToCN(v)
		case "reason":
			out["原因"] = reasonToCN(v)
		case "target":
			out["目标仓位"] = v
		case "delta":
			out["增量Δ"] = v
		case "cost":
			out["预计成本"] = v
		case "sharpe":
			out["夏普比"] = v
		case "pnl":
			out["平仓盈亏"] = v
		default:
			out[k] = v
		}
	}
	// 附加中文动作/标签快照（不影响排序权重）
	out["_动作"] = sideToCN(side)
	out["_标签"] = tagToCN(tag)
	return out
}
