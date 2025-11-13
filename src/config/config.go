package config

// 配置（Config）层 —— 量化交易系统
//
// 设计目标：
// 1) 支持 YAML 文件 + 环境变量（ENV）覆盖；
// 2) 提供合理的默认值，开箱可用；
// 3) 加入严格校验（Validate），在启动时尽早发现问题；
// 4) 依赖轻量，仅使用 yaml.v3；
// 5) 中文注释，便于团队协作与维护。
//
// 常用环境变量（统一前缀：TRADER_）：
//   TRADER_APP_NAME=my-trader
//   TRADER_APP_ENV=dev                # dev|staging|prod
//   TRADER_APP_DATA_DIR=./data
//   TRADER_APP_TIMEZONE=Asia/Singapore
//
//   TRADER_MARKET_WS_URL=wss://ws.okx.com:8443/ws/v5/public
//   TRADER_MARKET_HTTP_URL=https://www.okx.com
//   TRADER_MARKET_SYMBOLS=BTC-USDT-SWAP,ETH-USDT-SWAP
//   TRADER_MARKET_TIMEFRAME=1m        # 1m|5m|15m|30m|1h|4h|1d
//   TRADER_MARKET_INSTTYPE=SWAP       # SPOT|SWAP|FUTURES|OPTION
//
//   TRADER_EXCHANGE_NAME=okx
//   TRADER_EXCHANGE_API_KEY=xxx
//   TRADER_EXCHANGE_SECRET_KEY=xxx
//   TRADER_EXCHANGE_PASSPHRASE=xxx
//   TRADER_EXCHANGE_BASE_URL=https://www.okx.com
//   TRADER_EXCHANGE_WS_URL=wss://ws.okx.com:8443/ws/v5/private
//   TRADER_EXCHANGE_SIMULATED=false
//
//   TRADER_RISK_MAX_LEVERAGE=5
//   TRADER_RISK_MAX_POS=2
//   TRADER_RISK_MAX_NOTIONAL=100000
//   TRADER_RISK_PX_DEVIATION_BPS=50   # 价格偏离（基点）
//   TRADER_RISK_MAX_ORDER_RATE=5      # 每秒最大下单次数
//   TRADER_RISK_MAX_OPEN_ORDERS=50
//   TRADER_RISK_KILL_SWITCH_DRAWDOWN_PCT=20
//
//   TRADER_EXECUTION_ENABLE=true
//   TRADER_EXECUTION_DRY_RUN=false
//   TRADER_EXECUTION_SLIPPAGE_BPS=5
//   TRADER_EXECUTION_RETRY_MAX_ATTEMPTS=5
//   TRADER_EXECUTION_RETRY_BACKOFF_MS=250
//
//   TRADER_PORTFOLIO_BASE_CCY=USDT
//   TRADER_PORTFOLIO_PER_SYMBOL_MAX_EXPOSURE=0.3
//
//   TRADER_LOG_LEVEL=info             # debug|info|warn|error
//   TRADER_LOG_JSON=false
//
// 示例 YAML（configs/trader.yaml）：
// ---
// app:
//   name: my-trader
//   env: dev
//   dataDir: ./data
//   timezone: Asia/Singapore
// market:
//   wsURL:  wss://ws.okx.com:8443/ws/v5/public
//   httpURL: https://www.okx.com
//   symbols: [BTC-USDT-SWAP, ETH-USDT-SWAP]
//   timeframe: 1m
//   instType: SWAP
// exchange:
//   name: okx
//   apiKey: ""
//   secretKey: ""
//   passphrase: ""
//   baseURL: https://www.okx.com
//   wsURL:   wss://ws.okx.com:8443/ws/v5/private
//   simulated: false
// risk:
//   maxLeverage: 5
//   maxPos: 2
//   maxNotional: 100000
//   priceDeviationBps: 50
//   maxOrderRatePerSec: 5
//   maxOpenOrders: 50
//   killSwitchDrawdownPct: 20
// execution:
//   enable: false
//   dryRun: true
//   slippageBps: 5
//   retry:
//     maxAttempts: 5
//     backoffMs: 250
// portfolio:
//   baseCurrency: USDT
//   perSymbolMaxExposure: 0.3
// logging:
//   level: info
//   json: false

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// 根配置结构体（导出字段便于 YAML 反序列化）
type Config struct {
	App       AppConfig       `yaml:"app"`
	Market    MarketConfig    `yaml:"market"`
	Exchange  ExchangeConfig  `yaml:"exchange"`
	Risk      RiskConfig      `yaml:"risk"`
	Execution ExecutionConfig `yaml:"execution"`
	Portfolio PortfolioConfig `yaml:"portfolio"`
	Logging   LoggingConfig   `yaml:"logging"`
}

// 应用基础配置
type AppConfig struct {
	Name     string `yaml:"name"`     // 应用名
	Env      string `yaml:"env"`      // 环境：dev|staging|prod
	DataDir  string `yaml:"dataDir"`  // 数据目录（日志/缓存/状态）
	Timezone string `yaml:"timezone"` // 时区，例如：Asia/Singapore
}

// 行情配置（市场数据）
type MarketConfig struct {
	WSURL     string   `yaml:"wsURL"`     // 公共 WS 地址（如 OKX public）
	HTTPURL   string   `yaml:"httpURL"`   // 公共 HTTP 地址（REST）
	Symbols   []string `yaml:"symbols"`   // 订阅的产品ID列表，如 [BTC-USDT-SWAP]
	Timeframe string   `yaml:"timeframe"` // K线周期：1m|5m|15m|30m|1h|4h|1d
	InstType  string   `yaml:"instType"`  // SPOT|SWAP|FUTURES|OPTION（用于约束/自检）
}

// 交易所认证/地址配置
type ExchangeConfig struct {
	Name       string `yaml:"name"`       // 交易所名称（例如 okx）
	APIKey     string `yaml:"apiKey"`     // API Key（实盘/私有WS 需要）
	SecretKey  string `yaml:"secretKey"`  // Secret Key
	Passphrase string `yaml:"passphrase"` // OKX 专用 passphrase
	BaseURL    string `yaml:"baseURL"`    // REST 基地址（私有接口）
	WSURL      string `yaml:"wsURL"`      // 私有 WS 地址（订单/账户推送）
	Simulated  bool   `yaml:"simulated"`  // 模拟盘（若交易所支持）
}

// 风控配置
type RiskConfig struct {
	MaxLeverage           float64 `yaml:"maxLeverage"`           // 最大杠杆倍数
	MaxPos                float64 `yaml:"maxPos"`                // 单品种最大持仓（张/币）
	MaxNotional           float64 `yaml:"maxNotional"`           // 单品种名义金额上限（USD）
	PriceDeviationBps     int     `yaml:"priceDeviationBps"`     // 下单价格允许偏离（基点，1bp=0.01%）
	MaxOrderRatePerSec    float64 `yaml:"maxOrderRatePerSec"`    // 每秒最大下单次数（频控）
	MaxOpenOrders         int     `yaml:"maxOpenOrders"`         // 最大挂单数
	KillSwitchDrawdownPct float64 `yaml:"killSwitchDrawdownPct"` // 回撤阈值（%），触发 “杀死开关”
}

// 执行配置（下单/撤单）
type ExecutionConfig struct {
	Enable      bool        `yaml:"enable"`      // 是否启用实盘执行（false 表示只跑策略/不下单）
	DryRun      bool        `yaml:"dryRun"`      // 干跑：即使 Enable=true 也不真下单
	SlippageBps int         `yaml:"slippageBps"` // 预估滑点（基点）
	Retry       RetryConfig `yaml:"retry"`       // 重试参数
}

type RetryConfig struct {
	MaxAttempts int `yaml:"maxAttempts"` // 最大尝试次数
	BackoffMs   int `yaml:"backoffMs"`   // 退避毫秒（线性/指数策略由执行层决定）
}

// 组合/资金配置
type PortfolioConfig struct {
	BaseCurrency         string  `yaml:"baseCurrency"`         // 记账基准货币（如 USDT）
	PerSymbolMaxExposure float64 `yaml:"perSymbolMaxExposure"` // 单品种最大资金占比（0~1）
}

// 日志配置
type LoggingConfig struct {
	Level string `yaml:"level"` // debug|info|warn|error
	JSON  bool   `yaml:"json"`  // 是否 JSON 输出
}

// ===================== 对外 API =====================

// Default 返回带默认值的配置（不包含任何敏感密钥）
func Default() Config {
	return Config{
		App: AppConfig{
			Name:     "trader",
			Env:      "dev",
			DataDir:  "./data",
			Timezone: "Asia/Singapore",
		},
		Market: MarketConfig{
			WSURL:     "wss://ws.okx.com:8443/ws/v5/public",
			HTTPURL:   "https://www.okx.com",
			Symbols:   []string{"BTC-USDT-SWAP"},
			Timeframe: "1m",
			InstType:  "SWAP",
		},
		Exchange: ExchangeConfig{
			Name:    "okx",
			BaseURL: "https://www.okx.com",
			WSURL:   "wss://ws.okx.com:8443/ws/v5/private",
		},
		Risk: RiskConfig{
			MaxLeverage:           5,
			MaxPos:                2,
			MaxNotional:           100000,
			PriceDeviationBps:     50,
			MaxOrderRatePerSec:    5,
			MaxOpenOrders:         50,
			KillSwitchDrawdownPct: 20,
		},
		Execution: ExecutionConfig{
			Enable:      false,
			DryRun:      true,
			SlippageBps: 5,
			Retry:       RetryConfig{MaxAttempts: 5, BackoffMs: 250},
		},
		Portfolio: PortfolioConfig{
			BaseCurrency:         "USDT",
			PerSymbolMaxExposure: 0.3,
		},
		Logging: LoggingConfig{
			Level: "info",
			JSON:  false,
		},
	}
}

// Load 按优先顺序读取 YAML，并应用 ENV 覆盖与校验。
// 说明：
//   - paths 为空时会尝试：./configs/trader.yaml、./config.yaml、./trader.yaml
//   - 若找不到任何文件，则仅用默认值 + 环境变量。
func Load(paths ...string) (*Config, error) {
	c := Default()

	// 搜索默认路径
	if len(paths) == 0 {
		paths = []string{
			"./configs/trader.yaml",
			"./config.yaml",
			"./trader.yaml",
		}
	}

	var used string
	for _, p := range paths {
		abs := p
		if !filepath.IsAbs(p) {
			abs, _ = filepath.Abs(p)
		}
		if fi, err := os.Stat(abs); err == nil && !fi.IsDir() {
			b, err := os.ReadFile(abs)
			if err != nil {
				return nil, fmt.Errorf("读取配置文件失败: %w", err)
			}
			if err := yaml.Unmarshal(b, &c); err != nil {
				return nil, fmt.Errorf("解析 YAML 失败: %w", err)
			}
			used = abs
			break
		}
	}
	if used != "" {
		fmt.Printf("📄 使用配置文件: %s", used)
	} else {
		fmt.Println("⚠️ 未找到配置文件，使用默认值 + 环境变量")
	}

	// 环境变量覆盖
	c.applyEnv("TRADER_")

	// 校验
	if err := c.Validate(); err != nil {
		return nil, err
	}
	return &c, nil
}

// Validate 对配置进行一致性与边界校验。
func (c *Config) Validate() error {
	if c.App.Name == "" {
		return errors.New("app.name 不能为空")
	}
	if c.App.Env == "" {
		c.App.Env = "dev"
	}
	switch strings.ToLower(c.App.Env) {
	case "dev", "staging", "prod":
	default:
		return fmt.Errorf("app.env 无效: %s (允许: dev|staging|prod)", c.App.Env)
	}
	if c.App.Timezone == "" {
		c.App.Timezone = "Asia/Singapore"
	}
	if _, err := time.LoadLocation(c.App.Timezone); err != nil {
		return fmt.Errorf("app.timezone 无效: %v", err)
	}
	if c.App.DataDir == "" {
		c.App.DataDir = "./data"
	}

	// Market
	if c.Market.WSURL == "" || c.Market.HTTPURL == "" {
		return errors.New("market.wsURL / market.httpURL 不能为空")
	}
	if len(c.Market.Symbols) == 0 {
		return errors.New("market.symbols 至少包含一个 instId")
	}
	allowedTF := map[string]bool{"1m": true, "5m": true, "15m": true, "30m": true, "1h": true, "4h": true, "1d": true}
	if !allowedTF[strings.ToLower(c.Market.Timeframe)] {
		return fmt.Errorf("market.timeframe 无效: %s", c.Market.Timeframe)
	}
	inst := strings.ToUpper(c.Market.InstType)
	if inst == "" {
		inst = "SWAP"
	}
	switch inst {
	case "SPOT", "SWAP", "FUTURES", "OPTION":
		c.Market.InstType = inst
	default:
		return fmt.Errorf("market.instType 无效: %s", c.Market.InstType)
	}

	// Exchange（当 execution.enable 且 非 dryRun 时，需要更严格）
	if c.Exchange.Name == "" {
		c.Exchange.Name = "okx"
	}
	if c.Execution.Enable && !c.Execution.DryRun {
		if c.Exchange.BaseURL == "" || c.Exchange.WSURL == "" {
			return errors.New("实盘执行需要 exchange.baseURL 与 exchange.wsURL")
		}
		if c.Exchange.APIKey == "" || c.Exchange.SecretKey == "" || c.Exchange.Passphrase == "" {
			return errors.New("实盘执行需要完整的 API 凭据(apiKey/secretKey/passphrase)")
		}
	}

	// Risk
	if c.Risk.MaxLeverage <= 0 {
		return errors.New("risk.maxLeverage 必须 > 0")
	}
	if c.Risk.MaxPos < 0 {
		return errors.New("risk.maxPos 不能为负")
	}
	if c.Risk.MaxNotional < 0 {
		return errors.New("risk.maxNotional 不能为负")
	}
	if c.Risk.PriceDeviationBps < 0 {
		return errors.New("risk.priceDeviationBps 不能为负")
	}
	if c.Risk.MaxOrderRatePerSec < 0 {
		return errors.New("risk.maxOrderRatePerSec 不能为负")
	}
	if c.Risk.MaxOpenOrders < 0 {
		return errors.New("risk.maxOpenOrders 不能为负")
	}
	if c.Risk.KillSwitchDrawdownPct < 0 || c.Risk.KillSwitchDrawdownPct > 100 {
		return errors.New("risk.killSwitchDrawdownPct 需在 0~100 之间")
	}

	// Execution
	if c.Execution.Retry.MaxAttempts < 0 {
		return errors.New("execution.retry.maxAttempts 不能为负")
	}
	if c.Execution.Retry.BackoffMs < 0 {
		return errors.New("execution.retry.backoffMs 不能为负")
	}
	if c.Execution.SlippageBps < 0 {
		return errors.New("execution.slippageBps 不能为负")
	}

	// Portfolio
	if c.Portfolio.BaseCurrency == "" {
		c.Portfolio.BaseCurrency = "USDT"
	}
	if c.Portfolio.PerSymbolMaxExposure < 0 || c.Portfolio.PerSymbolMaxExposure > 1 {
		return errors.New("portfolio.perSymbolMaxExposure 需在 0~1 之间")
	}

	// Logging
	switch strings.ToLower(c.Logging.Level) {
	case "", "debug", "info", "warn", "error":
		if c.Logging.Level == "" {
			c.Logging.Level = "info"
		}
	default:
		return fmt.Errorf("logging.level 无效: %s", c.Logging.Level)
	}
	return nil
}

// ===================== 环境变量覆盖 =====================

// applyEnv 读取以 prefix 开头的环境变量并覆盖配置。
func (c *Config) applyEnv(prefix string) {
	// App
	c.App.Name = pickStr(os.Getenv(prefix+"APP_NAME"), c.App.Name)
	c.App.Env = pickStr(os.Getenv(prefix+"APP_ENV"), c.App.Env)
	c.App.DataDir = pickStr(os.Getenv(prefix+"APP_DATA_DIR"), c.App.DataDir)
	c.App.Timezone = pickStr(os.Getenv(prefix+"APP_TIMEZONE"), c.App.Timezone)

	// Market
	c.Market.WSURL = pickStr(os.Getenv(prefix+"MARKET_WS_URL"), c.Market.WSURL)
	c.Market.HTTPURL = pickStr(os.Getenv(prefix+"MARKET_HTTP_URL"), c.Market.HTTPURL)
	if v := os.Getenv(prefix + "MARKET_SYMBOLS"); v != "" {
		c.Market.Symbols = splitCSV(v)
	}
	c.Market.Timeframe = pickStr(os.Getenv(prefix+"MARKET_TIMEFRAME"), c.Market.Timeframe)
	c.Market.InstType = pickStr(os.Getenv(prefix+"MARKET_INSTTYPE"), c.Market.InstType)

	// Exchange
	c.Exchange.Name = pickStr(os.Getenv(prefix+"EXCHANGE_NAME"), c.Exchange.Name)
	c.Exchange.APIKey = pickStr(os.Getenv(prefix+"EXCHANGE_API_KEY"), c.Exchange.APIKey)
	c.Exchange.SecretKey = pickStr(os.Getenv(prefix+"EXCHANGE_SECRET_KEY"), c.Exchange.SecretKey)
	c.Exchange.Passphrase = pickStr(os.Getenv(prefix+"EXCHANGE_PASSPHRASE"), c.Exchange.Passphrase)
	c.Exchange.BaseURL = pickStr(os.Getenv(prefix+"EXCHANGE_BASE_URL"), c.Exchange.BaseURL)
	c.Exchange.WSURL = pickStr(os.Getenv(prefix+"EXCHANGE_WS_URL"), c.Exchange.WSURL)
	c.Exchange.Simulated = pickBool(os.Getenv(prefix+"EXCHANGE_SIMULATED"), c.Exchange.Simulated)

	// Risk
	c.Risk.MaxLeverage = pickFloat(os.Getenv(prefix+"RISK_MAX_LEVERAGE"), c.Risk.MaxLeverage)
	c.Risk.MaxPos = pickFloat(os.Getenv(prefix+"RISK_MAX_POS"), c.Risk.MaxPos)
	c.Risk.MaxNotional = pickFloat(os.Getenv(prefix+"RISK_MAX_NOTIONAL"), c.Risk.MaxNotional)
	c.Risk.PriceDeviationBps = pickInt(os.Getenv(prefix+"RISK_PX_DEVIATION_BPS"), c.Risk.PriceDeviationBps)
	c.Risk.MaxOrderRatePerSec = pickFloat(os.Getenv(prefix+"RISK_MAX_ORDER_RATE"), c.Risk.MaxOrderRatePerSec)
	c.Risk.MaxOpenOrders = pickInt(os.Getenv(prefix+"RISK_MAX_OPEN_ORDERS"), c.Risk.MaxOpenOrders)
	c.Risk.KillSwitchDrawdownPct = pickFloat(os.Getenv(prefix+"RISK_KILL_SWITCH_DRAWDOWN_PCT"), c.Risk.KillSwitchDrawdownPct)

	// Execution
	c.Execution.Enable = pickBool(os.Getenv(prefix+"EXECUTION_ENABLE"), c.Execution.Enable)
	c.Execution.DryRun = pickBool(os.Getenv(prefix+"EXECUTION_DRY_RUN"), c.Execution.DryRun)
	c.Execution.SlippageBps = pickInt(os.Getenv(prefix+"EXECUTION_SLIPPAGE_BPS"), c.Execution.SlippageBps)
	c.Execution.Retry.MaxAttempts = pickInt(os.Getenv(prefix+"EXECUTION_RETRY_MAX_ATTEMPTS"), c.Execution.Retry.MaxAttempts)
	c.Execution.Retry.BackoffMs = pickInt(os.Getenv(prefix+"EXECUTION_RETRY_BACKOFF_MS"), c.Execution.Retry.BackoffMs)

	// Portfolio
	c.Portfolio.BaseCurrency = pickStr(os.Getenv(prefix+"PORTFOLIO_BASE_CCY"), c.Portfolio.BaseCurrency)
	c.Portfolio.PerSymbolMaxExposure = pickFloat(os.Getenv(prefix+"PORTFOLIO_PER_SYMBOL_MAX_EXPOSURE"), c.Portfolio.PerSymbolMaxExposure)

	// Logging
	c.Logging.Level = pickStr(os.Getenv(prefix+"LOG_LEVEL"), c.Logging.Level)
	c.Logging.JSON = pickBool(os.Getenv(prefix+"LOG_JSON"), c.Logging.JSON)
}

// ===================== 小工具函数 =====================

func pickStr(env, cur string) string {
	if strings.TrimSpace(env) != "" {
		return strings.TrimSpace(env)
	}
	return cur
}

func pickInt(env string, cur int) int {
	if strings.TrimSpace(env) == "" {
		return cur
	}
	if v, err := strconv.Atoi(strings.TrimSpace(env)); err == nil {
		return v
	}
	return cur
}

func pickFloat(env string, cur float64) float64 {
	if strings.TrimSpace(env) == "" {
		return cur
	}
	if v, err := strconv.ParseFloat(strings.TrimSpace(env), 64); err == nil {
		return v
	}
	return cur
}

func pickBool(env string, cur bool) bool {
	if strings.TrimSpace(env) == "" {
		return cur
	}
	s := strings.ToLower(strings.TrimSpace(env))
	return s == "1" || s == "true" || s == "yes" || s == "on"
}

func splitCSV(s string) []string {
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}
