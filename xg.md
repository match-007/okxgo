【项目上下文】
- 仓库：OkxApi（Go 语言），分层架构：stream/strategy/risk/execution/portfolio/storage/backtest。
- 运行入口：main.go -> BacktestRunner，读取 backtest_config.json（若无则抓取数据），输出到 backtest_results（stats.json / equity_curve.csv / trades*.csv|json）。
- 当前配置要点：use_risk:true、use_portfolio:false、单标的（BTC）、15m 周期；权重 trend_gain=3.0、mr_gain=0.25、breakout_gain=1.0；fallback_scale=0.35（debug_fallback_force:false）；strategy_max_leverage=2.0、strategy_max_abs_position=1.5。交易频率不重要，核心目标是**年化收益↑、最大回撤（MDD）↓**。

【问题】
- 启用风险控制与现有参数仍未显著改善收益/MDD，组合权重偏向趋势，均值回归权重过低；fallback 可能拉低期望；缺少更细粒度的头寸/止损/回撤熔断与动态权重切换。

【总体目标】
在不改动系统总体结构的前提下，完成一轮“可运行的策略与风控改造”，提供代码+配置+回测产物。要求确定性、不可前视（no look-ahead）、保留既有手续费/滑点模型。

────────────────────────────────
一、配置/schema 变更（storage 下 config 结构 + backtest_config.json）
1) 在 strategy 块新增/调整字段（保持向后兼容，均有默认值）：
  - "trend_gain": float        (默认 2.0)
  - "mr_gain": float           (默认 0.7)
  - "breakout_gain": float     (默认 1.0)
  - "regime.enable": bool               (默认 true)
  - "regime.trend_adx_period": int     (默认 14)
  - "regime.trend_adx_th": float       (默认 20)
  - "regime.range_bw_period": int      (默认 20)   // 布林带宽或价格标准差窗口
  - "regime.range_bw_th": float        (默认 0.05) // 低波动阈
  - "mTF.confirm.enable": bool         (默认 true) // 多周期确认
  - "mTF.higher_tf": string            (默认 "1h")
  - "mTF.trend_align": bool            (默认 true) // 15m 趋势需与 1h 同向时放大
  - "fallback.enable": bool            (默认 true)
  - "fallback.scale": float            (默认 0.25)
  - "fallback.ma_period": int          (默认 100)

2) 在 risk 块新增/调整字段：
  - "risk_target": float         (默认 0.6)  // 目标波动/风险刻度（用于缩放仓位）
  - "atr_period": int            (默认 14)
  - "atr_stop_k": float          (默认 2.5) // 初始止损 = k*ATR
  - "atr_trail_k": float         (默认 3.0) // 追踪止盈 = k*ATR
  - "max_leverage": float        (默认 2.0)
  - "max_abs_position": float    (默认 1.5)
  - "dd_circuit.enable": bool    (默认 true)
  - "dd_circuit.threshold": float (默认 0.15) // 从历史净值峰值回撤超过阈值触发
  - "dd_circuit.cooldown_bars": int (默认 96) // 暂停/降风控的 bar 数（15m*96≈1天）

请在 storage/config.go 中补充 struct、默认值、JSON 解析与向后兼容；在 backtest_config.json 提供示例。

示例片段：
{
  "use_risk": true,
  "use_portfolio": false,
  "strategy": {
    "trend_gain": 1.8,
    "mr_gain": 0.8,
    "breakout_gain": 1.0,
    "regime": { "enable": true, "trend_adx_period": 14, "trend_adx_th": 22,
                "range_bw_period": 20, "range_bw_th": 0.06 },
    "mTF": { "confirm.enable": true, "higher_tf": "1h", "trend_align": true },
    "fallback": { "enable": true, "scale": 0.2, "ma_period": 120 }
  },
  "risk": {
    "risk_target": 0.55, "atr_period": 14,
    "atr_stop_k": 2.5, "atr_trail_k": 3.0,
    "max_leverage": 2.0, "max_abs_position": 1.5,
    "dd_circuit": { "enable": true, "threshold": 0.15, "cooldown_bars": 96 }
  }
}

────────────────────────────────
二、策略层改造（strategy/）
1) 动态权重分配（Regime Switching）：
   - 计算 ADX(14) 作为趋势强度；计算布林带带宽或收益标准差作为区间/低波动判据。
   - 规则：当 ADX>阈值 -> 强趋势：提升 trend/breakout 权重（如 *1.25~1.5），降低 mr；
           当带宽<阈值 -> 震荡：提升 mr 权重（如 *1.5），降低 trend/breakout；
           其他维持基础权重。
   - 若 mTF.confirm.enable=true：仅当 15m 与 1h 的趋势方向一致时，才允许将最终权重放大；否则降权 30~50%。

2) 信号合成到目标裸头寸 pos_raw：
   pos_raw = clamp( Σ (w_i * signal_i), -max_abs_position, +max_abs_position )
   其中 w_i 为动态权重（含 regime/mTF 调整）。

3) Fallback 逻辑收紧：
   - 仅当 |pos_raw| < 0.1 且 regime 未识别出强信号时启用；
   - fallback 为简易 MA 趋势（ma_period 可配），头寸规模 = fallback.scale * sign(price - MA)。

────────────────────────────────
三、头寸与风控（risk/）
1) 波动率缩放头寸（Vol-Targeting）：
   - 计算 ATR(atr_period) 或收益年化波动 σ。
   - 缩放系数 s = risk_target / max(ε, σ 或 ATR/price)。
   - pos_scaled = pos_raw * s，并再次限幅到 max_abs_position；同时不得使账户杠杆超过 max_leverage。

2) 以 ATR 为基础的初始止损与追踪止盈：
   - 开仓时设置 stop = entry ± atr_stop_k * ATR（多减空加）。
   - 运行中维护 trailing = max/min(既得浮盈保护, atr_trail_k*ATR)。
   - 所有止损在 execution 层以模拟委托/触发执行（不改变撮合/滑点逻辑）。

3) 回撤熔断（账户级）：
   - 维护权益峰值 E_max；若 (E_max - E_now)/E_max ≥ threshold：
       a) 将 risk_target 暂降 50% 或
       b) 直接冻结开新仓，持仓按 trailing/止损自然退出；
     冷静期 cooldown_bars 结束后自动恢复。

────────────────────────────────
四、BacktestRunner 接线（main.go / backtest/）
- 将新的 strategy/risk 输出与现有 backtest 统计对接，增加以下指标落盘：
  - 分策略归因：trend/mr/breakout/fallback 的单独 PnL、胜率、平均盈亏比
  - 波动目标达成度（实际波动 vs risk_target）
  - 回撤熔断触发次数与区间
  - 多周期过滤命中率
- 结果写入：stats.json 增补字段；trades.csv 增加列：sub_strategy、stop_type（init/trail）、atr_on_entry、regime。

────────────────────────────────
五、实验与网格搜索（backtest/optimizer 或 BacktestRunner 内部简单实现）
- 对以下参数做小型网格/随机搜索（保持样本外验证分段）：
  trend_gain ∈ {1.2,1.5,1.8,2.0}；
  mr_gain ∈ {0.5,0.7,1.0}；
  breakout_gain ∈ {0.8,1.0,1.2}；
  risk_target ∈ {0.45,0.55,0.65}；
  atr_stop_k ∈ {2.0,2.5,3.0}；
  atr_trail_k ∈ {2.5,3.0,3.5}；
  regime 阈值 ±10% 扰动。
- 产出 leaderboard.csv（按 Calmar/Sharpe/MDD 排序），并给出**基线 vs 最优**对比报告（markdown 形式），落盘到 backtest_results/。

────────────────────────────────
六、验收标准（基线为改造前同区间同费用/滑点/数据的回测结果）
- 最大回撤 MDD 下降 ≥ 20%（相对基线）。
- 年化收益不低于基线的 90%（或更高）；若能同时提升则更优。
- Calmar 或 Sortino 提升 ≥ 15%。
- 代码通过现有测试，新增关键单元测试（regime、vol-targeting、熔断）与回测集成测试（可离线跑通）。

────────────────────────────────
七、实现注意事项
- 严禁前视偏差与重绘；跨周期数据做对齐与“仅用已知历史”缓存。
- 保持手续费、滑点、最小变动单位与交易规格不变。
- 随机性（若有）需固定种子，可复现实验。
- 避免引入外部依赖；若必须，引入标准库优先，第三方需最小化并在 go.mod 中锁定版本。

【提交物】
1) 代码改动（含注释）；
2) 更新后的 backtest_config.json 示例；
3) 回测输出（stats.json、leaderboard.csv、equity_curve.csv、trades.csv/json）；
4) 一页式改造报告（markdown）：改动点、关键指标对比、失败/熔断区间剖析、后续可选改进（如多资产扩展）。

注意点，为了避免utf-8的乱码可以全英文注释