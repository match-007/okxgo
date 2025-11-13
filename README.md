| Layer    | Responsibility                         | Example package        |
| -------- | --------------------------------------- | --------------------- |
| Stream   | Fetch real-time ticks/candles           | internal/stream       |
| Strategy | Signal generation                       | internal/strategy     |
| Risk     | Pre-trade risk checks                   | internal/risk         |
| Execution| Live order routing / order tracking     | internal/execution    |
| Portfolio| Exposure aggregation & capital control  | internal/portfolio    |
| Storage  | Market data cache / trade journal       | internal/storage      |
| Backtest | Offline simulation / historical replay  | internal/backtest     |

 