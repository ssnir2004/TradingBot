# TradingView Pine Script Strategies

## `daily_sma15_crossover_strategy.pine`

A Pine Script v6 **strategy** (not an indicator) that trades a simple
Daily-timeframe 15-period SMA crossover, long-only.

### Logic

- **SMA**: 15-period Simple Moving Average of the daily close. The script
  always evaluates the SMA on the Daily timeframe via `request.security`
  (using the previous, fully-closed daily bar), so it produces the same
  signals whether the chart is displayed on a Daily, Weekly, or intraday
  timeframe. When the chart itself is already Daily, it uses the native
  `ta.sma()` series directly to avoid any unnecessary repainting.
- **Entry**: when `close` crosses **above** the 15-day SMA, a long market
  order is submitted and filled on the next bar's open (Pine's default
  order-execution model).
- **Exit**: when `close` crosses **below** the 15-day SMA, the open long
  position is closed. No short positions are ever opened.
- **Position sizing**: 100% of equity per trade (`default_qty_type =
  strategy.percent_of_equity`, `default_qty_value = 100`).
- **Initial capital**: $10,000.
- **Commission**: 0% by default (change `commission_value` in the strategy
  declaration if you want to model real broker fees).
- **Trade cap**: an input, `Max Historical Trades`, defaults to 100 — once
  100 closed trades have occurred the strategy stops opening new ones (any
  open position will still be closed normally on the exit signal).

### On-chart visuals

- The 15-day SMA is plotted as a line (teal when price is above it, orange
  when price is below it).
- Green up-triangles below the bar mark BUY signals.
- Red down-triangles above the bar mark SELL signals.
- A summary table in the top-right corner of the chart shows the live
  backtest report: Total Return (%), Net Profit ($), Number of Trades,
  Win Rate (%), Profit Factor, Max Drawdown (%), and Average Trade Return
  (%). It updates automatically as new bars close.

### How to use it in TradingView

1. Open any symbol's chart on [TradingView](https://www.tradingview.com/).
2. Open the **Pine Editor** tab at the bottom of the screen.
3. Delete the editor's default template and paste in the full contents of
   `daily_sma15_crossover_strategy.pine`.
4. Click **Add to Chart**. The chart now shows the SMA, buy/sell markers,
   and the summary table.
5. Open the **Strategy Tester** tab (bottom panel) to see TradingView's own
   full performance report — Overview, Performance Summary, List of
   Trades, and Trade Analysis — computed automatically from the strategy's
   simulated orders.
6. To adjust behavior, click the gear/settings icon on the strategy (or
   reopen "Properties" in Strategy Tester):
   - **SMA Length**: change from 15 to any other period.
   - **Max Historical Trades**: cap on the number of closed trades used for
     evaluation.
   - **Properties tab**: initial capital, order size, commission, and
     slippage can also be edited here; they mirror the values already set
     in the script (`$10,000` capital, 100% equity per trade, 0%
     commission).
7. Because `strategy()` is used (not `indicator()`), TradingView
   automatically simulates every BUY/SELL signal as real orders and the
   Strategy Tester panel populates with historical performance — no extra
   backtesting engine is required.

### Notes

- The strategy is long-only by design: no `strategy.short`/short-exit
  logic exists, so there is no possibility of the strategy opening a
  short position.
- `process_orders_on_close = true` combined with the default
  `calc_on_every_tick = false` ensures orders are evaluated once per bar
  close, matching typical end-of-day daily-SMA strategies and keeping
  backtest results consistent with live/paper trading behavior.
- Pine's crossover functions (`ta.crossover` / `ta.crossunder`) require at
  least `smaLength + 1` bars of history before they can fire, and
  `max_bars_back = 5000` is set to give `request.security` and `ta.sma`
  enough historical lookback on symbols with long price histories.
