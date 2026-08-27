"""Touch & Turn Scalper: pure decision logic, shared by the live cycle
(cycle.py) and the backtester (src/backtest_engine.py) - same "one
implementation, two callers" pattern cycle._evaluate_filters_from_bars
and orb.evaluate_orb_entry already use, so the live bot and the
backtester can never quietly drift apart.

Strategy summary: the first N-minute candle of the regular session (the
"opening candle") sometimes represents a "Liquidity Candle" - an
outsized move relative to the stock's normal daily range. When it does,
price tends to later retest the far edge of that candle's own range and
reverse - "touch" the edge, then "turn" back toward the middle. This
module identifies whether TODAY's opening candle qualifies (liquidity
gate) and, if so, which side the reversal should be faded toward (bias)
and at what price/target/stop a resting limit order should be placed to
catch it - it does NOT itself detect the touch: that's a real broker-
side limit order sitting in the market (see cycle.py's pending-order
lifecycle), not something polled for here.

Deliberately self-contained (no import of cycle.py, same reasoning as
orb.py) - the ATR helper below is an intentional near-duplicate of
cycle._compute_atr/orb._compute_atr, not a shared abstraction.

Rules shape (see EXTRA_STRATEGY_PRESETS in src/db.py for the full Touch
& Turn Long/Short presets):
    {
      "opening_candle": {"timeframe_minutes": 15, "session_open_et": "09:30"},
      "liquidity_filter": {"atr_period": 14, "atr_multiplier": 0.25},
      "fib_targets": {"long_target_pct": 38.2, "short_target_pct": 61.8},
      "reward_risk_ratio": 2.0,
      "time_filter": {"entry_window_minutes": 90},
      "risk": {"max_risk_per_trade_pct": 1.0,
               "max_position_size_pct_of_portfolio": 10,
               "max_concurrent_positions": 5},
    }
A strategy is dispatched here (instead of cycle._evaluate_filters_from_bars
or orb.evaluate_orb_entry) whenever its rules dict has an "opening_candle"
key - see cycle.entry_scan.

Direction fits the existing one-active-strategy-per-side model exactly
like every other strategy here: THIS side's own bias must match today's
actual bias for a signal to fire at all - "Touch & Turn Scalper - Long"
(direction=long) only ever acts on a red opening candle (bias=long),
"Touch & Turn Scalper - Short" (direction=short) only on a green one.
Activating both at once (one per side) covers every day - whichever
bias today's candle produces, the matching side's strategy is the one
that actually has a live setup; the other simply sees "bias mismatch"
and stays idle. No signal_side trick needed (contrast cycle.py's fade
strategies) since here the strategy's own direction already IS the
signal.

The opening candle is built by aggregating the first
opening_candle.timeframe_minutes worth of already-fetched 5-minute bars
(same OHLC-from-5m-bars approach orb.compute_opening_range uses for its
15-minute range) rather than fetching a separate 15-minute series - this
bot's data layer (both live and backtest) only ever deals in 5-minute
bars. The spec's own step 4 ("switch to a 1-minute chart" to watch for
the retest) is likewise a non-issue for an automated implementation: a
real resting limit order fills at the exact touch regardless of what
timeframe a human would be watching it on, so no 1-minute data is
fetched here either.

A close exactly equal to open (a doji) is treated as bearish (bias=long)
- an arbitrary but deterministic tie-break, since the spec doesn't
address it and a doji opening candle is a rare edge case in practice.
"""
from datetime import time as dt_time

import pandas as pd

SESSION_OPEN_TIME = dt_time(9, 30)


def compute_opening_candle(today_bars: pd.DataFrame, timeframe_minutes: int) -> dict | None:
    """Aggregates the first `timeframe_minutes` of TODAY's 5-minute bars
    into one OHLC candle. Returns None ("not enough bars yet" - normal
    before the candle has fully closed, not an error) until enough
    5-minute bars exist to cover the full window."""
    bars_needed = timeframe_minutes // 5
    candle_bars = today_bars[today_bars.index.time >= SESSION_OPEN_TIME][:bars_needed]
    if len(candle_bars) < bars_needed:
        return None
    return {
        "open": float(candle_bars["Open"].iloc[0]),
        "high": float(candle_bars["High"].max()),
        "low": float(candle_bars["Low"].min()),
        "close": float(candle_bars["Close"].iloc[-1]),
        "end_ts": candle_bars.index[-1],
    }


def _compute_atr(daily: pd.DataFrame, period: int) -> float | None:
    """Wilder's ATR as of the last COMPLETE trading day in `daily` -
    excludes daily.iloc[-1] (today, still in progress). Intentional
    near-duplicate of cycle._compute_atr/orb._compute_atr - see this
    module's own docstring for why it isn't imported instead."""
    completed = daily.iloc[:-1]
    if len(completed) < period + 1:
        return None
    high, low, close = completed["High"], completed["Low"], completed["Close"]
    prev_close = close.shift(1)
    true_range = pd.concat([
        high - low,
        (high - prev_close).abs(),
        (low - prev_close).abs(),
    ], axis=1).max(axis=1)
    value = true_range.ewm(alpha=1 / period, min_periods=period, adjust=False).mean().iloc[-1]
    return float(value) if pd.notna(value) else None


def bias_for_candle(candle: dict) -> str:
    """"long" for a red/bearish opening candle (close <= open - the
    strategy fades it upward), "short" for a green/bullish one."""
    return "long" if candle["close"] <= candle["open"] else "short"


def fib_levels(high: float, low: float) -> dict:
    """Retracement levels for a Fib drawn from the candle's high down to
    its low (0% = high, 100% = low) - fib_382 sits closer to the high,
    fib_618 closer to the low."""
    rng = high - low
    return {"fib_382": high - 0.382 * rng, "fib_618": high - 0.618 * rng}


def evaluate_touch_turn_entry(
    daily: pd.DataFrame, intraday: pd.DataFrame, rules: dict, side: str,
) -> dict:
    """The Touch & Turn decision logic - no data fetching, no wall-clock
    "now": the day being evaluated is whatever the last date in
    `intraday`'s index is, and `daily` must already end at that day's
    prior trading day (same calling convention as
    cycle._evaluate_filters_from_bars/orb.evaluate_orb_entry, so cycle.py
    and backtest_engine.py can share this one implementation).

    Returns a dict always carrying "side". On insufficient data instead
    carries "error" (mirrors the other two models' own early-return shape
    - callers should treat this as "not yet evaluable", not a failed
    check). Once evaluable, always carries "or_high"/"or_low"/
    "opening_range"/"atr_threshold"/"liquidity_ok"/"bias"/"fib_382"/
    "fib_618" (diagnostic fields, for the Watchlist table and
    filter_stats). "pass" is True only when liquidity_ok AND today's bias
    matches THIS strategy's own side - on a pass it also carries
    "limit_price" (where to rest a limit order), "target_price",
    "initial_stop" (reward/risk = rules["reward_risk_ratio"], reward
    computed off the Fib target - see this module's own docstring), and
    "reward"/"risk" (per-share dollar distances, for logging).

    This function is meant to be called ONCE per symbol per day, right
    after the opening candle closes - unlike the D1-D3/I1-I3 and ORB
    models (re-evaluated every cycle tick because "does price qualify
    right now" is inherently continuous), nothing here changes again
    once the opening candle is fixed. The caller (cycle.py) is
    responsible for that once-per-day idempotency; this function itself
    has no notion of "already placed an order today"."""
    opening_cfg = rules["opening_candle"]
    liquidity_cfg = rules["liquidity_filter"]

    if intraday.empty:
        return {"side": side, "error": "no intraday data"}
    as_of_date = intraday.index[-1].date()
    today_bars = intraday[intraday.index.date == as_of_date]
    if today_bars.empty:
        return {"side": side, "error": "no bars for today yet"}

    candle = compute_opening_candle(today_bars, opening_cfg["timeframe_minutes"])
    if candle is None:
        return {"side": side, "error": "opening candle not yet formed"}

    atr_value = _compute_atr(daily, liquidity_cfg["atr_period"])
    if atr_value is None:
        return {"side": side, "error": "not enough daily history for ATR"}

    opening_range = candle["high"] - candle["low"]
    atr_threshold = atr_value * liquidity_cfg["atr_multiplier"]
    liquidity_ok = opening_range >= atr_threshold
    bias = bias_for_candle(candle)
    fibs = fib_levels(candle["high"], candle["low"])

    detail = {
        "side": side, "or_high": candle["high"], "or_low": candle["low"],
        "opening_range": opening_range, "atr_threshold": atr_threshold,
        "liquidity_ok": liquidity_ok, "bias": bias,
        "fib_382": fibs["fib_382"], "fib_618": fibs["fib_618"],
    }

    if not liquidity_ok or bias != side:
        return {"pass": False, **detail}

    if side == "long":
        limit_price, target_price = candle["low"], fibs["fib_382"]
    else:
        limit_price, target_price = candle["high"], fibs["fib_618"]

    reward = abs(target_price - limit_price)
    risk = reward / rules["reward_risk_ratio"]
    initial_stop = (limit_price - risk) if side == "long" else (limit_price + risk)

    return {
        "pass": True, "limit_price": limit_price, "target_price": target_price,
        "initial_stop": initial_stop, "reward": reward, "risk": risk,
        **detail,
    }
