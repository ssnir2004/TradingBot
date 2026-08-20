"""Historical backtest for the "Short Parabolic Reversal" strategy preset
(see EXTRA_STRATEGY_PRESETS in src/db.py) against real yfinance data.

Standalone dev tool, not part of the always-on trading engine (run_service.py
never imports this). It replays the exact D1-D3/I1-I3 filter math that
cycle.py's _evaluate_entry_filters uses in real time, bar-by-bar over
historical 5-minute data, to find every day a short signal would have fired,
then simulates the exit rules (breakeven flip / partial profit / swing-high
trailing stop / daily force-close) from cycle.py's manage_position to score
each one. Imports the real functions from cycle.py/src/db.py rather than
re-implementing the math, so this can't silently drift from what the live
bot actually does.

This bot force-closes every position before the close every day (see
cycle.py's force_close_all) — it's a day-trading engine, not a multi-day
swing short. So catching a multi-day decline here means catching each day's
intraday breakdown separately: one trade per day the filters fire, not one
position held across the whole move.

Data limits (yfinance free tier): daily history goes back years, but 5-minute
intraday history is capped at roughly the last 60 days, so signals can only
be found inside that window.

Usage:
    python backtest_short_parabolic.py MRNA
    python backtest_short_parabolic.py MRNA --strategy "Short Breakdown Conservative"
"""
import argparse
import sys
from datetime import time as dt_time
from pathlib import Path

import pandas as pd
import yfinance as yf

sys.path.insert(0, str(Path(__file__).resolve().parent))
from cycle import _compute_rsi, _find_latest_swing_high  # reuse production math verbatim
from src.db import EXTRA_STRATEGY_PRESETS

ET = "America/New_York"
FORCE_CLOSE_TIME = dt_time(15, 51)


def load_strategy(name: str) -> dict:
    for preset_name, rules, _risk_rating, _direction in EXTRA_STRATEGY_PRESETS:
        if preset_name == name:
            return rules
    available = ", ".join(p[0] for p in EXTRA_STRATEGY_PRESETS if p[3] == "short")
    raise SystemExit(f"unknown strategy {name!r}. Available short presets: {available}")


def fetch_data(symbol: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    daily = yf.Ticker(symbol).history(period="2y", interval="1d")
    intraday = yf.Ticker(symbol).history(period="60d", interval="5m", prepost=True)
    if not intraday.empty:
        intraday.index = intraday.index.tz_convert(ET)
    return daily, intraday


def find_signals(daily: pd.DataFrame, intraday: pd.DataFrame, rules: dict) -> list[dict]:
    """Replays cycle.py's short-side D1-D3/I1-I3 filter logic bar-by-bar over
    historical data. Returns at most one signal per calendar day (matching
    entry_scan, which stops scanning a symbol once it's held)."""
    daily_filters = rules["daily_filters"]
    intraday_filters = rules["intraday_filters"]
    time_filter = rules["time_filter"]
    earliest = dt_time.fromisoformat(time_filter["earliest_entry_et"])
    latest = dt_time.fromisoformat(time_filter["latest_entry_et"])
    lookback = intraday_filters["I3_rvol_lookback_days"]

    signals = []
    dates = sorted({ts.date() for ts in intraday.index})
    for date in dates:
        day_bars = intraday[intraday.index.date == date]
        if day_bars.empty:
            continue

        prior_daily = daily[daily.index.date < date]
        if len(prior_daily) < 201:
            continue  # not enough history for a stable SMA yet

        prior_day = prior_daily.iloc[-1]
        prior_close = float(prior_day["Close"])
        sma50 = prior_daily["Close"].iloc[-50:].mean()
        ext_pct = (prior_close - float(sma50)) / float(sma50) * 100 if sma50 else 0.0
        if ext_pct < daily_filters["D2_prior_close_pct_above_sma50_min"]:
            continue  # D2 is day-level (depends only on yesterday's close) -> skip the whole day early

        avg_daily_volume = float(prior_daily["Volume"].iloc[-lookback:].mean())

        premarket_bars = day_bars[day_bars.index.time < dt_time(9, 30)]
        regular_bars = day_bars[day_bars.index.time >= dt_time(9, 30)]
        if regular_bars.empty:
            continue
        premarket_extreme = float(premarket_bars["Low"].min()) if not premarket_bars.empty else float("inf")

        for idx in range(len(regular_bars)):
            ts = regular_bars.index[idx]
            t = ts.time()
            if not (earliest <= t <= latest):
                continue

            bar = regular_bars.iloc[idx]
            current_price = float(bar["Close"])
            gap_pct = (current_price - prior_close) / prior_close * 100 if prior_close else 0.0

            d1 = current_price < float(prior_day["Low"])
            d3 = gap_pct <= -daily_filters["D3_min_gap_pct_down_from_prior_close"]
            i1 = current_price < premarket_extreme

            rsi = _compute_rsi(intraday.loc[:ts, "Close"], intraday_filters.get("I2_rsi_period", 14))
            i2 = rsi is not None and rsi < intraday_filters["I2_rsi_below"]

            volume_so_far = float(day_bars.loc[:ts, "Volume"].sum())
            rvol = volume_so_far / avg_daily_volume if avg_daily_volume else 0.0
            i3 = rvol >= intraday_filters["I3_rvol_min"]

            if d1 and d3 and i1 and i2 and i3:
                stop_ref = float(regular_bars["High"].iloc[: idx + 1].max())
                signals.append({
                    "date": date, "entry_time": ts, "entry_price": current_price,
                    "stop_ref": stop_ref, "gap_pct": gap_pct, "rvol": rvol, "rsi": rsi,
                    "ext_pct": ext_pct,
                })
                break  # one entry per day, like the live bot (symbol becomes "held")

    return signals


def simulate_trade(intraday: pd.DataFrame, signal: dict, exit_cfg: dict) -> dict:
    """Mirror of cycle.py's manage_position for one short position, replayed
    over historical bars: breakeven flip, partial profit, swing-high
    trailing stop, and a hard force-close at 15:51 ET every day (this bot
    never holds overnight)."""
    entry_price = signal["entry_price"]
    initial_stop = signal["stop_ref"] * 1.01
    initial_risk = initial_stop - entry_price
    if initial_risk <= 0:
        return {**signal, "outcome": "invalid_risk", "exit_time": signal["entry_time"],
                "exit_price": entry_price, "r_multiple": 0.0}

    stop_price = initial_stop
    state = "pre_breakeven"
    remaining_fraction = 1.0
    realized_r = 0.0

    future = intraday.loc[intraday.index > signal["entry_time"]]
    for ts, bar in future.iterrows():
        high = float(bar["High"])
        close = float(bar["Close"])

        if high >= stop_price:
            realized_r += remaining_fraction * ((entry_price - stop_price) / initial_risk)
            return {**signal, "outcome": "stopped_out", "exit_time": ts,
                    "exit_price": stop_price, "r_multiple": realized_r}

        r_multiple_now = (entry_price - close) / initial_risk

        if state == "pre_breakeven":
            if r_multiple_now >= exit_cfg["breakeven_trigger_R"]:
                stop_price = entry_price
                state = "post_breakeven_no_partial"
            elif r_multiple_now >= exit_cfg["partial_profit_trigger_R"]:
                close_fraction = min(exit_cfg["partial_profit_fraction"], remaining_fraction)
                realized_r += close_fraction * r_multiple_now
                remaining_fraction -= close_fraction
                stop_price = entry_price * 1.01
                state = "post_breakeven_partial_done"

        if state.startswith("post_breakeven"):
            recent = intraday.loc[:ts].tail(200)
            swing = _find_latest_swing_high(recent)
            if swing is not None:
                candidate_stop = swing + 0.01
                if candidate_stop < stop_price:
                    stop_price = candidate_stop

        if ts.time() >= FORCE_CLOSE_TIME:
            realized_r += remaining_fraction * r_multiple_now
            return {**signal, "outcome": "force_close", "exit_time": ts,
                    "exit_price": close, "r_multiple": realized_r}

    last_close = float(intraday["Close"].iloc[-1])
    r_multiple_now = (entry_price - last_close) / initial_risk
    realized_r += remaining_fraction * r_multiple_now
    return {**signal, "outcome": "data_ended_still_open", "exit_time": intraday.index[-1],
            "exit_price": last_close, "r_multiple": realized_r}


def run_backtest(symbol: str, strategy_name: str = "Short Parabolic Reversal") -> list[dict]:
    rules = load_strategy(strategy_name)
    daily, intraday = fetch_data(symbol)
    if intraday.empty or daily.empty:
        raise SystemExit(f"no data returned for {symbol!r}")
    signals = find_signals(daily, intraday, rules)
    return [simulate_trade(intraday, signal, rules["exit"]) for signal in signals]


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("symbol")
    parser.add_argument("--strategy", default="Short Parabolic Reversal")
    args = parser.parse_args()

    trades = run_backtest(args.symbol, args.strategy)
    if not trades:
        print(f"No signals found for {args.symbol} under '{args.strategy}' in the available data window.")
        return

    print(f"{'date':<12}{'entry_time':<12}{'entry':>9}{'stop':>9}{'exit_time':<12}{'exit':>9}{'outcome':<22}{'R':>7}")
    total_r = 0.0
    wins = 0
    for t in trades:
        initial_stop = t["stop_ref"] * 1.01
        print(f"{str(t['date']):<12}{t['entry_time'].strftime('%H:%M'):<12}{t['entry_price']:>9.2f}"
              f"{initial_stop:>9.2f}{t['exit_time'].strftime('%m-%d %H:%M'):<12}{t['exit_price']:>9.2f}"
              f"{t['outcome']:<22}{t['r_multiple']:>7.2f}")
        total_r += t["r_multiple"]
        if t["r_multiple"] > 0:
            wins += 1

    n = len(trades)
    print(f"\n{n} trade(s) | win rate {wins / n * 100:.0f}% | total R {total_r:.2f} | avg R {total_r / n:.2f}")


if __name__ == "__main__":
    main()
