"""Backtest engine: replays a strategy day-by-day against historical bars
to produce a trade log and performance stats "as if" it had really been
running — reusing cycle.py's exact live decision logic
(_evaluate_filters_from_bars for entries, _breakeven_or_partial_decision/
_trailing_stop_decision for exits) so the backtester can never quietly
drift from what the live bot actually does; only the data source and the
"place a real order" step are different.

Daily bars (D1-D3, SMA200/50) come from yfinance, same as the live bot —
that history isn't limited the way intraday is. Intraday bars (I1-I3,
entry timing, exit management) come from the local IBKR cache (see
src/backtest_data.py), which is what lets this go back further than
yfinance's ~60-day intraday window.

Every simulated trade opens and force-closes within the same session,
mirroring the live bot's own end-of-day flatten (FORCE_CLOSE_START) —
there's no overnight-hold simulation in v1 (hold_overnight is a manual,
one-off dashboard action, not something an automated backtest should
assume every trade takes).

Sizing mirrors entry_scan's REAL formula exactly, including its existing
quirk: max_risk_pct/portfolio_value/max_trades_per_day come from the
caller-supplied virtual risk settings (matching the account-level "Risk
Settings" the live bot actually reads), while max_position_size_pct_of_
portfolio/max_concurrent_positions come from the strategy's own rules —
a strategy's own "risk.max_risk_per_trade_pct" field is realistic-but-
unused in the live bot too, so reproducing that exactly (not silently
"fixing" it) is what makes this a faithful replay rather than a nicer-
but-different simulation.
"""
import math
from datetime import time as dt_time

import pandas as pd
import yfinance as yf

import cycle
from src import backtest_data

BAR_SIZE = "5 mins"
INTRADAY_LOOKBACK_DAYS = 5  # matches _evaluate_entry_filters' own "5d" yfinance window


def fetch_daily_bars(symbol: str) -> pd.DataFrame | None:
    """Full daily history via yfinance, fetched once per symbol per
    backtest run then sliced per evaluation day — not the constrained
    resource, unlike intraday."""
    try:
        bars = yf.Ticker(symbol.replace(" ", "-")).history(period="max", interval="1d")
        return bars if not bars.empty else None
    except Exception:
        return None


def _trading_days(intraday: pd.DataFrame, start_date, end_date) -> list:
    dates = sorted({ts.date() for ts in intraday.index if start_date <= ts.date() <= end_date})
    return dates


def _daily_as_of(daily: pd.DataFrame, day) -> pd.DataFrame | None:
    """daily.iloc[-2] must be "yesterday" relative to `day` (see
    cycle._evaluate_filters_from_bars) — slice to everything strictly
    before `day`, then treat the last two rows as (day-2, day-1)."""
    sliced = daily[daily.index.date < day]
    if len(sliced) < 201:
        return None
    # Append a placeholder "today" row so .iloc[-2] lines up exactly like
    # the live function's daily fetch (which always includes today's
    # still-forming bar as the last row) — its own values are never read.
    return pd.concat([sliced, sliced.iloc[[-1]]])


def _intraday_window(intraday: pd.DataFrame, as_of: pd.Timestamp) -> pd.DataFrame:
    window_start = as_of - pd.Timedelta(days=INTRADAY_LOOKBACK_DAYS)
    return intraday[(intraday.index > window_start) & (intraday.index <= as_of)]


def _parse_force_close_time(strategy_rules: dict) -> dt_time:
    raw = strategy_rules.get("time_filter", {}).get("force_close_et", "15:51")
    try:
        hour, minute = (int(part) for part in raw.split(":", 1))
        return dt_time(hour, minute)
    except (ValueError, TypeError):
        return dt_time(15, 51)


def _find_stop_out(bars: pd.DataFrame, stop_price: float, side: str) -> pd.Timestamp | None:
    """First bar (if any) whose range crosses the stop, for a same-day
    intrabar stop-out — checked against High/Low, not just Close, since a
    real stop order fills on the wick, not the candle's close."""
    if side == "short":
        hit = bars[bars["High"] >= stop_price]
    else:
        hit = bars[bars["Low"] <= stop_price]
    return hit.index[0] if not hit.empty else None


def simulate_strategy(
    strategy_rules: dict,
    side: str,
    symbols: list[str],
    start_date,
    end_date,
    portfolio_value: float,
    max_risk_pct: float,
    max_trades_per_day: int,
) -> dict:
    """Runs one strategy over one date range against every symbol that has
    cached intraday data, returns {"trades": [...], "skipped_symbols": [...]}
    — trades in the same {symbol, side: "BUY"/"SELL", fill_price, size,
    timestamp_iso} shape db.get_trades rows have, so src/perf.py's
    pair_trades/aggregate/compute_r_multiples/histogram work on the result
    completely unchanged - the backtest's stats come from the exact same
    pipeline the live dashboard's Performance card already uses (including
    its existing 1%-proxy R-multiple approximation, rather than the more
    precise stop this engine actually simulated), so the two are always
    directly comparable."""
    exit_cfg = strategy_rules["exit"]
    max_concurrent = strategy_rules["risk"]["max_concurrent_positions"]
    max_position_pct = strategy_rules["risk"]["max_position_size_pct_of_portfolio"] / 100
    action = "BUY" if side == "long" else "SELL"
    close_action = "SELL" if side == "long" else "BUY"
    force_close_time = _parse_force_close_time(strategy_rules)

    daily_by_symbol: dict[str, pd.DataFrame] = {}
    intraday_by_symbol: dict[str, pd.DataFrame] = {}
    skipped = []
    for symbol in symbols:
        intraday = backtest_data.load_cached_bars(symbol, BAR_SIZE)
        if intraday is None or intraday.empty:
            skipped.append({"symbol": symbol, "reason": "no cached intraday bars"})
            continue
        daily = fetch_daily_bars(symbol)
        if daily is None:
            skipped.append({"symbol": symbol, "reason": "no daily bars"})
            continue
        daily_by_symbol[symbol] = daily
        intraday_by_symbol[symbol] = intraday

    if not intraday_by_symbol:
        return {"trades": [], "skipped_symbols": skipped}

    all_days = sorted(set().union(*[
        set(_trading_days(intraday, start_date, end_date)) for intraday in intraday_by_symbol.values()
    ]))

    trades = []
    trade_id = 0
    for day in all_days:
        open_positions: dict[str, dict] = {}  # symbol -> simulated position state
        entries_today = 0

        # Union of this day's regular-session 5-min timestamps across every
        # symbol, walked in chronological order — mirrors run_cycle's own
        # structure: at each tick, every open position is managed first,
        # THEN the watchlist is scanned for new entries, so a symbol never
        # "uses up" the whole day's trade/position caps before another
        # symbol even gets checked at the session's earlier ticks.
        day_bar_times = set()
        for intraday in intraday_by_symbol.values():
            day_bars = intraday[intraday.index.date == day]
            regular_bars = day_bars[day_bars.index.time >= dt_time(9, 30)]
            day_bar_times.update(regular_bars.index)
        sorted_times = sorted(day_bar_times)

        for bar_ts in sorted_times:
            if bar_ts.time() >= force_close_time:
                # Mirrors run_cycle's force_close_all: once the strategy's
                # own force_close_et is reached, every open position is
                # flattened immediately at this bar's price, no further
                # management or new entries for the rest of the day.
                for symbol, pos in list(open_positions.items()):
                    trade_id += 1
                    trades.append({
                        "id": trade_id, "symbol": symbol, "side": close_action,
                        "fill_price": float(intraday_by_symbol[symbol].loc[bar_ts, "Close"])
                        if bar_ts in intraday_by_symbol[symbol].index else pos["entry_price"],
                        "size": pos["qty"], "timestamp_iso": bar_ts.isoformat(),
                    })
                open_positions.clear()
                break

            # --- Step 1: manage every currently open position ---
            for symbol in list(open_positions.keys()):
                intraday = intraday_by_symbol[symbol]
                if bar_ts not in intraday.index:
                    continue  # no new bar for this symbol at this exact tick
                pos = open_positions[symbol]
                bar = intraday.loc[bar_ts]

                stop_hit = _find_stop_out(intraday.loc[[bar_ts]], pos["stop_price"], side)
                if stop_hit is not None:
                    trade_id += 1
                    trades.append({
                        "id": trade_id, "symbol": symbol, "side": close_action,
                        "fill_price": pos["stop_price"], "size": pos["qty"],
                        "timestamp_iso": bar_ts.isoformat(),
                    })
                    del open_positions[symbol]
                    continue

                price = float(bar["Close"])
                initial_risk = (pos["initial_stop"] - pos["entry_price"]) if side == "short" else (pos["entry_price"] - pos["initial_stop"])
                r_multiple = ((pos["entry_price"] - price) if side == "short" else (price - pos["entry_price"])) / initial_risk if initial_risk > 0 else 0.0

                if pos["state"] == "pre_breakeven":
                    decision = cycle._breakeven_or_partial_decision(pos, exit_cfg, r_multiple)
                    if decision["action"] == "breakeven_flip":
                        pos["stop_price"] = decision["new_stop_price"]
                        pos["state"] = decision["new_state"]
                    elif decision["action"] == "partial_profit":
                        close_qty = decision["close_qty"]
                        trade_id += 1
                        trades.append({
                            "id": trade_id, "symbol": symbol, "side": close_action,
                            "fill_price": price, "size": close_qty, "timestamp_iso": bar_ts.isoformat(),
                        })
                        pos["qty"] -= close_qty
                        pos["stop_price"] = decision["new_stop_price"]
                        pos["state"] = decision["new_state"]

                if pos["qty"] > 0 and pos["state"].startswith("post_breakeven"):
                    recent_bars = intraday[intraday.index <= bar_ts].tail(30)
                    swing = (
                        cycle._find_latest_swing_high(recent_bars) if side == "short"
                        else cycle._find_latest_swing_low(recent_bars)
                    )
                    candidate = None
                    if swing is not None and len(recent_bars) > 5:
                        candidate = (swing + 0.01) if side == "short" else (swing - 0.01)
                    trail_decision = cycle._trailing_stop_decision(pos, candidate)
                    if trail_decision["action"] == "trail_stop":
                        pos["stop_price"] = trail_decision["new_stop_price"]

                if pos["qty"] <= 0:
                    del open_positions[symbol]

            # --- Step 2: scan for new entries across the whole universe ---
            for symbol, intraday in intraday_by_symbol.items():
                if symbol in open_positions:
                    continue
                if len(open_positions) >= max_concurrent or entries_today >= max_trades_per_day:
                    break
                if bar_ts not in intraday.index:
                    continue
                if not cycle._within_entry_window(strategy_rules, bar_ts.to_pydatetime()):
                    continue

                daily_slice = _daily_as_of(daily_by_symbol[symbol], day)
                if daily_slice is None:
                    continue
                intraday_slice = _intraday_window(intraday, bar_ts)
                detail = cycle._evaluate_filters_from_bars(daily_slice, intraday_slice, strategy_rules, side)
                if not detail.get("pass"):
                    continue

                price = detail["price"]
                stop_ref = detail["stop_ref"]
                initial_stop = stop_ref * 1.01 if side == "short" else stop_ref * 0.99
                r = (initial_stop - price) if side == "short" else (price - initial_stop)
                if r <= 0:
                    continue

                risk_dollars = portfolio_value * (max_risk_pct / 100)
                size_by_risk = math.floor(risk_dollars / r)
                size_by_cap = math.floor(portfolio_value * max_position_pct / price)
                size = min(size_by_risk, size_by_cap)
                if size < 1:
                    continue

                trade_id += 1
                trades.append({
                    "id": trade_id, "symbol": symbol, "side": action,
                    "fill_price": price, "size": size, "timestamp_iso": bar_ts.isoformat(),
                })
                open_positions[symbol] = {
                    "side": side, "entry_price": price, "initial_stop": initial_stop,
                    "stop_price": initial_stop, "qty": size, "state": "pre_breakeven",
                }
                entries_today += 1

        # Fallback for the rare case the force_close_time tick was never
        # reached this day (e.g. a holiday-shortened session that ends
        # before force_close_et) — the loop above already handles the
        # normal case.
        for symbol, pos in open_positions.items():
            intraday = intraday_by_symbol[symbol]
            day_bars = intraday[intraday.index.date == day]
            if day_bars.empty:
                continue
            last_bar = day_bars.iloc[-1]
            trade_id += 1
            trades.append({
                "id": trade_id, "symbol": symbol, "side": close_action,
                "fill_price": float(last_bar["Close"]), "size": pos["qty"],
                "timestamp_iso": day_bars.index[-1].isoformat(),
            })

    return {"trades": trades, "skipped_symbols": skipped}
