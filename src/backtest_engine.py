"""Backtest engine: replays a strategy day-by-day against historical bars
to produce a trade log and performance stats "as if" it had really been
running — reusing cycle.py's exact live decision logic
(_evaluate_filters_from_bars for entries, _breakeven_or_partial_decision/
_trailing_stop_decision for exits) so the backtester can never quietly
drift from what the live bot actually does; only the data source and the
"place a real order" step are different.

Daily bars (D1-D3, SMA200/50) come from yfinance, same as the live bot —
that history isn't limited the way intraday is, and deliberately isn't
aggregated from the cached IBKR intraday bars instead: doing that would
risk quietly drifting from the live bot's actual daily-bar values
(vendor differences in split/dividend adjustment, rounding) and could
run short of the 200 days SMA200 needs near the start of the cached
intraday window, defeating the "faithful replay" the whole engine exists
for. Cached to disk on first use (see fetch_daily_bars, src/backtest_
data.py) purely to avoid re-downloading yfinance's full history on every
run, not to change where it comes from. Intraday bars (I1-I3, entry
timing, exit management) come from the local IBKR cache (see
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
import concurrent.futures
import math
from datetime import date, timedelta
from datetime import time as dt_time

import pandas as pd
import yfinance as yf

import cycle
from src import backtest_data

BAR_SIZE = "5 mins"
DAILY_BAR_SIZE = "1 day"
# Reads cycle's own constant rather than hardcoding a second copy of the
# same number - this MUST match _evaluate_entry_filters' live intraday
# fetch window (I3's relative-volume lookback needs that much history
# available either way), and a hardcoded copy here is exactly the kind of
# thing that quietly drifts out of sync with a comment alone to enforce it.
INTRADAY_LOOKBACK_DAYS = cycle.INTRADAY_FETCH_LOOKBACK_DAYS
_DAILY_COLUMNS = ["Open", "High", "Low", "Close", "Volume"]

# yfinance's own `timeout=` kwarg only bounds a single HTTP request; its
# internal retry/backoff (and Yahoo occasionally sending a large
# Retry-After on a 429, which cloud-datacenter IPs like this server's draw
# far more often than home IPs) can still stall one .history() call for a
# very long time. simulate_strategy calls fetch_daily_bars sequentially
# for every symbol in a background thread, so one throttled/hung symbol
# would otherwise freeze an entire backtest run with nothing to show for
# it - not even an error, just stuck on "running" - so every call here
# gets a hard wall-clock ceiling of its own, timing out to "no data for
# this symbol" exactly like the yfinance-side exceptions already handled
# below.
_YF_TIMEOUT_SECONDS = 45
# Bounded rather than "one thread per symbol" so a fully-throttled Yahoo
# session doesn't just turn N sequential 45s stalls into N concurrent
# ones - a handful of workers still gets real parallelism on the common
# case (network latency, not CPU) without hammering Yahoo harder than a
# person clicking around would.
_DAILY_FETCH_WORKERS = 6


def _yf_history(yf_symbol: str, **kwargs) -> pd.DataFrame:
    executor = concurrent.futures.ThreadPoolExecutor(max_workers=1)
    try:
        future = executor.submit(yf.Ticker(yf_symbol).history, **kwargs)
        return future.result(timeout=_YF_TIMEOUT_SECONDS)
    finally:
        # wait=False: a genuinely hung call can't be cancelled mid-flight,
        # so don't block here re-waiting on it - just abandon this
        # executor and move on to the next symbol.
        executor.shutdown(wait=False)


def fetch_daily_bars(symbol: str) -> pd.DataFrame | None:
    """Daily history via yfinance, cached to disk forever (same cache as
    the intraday IBKR bars, keyed by DAILY_BAR_SIZE) so repeat backtest
    runs - across strategies in the same batch, or on a later day - don't
    re-download yfinance's full "period=max" history from scratch every
    time. yfinance stays the source of truth for daily bars (matching
    what the live bot itself reads - see module docstring on why this
    can't just be aggregated from the cached IBKR intraday bars instead);
    this only avoids re-asking it for data it already gave us, by
    re-fetching just the gap since the last cached day instead of
    everything."""
    yf_symbol = symbol.replace(" ", "-")
    cached = backtest_data.load_cached_bars(symbol, DAILY_BAR_SIZE)
    if cached is not None and not cached.empty:
        if cached.index.max().date() >= date.today() - timedelta(days=1):
            return cached
        try:
            fresh = _yf_history(yf_symbol, start=cached.index.max().date(), interval="1d")
        except Exception:
            return cached
        if fresh.empty:
            return cached
        merged = backtest_data.merge_bars(cached, fresh[_DAILY_COLUMNS])
        backtest_data.save_cached_bars(symbol, DAILY_BAR_SIZE, merged)
        return merged

    try:
        bars = _yf_history(yf_symbol, period="max", interval="1d")
    except Exception:
        return None
    if bars.empty:
        return None
    bars = bars[_DAILY_COLUMNS]
    backtest_data.save_cached_bars(symbol, DAILY_BAR_SIZE, bars)
    return bars


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
    cached intraday data, returns {"trades": [...], "skipped_symbols": [...],
    "filter_stats": {...}} — trades in the same {symbol, side: "BUY"/"SELL",
    fill_price, size, timestamp_iso} shape db.get_trades rows have, so
    src/perf.py's pair_trades/aggregate/compute_r_multiples/histogram work
    on the result completely unchanged - the backtest's stats come from the
    exact same pipeline the live dashboard's Performance card already uses
    (including its existing 1%-proxy R-multiple approximation, rather than
    the more precise stop this engine actually simulated), so the two are
    always directly comparable. filter_stats is {"evaluations": int,
    "insufficient_data": int, "D1"|"D2"|"D3"|"I1"|"I2"|"I3": int} - a pass
    count for each condition out of "evaluations", for surfacing WHY a
    strategy found few or no trades (which specific condition(s) are
    actually the bottleneck) instead of leaving that to guesswork."""
    exit_cfg = strategy_rules["exit"]
    max_concurrent = strategy_rules["risk"]["max_concurrent_positions"]
    max_position_pct = strategy_rules["risk"]["max_position_size_pct_of_portfolio"] / 100
    action = "BUY" if side == "long" else "SELL"
    close_action = "SELL" if side == "long" else "BUY"
    force_close_time = _parse_force_close_time(strategy_rules)

    daily_by_symbol: dict[str, pd.DataFrame] = {}
    intraday_by_symbol: dict[str, pd.DataFrame] = {}
    skipped = []
    daily_candidates = {}  # symbol -> already-loaded intraday df, pending a daily fetch
    for symbol in symbols:
        intraday = backtest_data.load_cached_bars(symbol, BAR_SIZE)
        if intraday is None or intraday.empty:
            skipped.append({"symbol": symbol, "reason": "no cached intraday bars"})
            continue
        # The cache holds a symbol's ENTIRE fetched history (200+ days, per
        # fetch_backtest_data.py), but only [start_date - lookback,
        # end_date] is ever actually read below (_intraday_window never
        # looks further back than INTRADAY_LOOKBACK_DAYS, and
        # _trading_days/day_bar_times never look past end_date) - keeping
        # the rest in memory for the whole simulation was pure waste,
        # multiplied by however many symbols are in the universe. This is
        # the dominant memory cost of a backtest (confirmed live: even a
        # single-day run against ~500 symbols was memory-heavy), and
        # trimming it here is a pure performance change with no effect on
        # which trades get simulated - daily bars (SMA200 etc.) are a
        # separate, much smaller cache and don't need this.
        window_start = pd.Timestamp(start_date, tz=intraday.index.tz) - pd.Timedelta(days=INTRADAY_LOOKBACK_DAYS)
        window_end = pd.Timestamp(end_date, tz=intraday.index.tz) + pd.Timedelta(days=1)
        intraday = intraday[(intraday.index >= window_start) & (intraday.index < window_end)]
        if intraday.empty:
            skipped.append({"symbol": symbol, "reason": "no cached bars in the requested date range"})
            continue
        daily_candidates[symbol] = intraday

    # `symbols` is the whole cached universe regardless of the requested
    # date range or strategy count (a 1-day backtest still needs every
    # symbol's 200+ days of daily history for the SMA filters), and any
    # symbol whose daily bars aren't cached yet costs a real yfinance call
    # - up to fetch_daily_bars' own _YF_TIMEOUT_SECONDS ceiling apiece if
    # Yahoo is throttling this box. Fetching those concurrently (bounded
    # pool) instead of one-by-one is what actually cuts wall-clock time;
    # the per-call timeout still protects each individual fetch.
    with concurrent.futures.ThreadPoolExecutor(max_workers=_DAILY_FETCH_WORKERS) as pool:
        future_to_symbol = {pool.submit(fetch_daily_bars, symbol): symbol for symbol in daily_candidates}
        for future in concurrent.futures.as_completed(future_to_symbol):
            symbol = future_to_symbol[future]
            daily = future.result()
            if daily is None:
                skipped.append({"symbol": symbol, "reason": "no daily bars"})
                continue
            daily_by_symbol[symbol] = daily
            intraday_by_symbol[symbol] = daily_candidates[symbol]

    # Every entry-scan check runs the FULL D1-I3 evaluation regardless of
    # which condition(s) actually fail (_evaluate_filters_from_bars never
    # short-circuits - see its own docstring), so every single check gives
    # a complete picture of all six conditions, not just whichever one
    # would have failed first. Aggregating that here answers "why are
    # there so few/no trades" directly, instead of guessing - e.g. a
    # strategy needing a rare gap AND high relative volume simultaneously
    # can legitimately have a near-zero combined pass rate over a short
    # date range even though no single condition is unreasonable on its
    # own. "evaluations" excludes checks that bailed out early for
    # insufficient daily/intraday history (counted separately) - those
    # never got to compute D1-I3 at all.
    filter_stats = {"evaluations": 0, "insufficient_data": 0,
                     "D1": 0, "D2": 0, "D3": 0, "I1": 0, "I2": 0, "I3": 0}

    if not intraday_by_symbol:
        return {"trades": [], "skipped_symbols": skipped, "filter_stats": filter_stats}

    all_days = sorted(set().union(*[
        set(_trading_days(intraday, start_date, end_date)) for intraday in intraday_by_symbol.values()
    ]))

    # One split-by-calendar-day pass per symbol, done ONCE for the whole
    # run (not per simulated day, and definitely not per tick) - I3's
    # time-of-day-adjusted relative volume (see cycle._evaluate_filters_
    # from_bars) needs "this symbol's bars for day X" for each of several
    # prior trading days, and re-deriving that from the full intraday
    # window on every single entry-scan tick (as a naive implementation
    # would) redoes the same O(window size) scan hundreds of times a day
    # per symbol for nothing - the exact same class of waste
    # daily_slice_by_symbol below already fixed for the daily-bar side.
    day_groups_by_symbol = {
        symbol: dict(tuple(intraday.groupby(intraday.index.date)))
        for symbol, intraday in intraday_by_symbol.items()
    }

    trades = []
    trade_id = 0
    for day in all_days:
        open_positions: dict[str, dict] = {}  # symbol -> simulated position state
        entries_today = 0

        # _daily_as_of(daily_by_symbol[symbol], day) depends only on
        # (symbol, day), not on the tick - computing it fresh on every
        # single intraday bar in Step 2 below (as this used to) redid the
        # same full-history slice up to ~100-190x per symbol per day for
        # nothing. Once per (symbol, day) here instead.
        daily_slice_by_symbol = {
            symbol: _daily_as_of(daily_by_symbol[symbol], day) for symbol in intraday_by_symbol
        }
        # Cheap dict filtering (no DataFrame scanning) over the day groups
        # already split out above - just "which of this symbol's already-
        # separated days are before today".
        prior_day_bars_by_symbol = {
            symbol: {d: bars for d, bars in groups.items() if d < day}
            for symbol, groups in day_groups_by_symbol.items()
        }

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
                        "exit_reason": "eod_close",
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
                        "exit_reason": "stop_loss" if pos["state"] == "pre_breakeven" else "trailing_stop",
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
                            "exit_reason": "partial_profit",
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

                daily_slice = daily_slice_by_symbol[symbol]
                if daily_slice is None:
                    continue
                intraday_slice = _intraday_window(intraday, bar_ts)
                detail = cycle._evaluate_filters_from_bars(
                    daily_slice, intraday_slice, strategy_rules, side,
                    prior_day_bars=prior_day_bars_by_symbol[symbol],
                )
                if "error" in detail:
                    filter_stats["insufficient_data"] += 1
                    continue
                filter_stats["evaluations"] += 1
                for cond in ("D1", "D2", "D3", "I1", "I2", "I3"):
                    if detail.get(cond):
                        filter_stats[cond] += 1
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
                "exit_reason": "eod_close",
            })

    return {"trades": trades, "skipped_symbols": skipped, "filter_stats": filter_stats}
