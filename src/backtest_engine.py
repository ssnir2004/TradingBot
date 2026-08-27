"""Backtest engine: replays a strategy day-by-day against historical bars
to produce a trade log and performance stats "as if" it had really been
running — reusing cycle.py's exact live decision logic
(_evaluate_filters_from_bars for entries, _breakeven_decision/
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
from src import backtest_data, orb, touch_turn

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
    commission_per_trade: float = 0.0,
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
    actually the bottleneck) instead of leaving that to guesswork.

    commission_per_trade is charged per FILL (both the entry and the exit
    cost it separately - 2x per closed position, not 1x), recorded on each
    trade record as "commission" and only ever summed up
    downstream in perf.pair_trades/aggregate - never subtracted from
    fill_price itself, so it can't quietly distort stop/sizing math that
    has nothing to do with transaction costs. Defaults to 0 (unchanged
    behavior, matching every trade source that predates this - live/paper
    trades never set this field at all)."""
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
        # daily_derived_cache_by_symbol memoizes SMA200/SMA50/ATR per
        # symbol for today (see _evaluate_filters_from_bars' own
        # daily_derived_cache param) - all three depend only on
        # daily_slice, never on the tick, so recomputing them on every one
        # of a symbol's ~24 evaluate calls today was pure waste. A fresh
        # dict per (symbol, day), same lifetime as daily_slice_by_symbol.
        daily_derived_cache_by_symbol: dict[str, dict] = {}
        # D2 is the one daily filter with NO intraday/current-price
        # component at all (unlike D1's "above yesterday's high" or D3's
        # gap %, both genuinely tick-dependent as price moves through the
        # day) - once it fails for a symbol today, it can never pass later
        # today, so every later tick can skip that symbol's evaluate call
        # entirely rather than re-deriving the same D2=False result.
        d2_failed_today: set[str] = set()

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
                        "exit_reason": "eod_close", "commission": commission_per_trade,
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
                        "commission": commission_per_trade,
                    })
                    del open_positions[symbol]
                    continue

                price = float(bar["Close"])
                initial_risk = (pos["initial_stop"] - pos["entry_price"]) if side == "short" else (pos["entry_price"] - pos["initial_stop"])
                r_multiple = ((pos["entry_price"] - price) if side == "short" else (price - pos["entry_price"])) / initial_risk if initial_risk > 0 else 0.0

                if pos["state"] == "pre_breakeven":
                    decision = cycle._breakeven_decision(pos, exit_cfg, r_multiple)
                    if decision["action"] == "breakeven_flip":
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
            # _within_entry_window depends only on strategy_rules + bar_ts,
            # never on the symbol - checking it once per tick here instead
            # of once per (symbol, tick) skips the entire per-symbol loop
            # below outright outside the entry window, rather than paying
            # the same symbol-independent check up to ~500 times over.
            if cycle._within_entry_window(strategy_rules, bar_ts.to_pydatetime()):
                for symbol, intraday in intraday_by_symbol.items():
                    if symbol in open_positions:
                        continue
                    if symbol in d2_failed_today:
                        continue
                    if len(open_positions) >= max_concurrent or entries_today >= max_trades_per_day:
                        break
                    if bar_ts not in intraday.index:
                        continue

                    daily_slice = daily_slice_by_symbol[symbol]
                    if daily_slice is None:
                        continue
                    # _evaluate_filters_from_bars only ever reads TODAY's
                    # bars off its `intraday` param once prior_day_bars is
                    # supplied (always true here - see its own docstring);
                    # the other ~24 days _intraday_window used to hand it
                    # were dead weight on every single tick.
                    # day_groups_by_symbol already has today's bars split
                    # out for free (see prior_day_bars_by_symbol above) -
                    # slicing that to <= bar_ts is a mask over ~1 day's
                    # rows instead of ~25 days', on the single hottest call
                    # in this whole loop (one evaluate call per symbol not
                    # yet in a position, per tick, per day).
                    today_bars_full = day_groups_by_symbol[symbol].get(day)
                    if today_bars_full is None:
                        continue
                    intraday_slice = today_bars_full[today_bars_full.index <= bar_ts]
                    detail = cycle._evaluate_filters_from_bars(
                        daily_slice, intraday_slice, strategy_rules, side,
                        prior_day_bars=prior_day_bars_by_symbol[symbol],
                        signal_side=strategy_rules.get("signal_side"),
                        daily_derived_cache=daily_derived_cache_by_symbol.setdefault(symbol, {}),
                    )
                    if "error" in detail:
                        filter_stats["insufficient_data"] += 1
                        continue
                    filter_stats["evaluations"] += 1
                    for cond in ("D1", "D2", "D3", "I1", "I2", "I3"):
                        if detail.get(cond):
                            filter_stats[cond] += 1
                    if not detail.get("D2"):
                        d2_failed_today.add(symbol)
                    if not detail.get("pass"):
                        continue

                    price = detail["price"]
                    initial_stop = cycle._resolve_initial_stop(detail, strategy_rules, side)
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
                        "initial_stop": initial_stop, "commission": commission_per_trade,
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
                "exit_reason": "eod_close", "commission": commission_per_trade,
            })

    return {"trades": trades, "skipped_symbols": skipped, "filter_stats": filter_stats}


def _daily_as_of_light(daily: pd.DataFrame, day, min_days: int) -> pd.DataFrame | None:
    """Same "yesterday" alignment as _daily_as_of (daily.iloc[-2] must be
    the prior trading day relative to `day`), but for ORB's own ATR-only
    need instead of D1-D3's 200-day SMA requirement - only `min_days`
    complete days are required, not 201, so ORB doesn't need anywhere
    near as much daily history warmed up before it can start evaluating."""
    sliced = daily[daily.index.date < day]
    if len(sliced) < min_days:
        return None
    return pd.concat([sliced, sliced.iloc[[-1]]])  # placeholder "today" row, same reasoning as _daily_as_of


def _find_target_out(bars: pd.DataFrame, target_price: float, side: str) -> pd.Timestamp | None:
    """Mirror of _find_stop_out for the fixed target side - first bar (if
    any) whose range crosses the target, checked against High/Low."""
    if side == "short":
        hit = bars[bars["Low"] <= target_price]
    else:
        hit = bars[bars["High"] >= target_price]
    return hit.index[0] if not hit.empty else None


def simulate_orb_strategy(
    strategy_rules: dict,
    side: str,
    symbols: list[str],
    start_date,
    end_date,
    portfolio_value: float,
    max_risk_pct: float,
    max_trades_per_day: int,
    commission_per_trade: float = 0.0,
) -> dict:
    """ORB's own replay loop - dispatched from src/backtest_runner.py
    whenever a strategy's rules carry an "opening_range" key, instead of
    simulate_strategy above. Same day-by-day/bar-by-bar structure and the
    same {"trades": [...], "skipped_symbols": [...], "filter_stats": {...}}
    return shape (so perf.py's pipeline works unchanged), but:
      - entries come from orb.evaluate_orb_entry, not cycle._evaluate_
        filters_from_bars - no daily_filters/D1-D3 at all, so daily bars
        only need to cover ATR's own lookback (_daily_as_of_light), not
        200 days for an SMA.
      - exits depend on exit.management_style: "fixed_target_no_trail"
        (the original ORB Long/ORB Short) is stop-or-fixed-target, no
        position "state" machine, just a stop check and a target check
        each tick (see orb.fixed_target_decision). "staged_trail" (an
        "improve ORB" v2 variant) instead reuses cycle._breakeven_
        decision/_trailing_stop_decision - same pure functions
        simulate_strategy above already shares with the live bot - gated
        by exit.trailing_trigger_R before trailing starts, and trailing
        off orb.low_of_last_n_bars/high_of_last_n_bars instead of a
        swing-pivot detection.
      - filter_stats' condition keys are ORB's own diagnostic flags
        (or_formed/confirmed/volatility_ok/confluence_ok) instead of
        D1-I3, but the shape (an "evaluations" pass-rate per key) is the
        same convention.

    This is a deliberately separate function rather than a branch inside
    simulate_strategy - some setup code is duplicated (loading cached
    bars, the concurrent daily-bar fetch), but it keeps this brand new,
    unvalidated strategy's replay logic from touching simulate_strategy's
    own well-exercised code path at all."""
    max_concurrent = strategy_rules["risk"]["max_concurrent_positions"]
    max_position_pct = strategy_rules["risk"]["max_position_size_pct_of_portfolio"] / 100
    action = "BUY" if side == "long" else "SELL"
    close_action = "SELL" if side == "long" else "BUY"
    force_close_time = _parse_force_close_time(strategy_rules)
    atr_period = strategy_rules["volatility_filters"].get("V2_atr_period", 14)
    min_daily_days = atr_period + 1
    exit_cfg = strategy_rules["exit"]
    management_style = exit_cfg.get("management_style", "fixed_target_no_trail")
    trailing_trigger_r = exit_cfg.get("trailing_trigger_R", 3.0)

    daily_by_symbol: dict[str, pd.DataFrame] = {}
    intraday_by_symbol: dict[str, pd.DataFrame] = {}
    skipped = []
    daily_candidates = {}
    for symbol in symbols:
        intraday = backtest_data.load_cached_bars(symbol, BAR_SIZE)
        if intraday is None or intraday.empty:
            skipped.append({"symbol": symbol, "reason": "no cached intraday bars"})
            continue
        window_start = pd.Timestamp(start_date, tz=intraday.index.tz) - pd.Timedelta(days=INTRADAY_LOOKBACK_DAYS)
        window_end = pd.Timestamp(end_date, tz=intraday.index.tz) + pd.Timedelta(days=1)
        intraday = intraday[(intraday.index >= window_start) & (intraday.index < window_end)]
        if intraday.empty:
            skipped.append({"symbol": symbol, "reason": "no cached bars in the requested date range"})
            continue
        daily_candidates[symbol] = intraday

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

    # confluence_ok is only a real (non-trivial) filter for a strategy
    # that actually configures entry_confluence (see orb.evaluate_orb_
    # entry) - omitted here for a v1-style strategy so its filter_stats
    # output stays identical to before (a filter that's always True isn't
    # useful signal, just noise in the funnel).
    has_confluence = "entry_confluence" in strategy_rules
    filter_stats = {"evaluations": 0, "insufficient_data": 0, "or_formed": 0, "confirmed": 0, "volatility_ok": 0}
    if has_confluence:
        filter_stats["confluence_ok"] = 0

    if not intraday_by_symbol:
        return {"trades": [], "skipped_symbols": skipped, "filter_stats": filter_stats}

    all_days = sorted(set().union(*[
        set(_trading_days(intraday, start_date, end_date)) for intraday in intraday_by_symbol.values()
    ]))
    day_groups_by_symbol = {
        symbol: dict(tuple(intraday.groupby(intraday.index.date)))
        for symbol, intraday in intraday_by_symbol.items()
    }

    trades = []
    trade_id = 0
    for day in all_days:
        open_positions: dict[str, dict] = {}
        entries_today = 0

        daily_slice_by_symbol = {
            symbol: _daily_as_of_light(daily_by_symbol[symbol], day, min_daily_days) for symbol in intraday_by_symbol
        }
        prior_day_bars_by_symbol = {
            symbol: {d: bars for d, bars in groups.items() if d < day}
            for symbol, groups in day_groups_by_symbol.items()
        }

        day_bar_times = set()
        for intraday in intraday_by_symbol.values():
            day_bars = intraday[intraday.index.date == day]
            regular_bars = day_bars[day_bars.index.time >= dt_time(9, 30)]
            day_bar_times.update(regular_bars.index)
        sorted_times = sorted(day_bar_times)

        for bar_ts in sorted_times:
            if bar_ts.time() >= force_close_time:
                for symbol, pos in list(open_positions.items()):
                    trade_id += 1
                    trades.append({
                        "id": trade_id, "symbol": symbol, "side": close_action,
                        "fill_price": float(intraday_by_symbol[symbol].loc[bar_ts, "Close"])
                        if bar_ts in intraday_by_symbol[symbol].index else pos["entry_price"],
                        "size": pos["qty"], "timestamp_iso": bar_ts.isoformat(),
                        "exit_reason": "eod_close", "commission": commission_per_trade,
                    })
                open_positions.clear()
                break

            # --- Step 1: manage every currently open position ---
            for symbol in list(open_positions.keys()):
                intraday = intraday_by_symbol[symbol]
                if bar_ts not in intraday.index:
                    continue
                pos = open_positions[symbol]
                this_bar = intraday.loc[[bar_ts]]

                if management_style == "fixed_target_no_trail":
                    # Stop or fixed target, whichever hits first this bar.
                    stop_hit = _find_stop_out(this_bar, pos["stop_price"], side)
                    target_hit = _find_target_out(this_bar, pos["target_price"], side)
                    if stop_hit is not None or target_hit is not None:
                        # Both are always the SAME timestamp when both fire (a
                        # single-row `this_bar` slice - either hit's index is
                        # just bar_ts) - OHLC bars alone can't say which one the
                        # price actually touched first intrabar, so a same-bar
                        # collision is resolved optimistically (target wins).
                        # This is a simplification worth knowing about when
                        # reading backtest results, not a bug.
                        hit_target = target_hit is not None and (stop_hit is None or target_hit <= stop_hit)
                        trade_id += 1
                        trades.append({
                            "id": trade_id, "symbol": symbol, "side": close_action,
                            "fill_price": pos["target_price"] if hit_target else pos["stop_price"],
                            "size": pos["qty"], "timestamp_iso": bar_ts.isoformat(),
                            "exit_reason": "target" if hit_target else "stop_loss",
                            "commission": commission_per_trade,
                        })
                        del open_positions[symbol]
                    continue

                # staged_trail: stop-out check first (against whatever
                # stop_price currently is - initial, breakeven, or
                # trailing), then breakeven-flip and gated-trailing, same
                # pattern simulate_strategy's own D1-D3 loop already uses,
                # just with orb's own trailing reference/gate.
                stop_hit = _find_stop_out(this_bar, pos["stop_price"], side)
                if stop_hit is not None:
                    trade_id += 1
                    trades.append({
                        "id": trade_id, "symbol": symbol, "side": close_action,
                        "fill_price": pos["stop_price"], "size": pos["qty"],
                        "timestamp_iso": bar_ts.isoformat(),
                        "exit_reason": "stop_loss" if pos["state"] == "pre_breakeven" else "trailing_stop",
                        "commission": commission_per_trade,
                    })
                    del open_positions[symbol]
                    continue

                price = float(this_bar["Close"].iloc[0])
                initial_risk = (pos["initial_stop"] - pos["entry_price"]) if side == "short" else (pos["entry_price"] - pos["initial_stop"])
                r_multiple = ((pos["entry_price"] - price) if side == "short" else (price - pos["entry_price"])) / initial_risk if initial_risk > 0 else 0.0

                if pos["state"] == "pre_breakeven":
                    decision = cycle._breakeven_decision(pos, exit_cfg, r_multiple)
                    if decision["action"] == "breakeven_flip":
                        pos["stop_price"] = decision["new_stop_price"]
                        pos["state"] = decision["new_state"]

                if pos["state"].startswith("post_breakeven") and r_multiple >= trailing_trigger_r:
                    recent_bars = intraday[intraday.index <= bar_ts].tail(2)
                    candidate = (
                        orb.low_of_last_n_bars(recent_bars, 2) if side == "long" else orb.high_of_last_n_bars(recent_bars, 2)
                    )
                    trail_decision = cycle._trailing_stop_decision(pos, candidate)
                    if trail_decision["action"] == "trail_stop":
                        pos["stop_price"] = trail_decision["new_stop_price"]

            # --- Step 2: scan for new entries across the whole universe ---
            if cycle._within_entry_window(strategy_rules, bar_ts.to_pydatetime()):
                for symbol, intraday in intraday_by_symbol.items():
                    if symbol in open_positions:
                        continue
                    if len(open_positions) >= max_concurrent or entries_today >= max_trades_per_day:
                        break
                    if bar_ts not in intraday.index:
                        continue

                    daily_slice = daily_slice_by_symbol[symbol]
                    if daily_slice is None:
                        continue
                    today_bars_full = day_groups_by_symbol[symbol].get(day)
                    if today_bars_full is None:
                        continue
                    if has_confluence:
                        # entry_confluence's RSI/EMA need the CONTINUOUS
                        # multi-day close series to have actually warmed up
                        # (same "closes across session boundaries"
                        # convention as cycle._compute_ema/_compute_rsi) -
                        # a today-only slice (a handful of bars) starves
                        # them of history and orb._trend_confluence_ok
                        # would see them as insufficient every time (RSI/
                        # EMA are correctly "False, not unknown" on too
                        # little data - see its own docstring). RVOL is
                        # unaffected either way since it always reads
                        # prior_day_bars, never this param, when supplied.
                        intraday_slice = intraday[intraday.index <= bar_ts]
                    else:
                        intraday_slice = today_bars_full[today_bars_full.index <= bar_ts]
                    detail = orb.evaluate_orb_entry(
                        daily_slice, intraday_slice, strategy_rules, side,
                        prior_day_bars=prior_day_bars_by_symbol[symbol],
                        signal_side=strategy_rules.get("signal_side"),
                    )
                    if "error" in detail:
                        filter_stats["insufficient_data"] += 1
                        continue
                    filter_stats["evaluations"] += 1
                    tracked_conditions = ("or_formed", "confirmed", "volatility_ok", "confluence_ok") if has_confluence else ("or_formed", "confirmed", "volatility_ok")
                    for cond in tracked_conditions:
                        if detail.get(cond):
                            filter_stats[cond] += 1
                    if not detail.get("pass"):
                        continue

                    price = detail["price"]
                    initial_stop = detail["initial_stop"]
                    target_price = detail["target_price"]
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
                        "initial_stop": initial_stop, "commission": commission_per_trade,
                        # Which entry model fired (breakout/retest - see
                        # orb.evaluate_orb_entry) - carried through by
                        # perf.pair_trades into each closed pair, so a
                        # strategy diagnostic (analyze_strategy.py) can break
                        # performance down by model, not just in aggregate.
                        "model": detail.get("model"),
                    })
                    open_positions[symbol] = {
                        # "side" is read internally by cycle._trailing_stop_
                        # decision (pos.get("side", "long")) - without it, a
                        # staged_trail ORB SHORT's trailing candidate would
                        # silently be validated/compared as if it were a
                        # long, defaulting wrong.
                        "side": side,
                        "entry_price": price, "initial_stop": initial_stop,
                        "stop_price": initial_stop, "target_price": target_price, "qty": size,
                        "state": "pre_breakeven",
                    }
                    entries_today += 1

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
                "exit_reason": "eod_close", "commission": commission_per_trade,
            })

    return {"trades": trades, "skipped_symbols": skipped, "filter_stats": filter_stats}


def simulate_touch_turn_strategy(
    strategy_rules: dict,
    side: str,
    symbols: list[str],
    start_date,
    end_date,
    portfolio_value: float,
    max_risk_pct: float,
    max_trades_per_day: int,
    commission_per_trade: float = 0.0,
) -> dict:
    """Touch & Turn's own replay loop - dispatched from src/backtest_
    runner.py whenever a strategy's rules carry an "opening_candle" key,
    instead of simulate_strategy/simulate_orb_strategy above. Same
    day-by-day/bar-by-bar structure and {"trades": [...],
    "skipped_symbols": [...], "filter_stats": {...}} return shape as
    simulate_orb_strategy (so perf.py's pipeline works unchanged), but a
    genuinely different entry mechanic: touch_turn.evaluate_touch_turn_
    entry is called once per symbol per day (retried each bar until the
    opening candle has enough bars to form, then not again that day - see
    the "evaluated_today" set below), not on every bar the way the other
    two models' entry signal is. A pass doesn't fill immediately - it
    opens a "resting limit order" state (`pending_orders`) that
    subsequent bars check for a touch (reusing _find_stop_out - despite
    its name, it's exactly "first bar whose range crosses a price, in
    this side's own fill direction", which is exactly the condition a
    resting Buy/Sell Limit's fill needs too) up to time_filter.
    entry_window_minutes later, or cancels unfilled at that deadline -
    mirroring the live engine's own cycle.touch_turn_entry_scan/
    check_pending_touch_turn_orders (a real IBKR GTD limit order there)
    as faithfully as an OHLC-bar replay can. max_concurrent_positions
    only ever gates NEW placements (Step 3 below), never a touch/fill
    already in flight (Step 2) - a real resting broker order doesn't know
    or care about this bot's own concurrency bookkeeping, exactly
    mirroring check_pending_touch_turn_orders never gating a fill either.

    Exit is always "fixed_target_no_trail" for this strategy (see the
    Touch & Turn presets) - the same stop-or-target-whichever-first logic
    as simulate_orb_strategy's own fixed_target_no_trail branch, kept as
    its own copy here rather than shared, same "deliberately separate
    function, don't touch a well-exercised path" reasoning simulate_orb_
    strategy's own docstring explains for why IT doesn't call into
    simulate_strategy either."""
    max_concurrent = strategy_rules["risk"]["max_concurrent_positions"]
    max_position_pct = strategy_rules["risk"]["max_position_size_pct_of_portfolio"] / 100
    action = "BUY" if side == "long" else "SELL"
    close_action = "SELL" if side == "long" else "BUY"
    force_close_time = _parse_force_close_time(strategy_rules)
    atr_period = strategy_rules["liquidity_filter"]["atr_period"]
    min_daily_days = atr_period + 1
    entry_window_minutes = strategy_rules["time_filter"]["entry_window_minutes"]

    daily_by_symbol: dict[str, pd.DataFrame] = {}
    intraday_by_symbol: dict[str, pd.DataFrame] = {}
    skipped = []
    daily_candidates = {}
    for symbol in symbols:
        intraday = backtest_data.load_cached_bars(symbol, BAR_SIZE)
        if intraday is None or intraday.empty:
            skipped.append({"symbol": symbol, "reason": "no cached intraday bars"})
            continue
        window_start = pd.Timestamp(start_date, tz=intraday.index.tz) - pd.Timedelta(days=INTRADAY_LOOKBACK_DAYS)
        window_end = pd.Timestamp(end_date, tz=intraday.index.tz) + pd.Timedelta(days=1)
        intraday = intraday[(intraday.index >= window_start) & (intraday.index < window_end)]
        if intraday.empty:
            skipped.append({"symbol": symbol, "reason": "no cached bars in the requested date range"})
            continue
        daily_candidates[symbol] = intraday

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

    filter_stats = {"evaluations": 0, "insufficient_data": 0, "liquidity_ok": 0, "bias_match": 0}

    if not intraday_by_symbol:
        return {"trades": [], "skipped_symbols": skipped, "filter_stats": filter_stats}

    all_days = sorted(set().union(*[
        set(_trading_days(intraday, start_date, end_date)) for intraday in intraday_by_symbol.values()
    ]))
    day_groups_by_symbol = {
        symbol: dict(tuple(intraday.groupby(intraday.index.date)))
        for symbol, intraday in intraday_by_symbol.items()
    }

    trades = []
    trade_id = 0
    for day in all_days:
        open_positions: dict[str, dict] = {}
        pending_orders: dict[str, dict] = {}
        evaluated_today: set[str] = set()
        entries_today = 0

        daily_slice_by_symbol = {
            symbol: _daily_as_of_light(daily_by_symbol[symbol], day, min_daily_days) for symbol in intraday_by_symbol
        }

        day_bar_times = set()
        for intraday in intraday_by_symbol.values():
            day_bars = intraday[intraday.index.date == day]
            regular_bars = day_bars[day_bars.index.time >= dt_time(9, 30)]
            day_bar_times.update(regular_bars.index)
        sorted_times = sorted(day_bar_times)

        for bar_ts in sorted_times:
            if bar_ts.time() >= force_close_time:
                for symbol, pos in list(open_positions.items()):
                    trade_id += 1
                    trades.append({
                        "id": trade_id, "symbol": symbol, "side": close_action,
                        "fill_price": float(intraday_by_symbol[symbol].loc[bar_ts, "Close"])
                        if bar_ts in intraday_by_symbol[symbol].index else pos["entry_price"],
                        "size": pos["qty"], "timestamp_iso": bar_ts.isoformat(),
                        "exit_reason": "eod_close", "commission": commission_per_trade,
                    })
                open_positions.clear()
                pending_orders.clear()  # nothing left resting once the trading day's over
                break

            # --- Step 1: manage every currently open (FILLED) position - fixed target only, no trailing ---
            for symbol in list(open_positions.keys()):
                intraday = intraday_by_symbol[symbol]
                if bar_ts not in intraday.index:
                    continue
                pos = open_positions[symbol]
                this_bar = intraday.loc[[bar_ts]]
                stop_hit = _find_stop_out(this_bar, pos["stop_price"], side)
                target_hit = _find_target_out(this_bar, pos["target_price"], side)
                if stop_hit is not None or target_hit is not None:
                    # Same same-bar-collision simplification as simulate_orb_
                    # strategy's own fixed_target_no_trail branch (target
                    # wins on a tie - see its comment for the full reasoning).
                    hit_target = target_hit is not None and (stop_hit is None or target_hit <= stop_hit)
                    trade_id += 1
                    trades.append({
                        "id": trade_id, "symbol": symbol, "side": close_action,
                        "fill_price": pos["target_price"] if hit_target else pos["stop_price"],
                        "size": pos["qty"], "timestamp_iso": bar_ts.isoformat(),
                        "exit_reason": "target" if hit_target else "stop_loss",
                        "commission": commission_per_trade,
                    })
                    del open_positions[symbol]

            # --- Step 2: check every resting order for a touch, or expiry - never gated by max_concurrent (see docstring) ---
            for symbol in list(pending_orders.keys()):
                intraday = intraday_by_symbol[symbol]
                po = pending_orders[symbol]
                if bar_ts >= po["expiry_ts"]:
                    del pending_orders[symbol]
                    continue
                if bar_ts not in intraday.index:
                    continue
                this_bar = intraday.loc[[bar_ts]]
                touch = _find_stop_out(this_bar, po["limit_price"], side)
                if touch is None:
                    continue
                trade_id += 1
                trades.append({
                    "id": trade_id, "symbol": symbol, "side": action,
                    "fill_price": po["limit_price"], "size": po["qty"], "timestamp_iso": bar_ts.isoformat(),
                    "initial_stop": po["initial_stop"], "commission": commission_per_trade,
                })
                open_positions[symbol] = {
                    "side": side, "entry_price": po["limit_price"], "initial_stop": po["initial_stop"],
                    "stop_price": po["initial_stop"], "target_price": po["target_price"], "qty": po["qty"],
                    "state": "pre_breakeven",
                }
                entries_today += 1
                del pending_orders[symbol]

            # --- Step 3: evaluate the gate once per symbol per day, retried each bar until decidable ---
            for symbol, intraday in intraday_by_symbol.items():
                if symbol in evaluated_today or symbol in open_positions or symbol in pending_orders:
                    continue
                if bar_ts not in intraday.index:
                    continue
                daily_slice = daily_slice_by_symbol[symbol]
                if daily_slice is None:
                    continue
                today_bars_full = day_groups_by_symbol[symbol].get(day)
                if today_bars_full is None:
                    continue
                intraday_slice = today_bars_full[today_bars_full.index <= bar_ts]
                detail = touch_turn.evaluate_touch_turn_entry(daily_slice, intraday_slice, strategy_rules, side)
                if "error" in detail:
                    filter_stats["insufficient_data"] += 1
                    continue  # opening candle not formed yet at this bar - retried next bar, not counted as a real evaluation
                evaluated_today.add(symbol)
                filter_stats["evaluations"] += 1
                if detail.get("liquidity_ok"):
                    filter_stats["liquidity_ok"] += 1
                if detail.get("bias") == side:
                    filter_stats["bias_match"] += 1
                if not detail.get("pass"):
                    continue
                if len(open_positions) + len(pending_orders) >= max_concurrent or entries_today >= max_trades_per_day:
                    continue

                limit_price, initial_stop, target_price = detail["limit_price"], detail["initial_stop"], detail["target_price"]
                r = abs(limit_price - initial_stop)
                if r <= 0:
                    continue
                risk_dollars = portfolio_value * (max_risk_pct / 100)
                size_by_risk = math.floor(risk_dollars / r)
                size_by_cap = math.floor(portfolio_value * max_position_pct / limit_price)
                size = min(size_by_risk, size_by_cap)
                if size < 1:
                    continue

                session_open_ts = pd.Timestamp.combine(day, dt_time(9, 30)).tz_localize(intraday.index.tz)
                pending_orders[symbol] = {
                    "limit_price": limit_price, "target_price": target_price, "initial_stop": initial_stop,
                    "qty": size, "expiry_ts": session_open_ts + pd.Timedelta(minutes=entry_window_minutes),
                }

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
                "exit_reason": "eod_close", "commission": commission_per_trade,
            })

    return {"trades": trades, "skipped_symbols": skipped, "filter_stats": filter_stats}
