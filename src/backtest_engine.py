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
from src import backtest_data, entry_metrics, es_filter, orb, touch_turn

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


def _last_completed_trading_day(today: date) -> date:
    """The most recent weekday whose regular session has already fully
    closed as of `today` - Friday when `today` is Sat/Sun/Mon, otherwise
    just yesterday. Used to decide whether a cached daily-bar tail is
    "fresh enough" without re-asking yfinance - a plain "yesterday"
    threshold is wrong on a Sat/Sun/Mon (Friday's own bar is still the
    freshest one that COULD exist, but "yesterday" is a non-trading day
    then), which was needlessly re-triggering a full-universe yfinance
    re-fetch attempt on every single backtest run over an entire weekend.
    Doesn't know about exchange holidays (a full market calendar is more
    than this needs) - a holiday just means one extra, harmless re-fetch
    attempt that yfinance correctly returns empty for (see fetch_daily_
    bars' own `if fresh.empty: return cached` below), same as it always
    has, just far less often than every weekend."""
    offset = {5: 1, 6: 2, 0: 3}.get(today.weekday(), 1)
    return today - timedelta(days=offset)


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
        if cached.index.max().date() >= _last_completed_trading_day(date.today()):
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


def _update_excursion(pos: dict, bar) -> None:
    """Tracks the best (MFE) and worst (MAE) price seen while a position
    has been open, off this bar's High/Low rather than just its Close -
    same intrabar-realism principle _find_stop_out already applies to
    stops, since the real max/min move within a bar can go well past
    where it closed. `pos["mfe_price"]`/`pos["mae_price"]` start at
    entry_price when a position opens, so MFE/MAE are both exactly 0
    until the first tick this runs against - see each simulate_*
    function's own Step 1 (called every tick a position is open,
    including the one it's about to close on, so a stop-out that gapped
    through still shows the real touch in MAE, not just the stop level)."""
    high = float(bar["High"])
    low = float(bar["Low"])
    if pos["side"] == "short":
        pos["mfe_price"] = min(pos["mfe_price"], low)
        pos["mae_price"] = max(pos["mae_price"], high)
    else:
        pos["mfe_price"] = max(pos["mfe_price"], high)
        pos["mae_price"] = min(pos["mae_price"], low)


def _dynamic_risk_reduction_check(pos: dict, drr: dict, intraday: pd.DataFrame, bar_ts: pd.Timestamp, side: str) -> dict:
    """ORB Long V8/V9's own "V6 Risk Event" condition-level evaluation
    (see EXTRA_STRATEGY_PRESETS' own v8/v9 comment in src/db.py, "Fix and
    Validate ORB Long V8 Dynamic Risk Reduction") - the SAME 10-minute
    rule src/telemetry_engine.py's own "Early Failure V6" already
    evaluates POST-HOC on already-closed trades (see its own _early_
    failure_v6), but computed LIVE here instead, once per position, at
    the first bar >= drr["scheduled_ts"] (see _evaluate_v6_risk_event,
    the only caller). This is the only place in this codebase RSI/EMA9
    are computed DURING an open position's own life - telemetry_engine.py
    is explicitly passive/read-only/post-hoc (see its own module
    docstring: "NEVER read by any strategy's entry/exit decision") and is
    never imported or called from here.

    Reuses orb._compute_rsi_series/_compute_ema_series over a trailing
    orb._CONFLUENCE_LOOKBACK_BARS window of the symbol's own continuous
    multi-day closes - the exact same windowed-lookback convention orb.
    _trend_confluence_ok already uses for entry-time RSI/EMA (see its own
    docstring for why windowing doesn't change the converged result) -
    rather than telemetry_engine.py's own indicator frame, which computes
    over a symbol's ENTIRE cached history in one call. Uses ONLY `intraday`
    rows at or before `bar_ts` and `pos["mfe_price"]` (already intrabar-
    accurate as of this exact bar via _update_excursion, called before
    this function ever runs) - no future bar, no final trade outcome, no
    final MFE/MAE is ever read here (see the spec's own "No Look-Ahead
    Test"). Values are never rounded before the condition comparisons
    below - only _round() at export/display time elsewhere.

    Every condition (condition_current_r/condition_rsi_delta/condition_
    returned_inside_or/condition_lost_ema9/condition_mfe) is computed
    INDEPENDENTLY, never short-circuited, so a non-triggering trade's own
    audit record can show exactly which condition(s) failed - None (never
    a fabricated pass/fail) wherever its own required input couldn't be
    computed at all (insufficient RSI/EMA warmup, initial_risk_dist <= 0,
    missing opening-range bounds) - same "insufficient data means False,
    not unknown" convention orb._rsi_trending/_trend_confluence_ok already
    establish for `all_conditions_passed`/`triggered` themselves (a None
    condition can never contribute a pass), while still being reported as
    None (not False) in its own condition_* field so "genuinely failed"
    and "couldn't be evaluated" are never visually conflated - see
    `required_data_complete` for the single all-or-nothing data-
    completeness flag reporting/reconciliation actually needs."""
    cfg = drr["cfg"]
    entry_price, initial_stop = pos["entry_price"], pos["initial_stop"]
    initial_risk_dist = (initial_stop - entry_price) if side == "short" else (entry_price - initial_stop)
    sign = 1 if side == "long" else -1

    window = intraday[intraday.index <= bar_ts]["Close"].iloc[-orb._CONFLUENCE_LOOKBACK_BARS:]
    current_price = float(window.iloc[-1])
    rsi_series = orb._compute_rsi_series(window, cfg.get("rsi_period", 14))
    ema_series = orb._compute_ema_series(window, cfg.get("ema_period", 9))
    current_rsi = float(rsi_series.iloc[-1]) if not rsi_series.empty and not pd.isna(rsi_series.iloc[-1]) else None
    current_ema9 = float(ema_series.iloc[-1]) if not ema_series.empty and not pd.isna(ema_series.iloc[-1]) else None

    current_r = ((current_price - entry_price) / initial_risk_dist * sign) if initial_risk_dist > 0 else None
    mfe_r_so_far = (
        (((pos["mfe_price"] - entry_price) if side == "long" else (entry_price - pos["mfe_price"])) / initial_risk_dist)
        if initial_risk_dist > 0 else None
    )
    rsi_delta = (current_rsi - drr["entry_rsi"]) if current_rsi is not None and drr["entry_rsi"] is not None else None
    above_ema9_now = (current_price > current_ema9) if current_ema9 is not None else None
    lost_ema9 = bool(drr["entry_above_ema9"] is True and above_ema9_now is False)
    or_high, or_low = drr.get("or_high"), drr.get("or_low")
    returned_inside_or = bool(or_high is not None and or_low is not None and or_low <= current_price <= or_high)

    condition_current_r = None if current_r is None else bool(current_r <= cfg.get("current_r_max", -0.40))
    condition_rsi_delta = None if rsi_delta is None else bool(rsi_delta <= cfg.get("rsi_delta_max", -5))
    condition_returned_inside_or = returned_inside_or
    condition_lost_ema9 = lost_ema9
    condition_mfe = None if mfe_r_so_far is None else bool(mfe_r_so_far <= cfg.get("mfe_r_max", 0.30))

    required_data_complete = bool(
        current_r is not None and rsi_delta is not None and mfe_r_so_far is not None
        and or_high is not None and or_low is not None
    )
    all_conditions_passed = bool(
        condition_current_r and condition_rsi_delta and condition_returned_inside_or
        and condition_lost_ema9 and condition_mfe
    )
    return {
        "current_r": current_r, "current_rsi": current_rsi, "rsi_delta": rsi_delta,
        "mfe_r_so_far": mfe_r_so_far, "returned_inside_or": returned_inside_or, "lost_ema9": lost_ema9,
        "required_data_complete": required_data_complete,
        "condition_current_r": condition_current_r, "condition_rsi_delta": condition_rsi_delta,
        "condition_returned_inside_or": condition_returned_inside_or, "condition_lost_ema9": condition_lost_ema9,
        "condition_mfe": condition_mfe,
        "all_conditions_passed": all_conditions_passed, "triggered": all_conditions_passed,
    }


def _evaluate_v6_risk_event(pos: dict, intraday: pd.DataFrame, bar_ts: pd.Timestamp, side: str) -> None:
    """ORB v8/v9's own "V6 Risk Event" - called UNCONDITIONALLY for every
    open position, right after _update_excursion and BEFORE the
    management_style dispatch (see Step 1's own call site) - a no-op for
    any strategy that never set pos["dynamic_risk_reduction"] (v4/v4.1/
    v4.2/v4.3/v5/v5.1 never do - see EXTRA_STRATEGY_PRESETS' own v8/v9
    comment in src/db.py), and evaluated exactly ONCE per position,
    mutating pos["dynamic_risk_reduction"] ("drr") in place.

    Running this BEFORE the branch dispatch - not inside the "not yet
    trailing" branch alone, as an earlier version of this function did -
    is what correctly answers "was trailing active AT the V6 evaluation
    timestamp" even when trailing activated on an EARLIER bar than the
    V6 evaluation itself: pos["trail_activated"] is read here exactly as
    it stood at the START of this bar, before anything below has a
    chance to flip it. A trade that closes (via ANY exit path) before
    ever reaching a bar with bar_ts >= drr["scheduled_ts"] simply never
    gets this function to fire drr["checked"] = True at all - _v6_audit_
    record (called once at every actual close site) resolves that
    remaining case to V6_NOT_APPLICABLE_TRADE_CLOSED.

    IMPORTANT (see the spec's own "Do NOT exit the trade... Only risk
    management changes"): this NEVER closes the position - only ever
    tightens pos["hard_stop_price"], and even that only takes effect
    starting the NEXT bar (Step 1's own hard-stop check for THIS bar,
    immediately below wherever this is called from, already ran against
    the PRE-existing level before this function had a chance to update
    it) - see _V6_EXECUTION_ORDER_CONVENTION for why this eliminates any
    same-bar look-ahead ambiguity by construction."""
    drr = pos.get("dynamic_risk_reduction")
    if drr is None or drr["checked"] or bar_ts < drr["scheduled_ts"]:
        return
    drr["checked"] = True
    drr["actual_ts"] = bar_ts

    if pos.get("trail_activated"):
        drr["trail_active_at_evaluation"] = True
        drr["reason"] = "V6_NOT_APPLICABLE_TRAILING_ACTIVE"
        drr["stop_after_r"] = drr["initial_hard_stop_r"]
        drr["stop_after_price"] = pos.get("hard_stop_price")
        return
    drr["trail_active_at_evaluation"] = False

    result = _dynamic_risk_reduction_check(pos, drr, intraday, bar_ts, side)
    drr.update(result)
    drr["stop_before_price"] = pos.get("hard_stop_price")

    if not result["required_data_complete"]:
        drr["reason"] = "V6_MISSING_REQUIRED_DATA"
        drr["stop_after_r"] = drr["initial_hard_stop_r"]
        drr["stop_after_price"] = drr["stop_before_price"]
        return

    if not result["triggered"]:
        drr["reason"] = "V6_NOT_TRIGGERED"
        drr["stop_after_r"] = drr["initial_hard_stop_r"]
        drr["stop_after_price"] = drr["stop_before_price"]
        return

    # Triggered - compute the requested adjusted stop off the trade's own
    # authoritative entry price/initial risk width (never a moving
    # reference), and apply it ONLY if it's actually tighter than
    # whatever is already active ("never loosen a stop" - the same one-
    # way rule cycle._trailing_stop_decision already applies to trailing
    # elsewhere in this file).
    new_hard_stop_r = drr["cfg"]["new_hard_stop_R"]
    initial_risk_dist = (
        (pos["initial_stop"] - pos["entry_price"]) if side == "short" else (pos["entry_price"] - pos["initial_stop"])
    )
    requested_price = None
    if initial_risk_dist > 0:
        requested_price = (
            pos["entry_price"] + new_hard_stop_r * initial_risk_dist if side == "short"
            else pos["entry_price"] - new_hard_stop_r * initial_risk_dist
        )
    drr["requested_adjusted_stop_r"] = new_hard_stop_r
    drr["requested_adjusted_stop_price"] = requested_price

    if requested_price is None or "hard_stop_price" not in pos:
        # Can't apply without a real hard_stop_price to compare against
        # (e.g. a future strategy sets dynamic_risk_reduction without
        # hard_stop_R) - not a "triggered" outcome in any meaningful
        # sense, since there is nothing to tighten.
        drr["reason"] = "V6_NOT_TRIGGERED"
        drr["stop_after_r"] = drr["initial_hard_stop_r"]
        drr["stop_after_price"] = drr["stop_before_price"]
        return

    is_tighter = (requested_price > pos["hard_stop_price"]) if side == "long" else (requested_price < pos["hard_stop_price"])
    if is_tighter:
        pos["hard_stop_price"] = requested_price
        drr["reason"] = "V6_TRIGGERED_STOP_TIGHTENED"
        drr["stop_after_r"] = new_hard_stop_r
        drr["stop_after_price"] = requested_price
    else:
        drr["reason"] = "V6_TRIGGERED_ALREADY_TIGHTER"
        drr["stop_after_r"] = drr["initial_hard_stop_r"]
        drr["stop_after_price"] = drr["stop_before_price"]


# Documented once, referenced by both _evaluate_v6_risk_event's own
# docstring and every _v6_audit_record's own "v6_execution_order_
# convention" field (see the spec's own "Critical Intrabar Validation").
_V6_EXECUTION_ORDER_CONVENTION = (
    "Conservative, no look-ahead: the V6 decision is evaluated at the close of the scheduled bar and applied to "
    "pos['hard_stop_price'] only AFTER that same bar's own stop-out check has already run against the PRE-existing "
    "(untightened) stop level. The tightened level is therefore only ever checked against price movement in bars "
    "STRICTLY AFTER the V6 decision bar - that decision bar's own High/Low is never evaluated against the newly-"
    "tightened level, which eliminates any need to guess intrabar ordering within that one bar by construction "
    "(v6_same_bar_stop_ambiguity is therefore always False)."
)


def _v6_audit_record(pos: dict, exit_reason: str | None, fill_price: float | None, exit_ts) -> dict | None:
    """Builds the COMPLETE V6 Risk Event audit record for one closed
    trade (every field the spec's own "Mandatory Trade-Level Audit
    Fields" / "V6 Risk Event Audit" sheet needs), from whatever state
    _evaluate_v6_risk_event already accumulated on pos["dynamic_risk_
    reduction"] over the position's life. Called once, right before
    EVERY trades.append({...}) call in simulate_orb_strategy (every exit
    path: original/tightened hard stop, trailing stop, eod_close, force_
    close), so every exit path reports these fields identically. None
    for a strategy that never opted into dynamic_risk_reduction at all
    (v4/v4.1/v4.2/v4.3/v5/v5.1) - the caller spreads this dict's own keys
    onto the trade record only when it isn't None."""
    drr = pos.get("dynamic_risk_reduction")
    if drr is None:
        return None
    if not drr["checked"]:
        # The ONLY way _evaluate_v6_risk_event (called on every open-
        # position bar, in both the trailing-active and not-yet-trailing
        # cases - see its own docstring) can have never set checked=True
        # is the trade itself closing before any bar ever reached
        # drr["scheduled_ts"].
        drr["checked"] = True
        drr["reason"] = "V6_NOT_APPLICABLE_TRADE_CLOSED"
        drr["stop_after_r"] = drr["initial_hard_stop_r"]
        drr["stop_after_price"] = pos.get("hard_stop_price")

    reason = drr.get("reason")
    evaluated = reason in ("V6_NOT_TRIGGERED", "V6_TRIGGERED_ALREADY_TIGHTER", "V6_TRIGGERED_STOP_TIGHTENED")
    applicable = reason not in ("V6_NOT_APPLICABLE_TRADE_CLOSED", "V6_NOT_APPLICABLE_TRAILING_ACTIVE")
    triggered = reason in ("V6_TRIGGERED_ALREADY_TIGHTER", "V6_TRIGGERED_STOP_TIGHTENED")
    stop_changed = reason == "V6_TRIGGERED_STOP_TIGHTENED"

    actual_ts = drr.get("actual_ts")
    scheduled_ts = drr["scheduled_ts"]
    trail_activation_ts = pos.get("trail_activation_ts")

    # Adjusted-stop-hit outcome fields - True only when this trade's own
    # ACTUAL exit was a hard_stop AND the stop had genuinely been
    # tightened beforehand (never inferred from price alone) - pos[
    # "hard_stop_price"]/fill_price are always exactly equal for a real
    # hard_stop exit (see the exit-site code below, "fill_price": pos[
    # "hard_stop_price"]), so no separate price-match check is needed.
    adjusted_stop_hit = bool(stop_changed and exit_reason == "hard_stop")
    if exit_reason == "trailing_stop":
        protective_stop_source = "V4_2_TRAILING_STOP"
    elif exit_reason == "hard_stop":
        protective_stop_source = "V6_ADJUSTED_HARD_STOP" if stop_changed else "INITIAL_HARD_STOP"
    else:
        protective_stop_source = None

    return {
        "v6_scheduled_evaluation_timestamp": scheduled_ts.isoformat(),
        "v6_actual_evaluation_timestamp": actual_ts.isoformat() if actual_ts is not None else None,
        "v6_evaluation_delay_seconds": (actual_ts - scheduled_ts).total_seconds() if actual_ts is not None else None,
        "v6_evaluated": evaluated,
        "v6_applicable": applicable,
        "v6_not_applicable_reason": reason if not applicable else None,
        "trade_open_at_v6_evaluation": actual_ts is not None,
        "trail_active_at_v6_evaluation": drr.get("trail_active_at_evaluation"),
        "trail_activation_timestamp": trail_activation_ts.isoformat() if trail_activation_ts is not None else None,
        "trail_activated_after_v6": bool(
            actual_ts is not None and trail_activation_ts is not None and trail_activation_ts > actual_ts
        ),
        "v6_current_r": drr.get("current_r"), "v6_entry_rsi": drr.get("entry_rsi"),
        "v6_current_rsi": drr.get("current_rsi"), "v6_rsi_delta": drr.get("rsi_delta"),
        "v6_returned_inside_opening_range": drr.get("returned_inside_or"), "v6_lost_ema9": drr.get("lost_ema9"),
        "v6_mfe_r_so_far": drr.get("mfe_r_so_far"), "v6_required_data_complete": drr.get("required_data_complete"),
        "v6_triggered": triggered,
        "condition_trade_open": actual_ts is not None,
        "condition_trailing_not_active": (drr.get("trail_active_at_evaluation") is False) if actual_ts is not None else None,
        "condition_current_r": drr.get("condition_current_r"), "condition_rsi_delta": drr.get("condition_rsi_delta"),
        "condition_returned_inside_or": drr.get("condition_returned_inside_or"),
        "condition_lost_ema9": drr.get("condition_lost_ema9"), "condition_mfe": drr.get("condition_mfe"),
        "all_conditions_passed": bool(drr.get("all_conditions_passed", False)),
        "stop_before_v6_r": drr.get("initial_hard_stop_r"), "stop_before_v6_price": drr.get("stop_before_price"),
        "requested_adjusted_stop_r": drr.get("requested_adjusted_stop_r", drr["cfg"].get("new_hard_stop_R")),
        "requested_adjusted_stop_price": drr.get("requested_adjusted_stop_price"),
        "stop_after_v6_r": drr.get("stop_after_r"), "stop_after_v6_price": drr.get("stop_after_price"),
        "v6_stop_changed": stop_changed,
        "v6_stop_change_timestamp": actual_ts.isoformat() if stop_changed and actual_ts is not None else None,
        "v6_stop_change_reason": reason,
        "adjusted_stop_hit": adjusted_stop_hit,
        "adjusted_stop_hit_timestamp": exit_ts.isoformat() if adjusted_stop_hit and hasattr(exit_ts, "isoformat") else (exit_ts if adjusted_stop_hit else None),
        "adjusted_stop_fill_price": fill_price if adjusted_stop_hit else None,
        # This codebase's own R-multiple convention is already commission-
        # free (perf.r_multiple is a pure price ratio - commission is
        # tracked separately, in dollars) - "before/after costs" therefore
        # coincide exactly for R; a genuine $ before/after-costs split is
        # reported at the trade-pair level instead (net_pnl_usd vs
        # gross_pnl_usd), not duplicated here.
        # Negated - stop_after_r/requested_adjusted_stop_r are UNSIGNED
        # magnitudes ("how many R away from entry", matching hard_stop_R's
        # own rules_json convention), but a stop-out is always an adverse
        # move by definition - this field instead matches final_r's own
        # SIGNED convention (negative = loss) elsewhere in this codebase,
        # so it's directly comparable to Final R/Delta R in the report.
        "adjusted_stop_exit_r_before_costs": -drr.get("stop_after_r") if adjusted_stop_hit and drr.get("stop_after_r") is not None else None,
        "adjusted_stop_exit_r_after_costs": -drr.get("stop_after_r") if adjusted_stop_hit and drr.get("stop_after_r") is not None else None,
        "protective_stop_source_at_exit": protective_stop_source,
        "v6_same_bar_stop_ambiguity": False,
        "v6_execution_order_convention": _V6_EXECUTION_ORDER_CONVENTION,
    }


\
# ============================================================ ORB v10 ===
# "Dynamic Recovery State Engine After Early-Failure Warning" - the SAME
# 10-minute V6 rule V8/V9 use, but treated as a WARNING rather than an
# immediate stop-tightening trigger: a v10 position that warns keeps
# monitoring at 15/20/30/45 minutes and only ever acts (tightening the
# hard stop to -1.5R) once deterioration is CONFIRMED across the 20m and
# 30m checkpoints (or the 45m final one) - a genuine recovery at any
# checkpoint cancels monitoring immediately, keeping the original -2.5R
# stop. See EXTRA_STRATEGY_PRESETS' own v10 comment in src/db.py.
#
# Entirely opt-in (exit_cfg["dynamic_recovery"]) - v4/v4.1/v4.2/v4.3/v5/
# v5.1/v8/v9 never set this key, so pos never carries "dynamic_recovery"
# for them and every function below is a pure no-op, same "opt-in key,
# absent = untouched" discipline v4.1 -> v4.2's own hard_stop_R (and v8/
# v9's own dynamic_risk_reduction) already established. v10 itself never
# sets dynamic_risk_reduction, so it never touches _evaluate_v6_risk_
# event/_v6_audit_record's own machinery either - the two mechanisms
# coexist in this file without ever both firing for the same trade.

_V10_STATE_NORMAL = "NORMAL"
_V10_STATE_MONITORING_RECOVERY = "MONITORING_RECOVERY"
_V10_STATE_RECOVERED = "RECOVERED"
_V10_STATE_PERSISTENT_FAILURE = "PERSISTENT_FAILURE"
_V10_STATE_TRAILING_ACTIVE = "TRAILING_ACTIVE"
_V10_STATE_CLOSED_BEFORE_EVALUATION = "CLOSED_BEFORE_EVALUATION"
_V10_STATE_MISSING_REQUIRED_DATA = "MISSING_REQUIRED_DATA"
# EARLY_WARNING is a transient, same-call transition straight into
# MONITORING_RECOVERY (see _evaluate_v10_recovery's own idx==0 branch) -
# never actually observed as pos["dynamic_recovery"]["state"] between
# calls, so it never needs its own branch below, only documenting here
# that it's the required state name for the 1-tick window the spec asks
# for.
_V10_TERMINAL_STATES = (
    _V10_STATE_RECOVERED, _V10_STATE_PERSISTENT_FAILURE, _V10_STATE_TRAILING_ACTIVE,
    _V10_STATE_CLOSED_BEFORE_EVALUATION, _V10_STATE_MISSING_REQUIRED_DATA,
)

_V10_CHECKPOINT_OFFSETS_MINUTES = [10, 15, 20, 30, 45]  # index 0 = the 10m V6 warning check


def _v10_point_in_time_features(dr: dict, pos: dict, intraday: pd.DataFrame, bar_ts: pd.Timestamp, side: str) -> dict:
    """Every point-in-time value V10's own R1-R8/D1-D8 signals and audit
    record need, computed ONLY off bars up to and including bar_ts (see
    the spec's own "No Look-Ahead" rule) - same windowed-lookback
    convention (orb._CONFLUENCE_LOOKBACK_BARS) V6's own _dynamic_risk_
    reduction_check already uses for RSI/EMA9, extended here with EMA20
    and session VWAP (orb._compute_vwap_series - the same session-reset
    VWAP formula src.es_filter.py's own market-level version already
    uses, just applied per-symbol instead of to ES)."""
    entry_price, initial_stop = pos["entry_price"], pos["initial_stop"]
    initial_risk_dist = (initial_stop - entry_price) if side == "short" else (entry_price - initial_stop)
    sign = 1 if side == "long" else -1
    cfg = dr["cfg"]

    window = intraday[intraday.index <= bar_ts].iloc[-orb._CONFLUENCE_LOOKBACK_BARS:]
    closes = window["Close"]
    current_price = float(closes.iloc[-1])
    rsi_series = orb._compute_rsi_series(closes, cfg.get("rsi_period", 14))
    ema9_series = orb._compute_ema_series(closes, cfg.get("ema9_period", 9))
    ema20_series = orb._compute_ema_series(closes, cfg.get("ema20_period", 20))
    current_rsi = float(rsi_series.iloc[-1]) if not rsi_series.empty and not pd.isna(rsi_series.iloc[-1]) else None
    current_ema9 = float(ema9_series.iloc[-1]) if not ema9_series.empty and not pd.isna(ema9_series.iloc[-1]) else None
    current_ema20 = float(ema20_series.iloc[-1]) if not ema20_series.empty and not pd.isna(ema20_series.iloc[-1]) else None

    today_bars = window[window.index.date == bar_ts.date()]
    vwap_series = orb._compute_vwap_series(today_bars) if not today_bars.empty else pd.Series(dtype=float)
    current_vwap = float(vwap_series.iloc[-1]) if not vwap_series.empty and not pd.isna(vwap_series.iloc[-1]) else None
    vwap_dist_pct = ((current_price - current_vwap) / current_vwap * 100) if current_vwap else None

    current_r = ((current_price - entry_price) / initial_risk_dist * sign) if initial_risk_dist > 0 else None
    mfe_r = (
        (((pos["mfe_price"] - entry_price) if side == "long" else (entry_price - pos["mfe_price"])) / initial_risk_dist)
        if initial_risk_dist > 0 else None
    )
    mae_r = (
        (((entry_price - pos["mae_price"]) if side == "long" else (pos["mae_price"] - entry_price)) / initial_risk_dist)
        if initial_risk_dist > 0 and pos.get("mae_price") is not None else None
    )
    return {
        "ts": bar_ts, "price": current_price, "rsi": current_rsi, "ema9": current_ema9, "ema20": current_ema20,
        "vwap": current_vwap, "vwap_dist_pct": vwap_dist_pct,
        "current_r": current_r, "mfe_r": mfe_r, "mae_r": mae_r,
        "above_ema9": (current_price > current_ema9) if current_ema9 is not None else None,
        "above_ema20": (current_price > current_ema20) if current_ema20 is not None else None,
        "above_vwap": (current_price > current_vwap) if current_vwap is not None else None,
        "bar_high": float(window["High"].iloc[-1]), "bar_low": float(window["Low"].iloc[-1]),
    }


def _v10_apply_persistent_failure_stop(pos: dict, dr: dict, side: str, bar_ts: pd.Timestamp) -> None:
    """Tightens pos["hard_stop_price"] to the confirmed-persistent-failure
    level (exit_cfg["dynamic_recovery"]["persistent_failure_stop_R"],
    default 1.5) - ONLY if that's actually tighter than whatever is
    already active ("never loosen a stop", the same one-way rule cycle.
    _trailing_stop_decision and _evaluate_v6_risk_event's own tightening
    already apply elsewhere in this file). The decision bar's own hard-
    stop check (Step 1, earlier in this same bar, BEFORE this function
    is ever called - see _evaluate_v10_recovery's own call site) already
    ran against the PRE-existing level, so the newly-tightened level is
    only ever checked starting the NEXT bar - see _V10_EXECUTION_ORDER_
    CONVENTION for why this eliminates same-bar look-ahead by
    construction, same reasoning as V6's own _V6_EXECUTION_ORDER_
    CONVENTION."""
    new_hard_stop_r = dr["cfg"].get("persistent_failure_stop_R", 1.5)
    initial_risk_dist = (
        (pos["initial_stop"] - pos["entry_price"]) if side == "short" else (pos["entry_price"] - pos["initial_stop"])
    )
    requested_price = None
    if initial_risk_dist > 0:
        requested_price = (
            pos["entry_price"] + new_hard_stop_r * initial_risk_dist if side == "short"
            else pos["entry_price"] - new_hard_stop_r * initial_risk_dist
        )
    dr["stop_before_r"] = dr["initial_hard_stop_r"]
    dr["stop_before_price"] = pos.get("hard_stop_price")
    dr["requested_stop_r"] = new_hard_stop_r
    dr["requested_stop_price"] = requested_price

    if requested_price is None or "hard_stop_price" not in pos:
        dr["stop_after_r"] = dr["initial_hard_stop_r"]
        dr["stop_after_price"] = pos.get("hard_stop_price")
        dr["stop_changed"] = False
        return

    is_tighter = (requested_price > pos["hard_stop_price"]) if side == "long" else (requested_price < pos["hard_stop_price"])
    if is_tighter:
        pos["hard_stop_price"] = requested_price
        dr["stop_after_r"] = new_hard_stop_r
        dr["stop_after_price"] = requested_price
        dr["stop_changed"] = True
        dr["stop_change_ts"] = bar_ts
    else:
        dr["stop_after_r"] = dr["initial_hard_stop_r"]
        dr["stop_after_price"] = pos.get("hard_stop_price")
        dr["stop_changed"] = False


# Documented once, referenced by _v10_audit_record's own "v10_execution_
# order_convention" field - same conservative, no-look-ahead convention
# _V6_EXECUTION_ORDER_CONVENTION already documents for V8/V9.
_V10_EXECUTION_ORDER_CONVENTION = (
    "Conservative, no look-ahead: a V10 persistent-failure decision is evaluated at the close of the scheduled "
    "checkpoint bar and applied to pos['hard_stop_price'] only AFTER that same bar's own stop-out check has "
    "already run against the PRE-existing (untightened) stop level. The tightened level is therefore only ever "
    "checked against price movement in bars STRICTLY AFTER the decision bar - that decision bar's own High/Low is "
    "never evaluated against the newly-tightened level, which eliminates any need to guess intrabar ordering "
    "within that one bar by construction (v10_same_bar_ambiguity is therefore always False)."
)


def _evaluate_v10_recovery(pos: dict, intraday: pd.DataFrame, bar_ts: pd.Timestamp, side: str) -> None:
    """V10's own per-bar state-machine driver - called UNCONDITIONALLY
    for every open position, right after _update_excursion/_evaluate_v6_
    risk_event and BEFORE the management_style dispatch (see Step 1's
    own call site), same placement discipline _evaluate_v6_risk_event's
    own docstring explains (trail_activated is read exactly as it stood
    at the START of this bar). A no-op for any strategy that never set
    pos["dynamic_recovery"] (every strategy except V10 itself).

    IMPORTANT (see the spec's own "Do not exit immediately"): this NEVER
    closes the position - only ever tightens pos["hard_stop_price"] via
    _v10_apply_persistent_failure_stop, and even that only takes effect
    starting the NEXT bar."""
    dr = pos.get("dynamic_recovery")
    if dr is None or dr["state"] in _V10_TERMINAL_STATES:
        return

    # Trailing Priority - can happen at ANY time (even before the 10m
    # warning), and always wins outright over whatever V10 checkpoint is
    # pending - see the spec's own "V10 must never delay or block
    # trailing activation" / "Stop all V10 warning and recovery
    # evaluations".
    if pos.get("trail_activated"):
        dr["state"] = _V10_STATE_TRAILING_ACTIVE
        return

    # Continuous per-bar accumulation since the 10m warning fired - volume
    # is tracked cumulatively "since warning" (per the spec's own D8),
    # while interval_high/interval_low reset at every checkpoint so
    # "Higher High/Low Since Previous Checkpoint" (R7/D7) compares THIS
    # interval's own extremes against the PRIOR interval's, not a single
    # running value that can only ever move one direction.
    if dr["warning_fired"] and bar_ts in intraday.index:
        bar = intraday.loc[bar_ts]
        bar_high, bar_low = float(bar["High"]), float(bar["Low"])
        if float(bar["Close"]) >= float(bar["Open"]):
            dr["up_volume_since_warning"] += float(bar.get("Volume") or 0)
        else:
            dr["down_volume_since_warning"] += float(bar.get("Volume") or 0)
        dr["interval_high"] = bar_high if dr["interval_high"] is None else max(dr["interval_high"], bar_high)
        dr["interval_low"] = bar_low if dr["interval_low"] is None else min(dr["interval_low"], bar_low)

    idx = dr["next_checkpoint_idx"]
    offsets = dr["checkpoints"]
    if idx >= len(offsets):
        return
    scheduled_ts = dr["entry_ts"] + pd.Timedelta(minutes=offsets[idx])
    if bar_ts < scheduled_ts:
        return

    dr["next_checkpoint_idx"] = idx + 1
    checkpoint_minutes = offsets[idx]
    feats = _v10_point_in_time_features(dr, pos, intraday, bar_ts, side)
    entry_rsi = dr["entry_rsi"]

    required_ok = bool(
        feats["current_r"] is not None and feats["rsi"] is not None and entry_rsi is not None
        and feats["mfe_r"] is not None and dr.get("or_high") is not None and dr.get("or_low") is not None
    )
    log_row = {
        "checkpoint_minutes": checkpoint_minutes, "scheduled_ts": scheduled_ts, "actual_ts": bar_ts,
        "delay_seconds": (bar_ts - scheduled_ts).total_seconds(),
    }
    if not required_ok:
        dr["state"] = _V10_STATE_MISSING_REQUIRED_DATA
        log_row["result"] = "MISSING_REQUIRED_DATA"
        dr["checkpoint_log"].append(log_row)
        return

    # ---------------------------------------------------- 10m V6 warning
    if idx == 0:
        dr["warning_evaluated"] = True
        returned_inside_or = bool(dr["or_low"] <= feats["price"] <= dr["or_high"])
        lost_ema9 = bool(dr["entry_above_ema9"] is True and feats["above_ema9"] is False)
        v6_conditions = {
            "current_r": feats["current_r"] <= dr["cfg"].get("current_r_max", -0.40),
            "rsi_delta": (feats["rsi"] - entry_rsi) <= dr["cfg"].get("rsi_delta_max", -5),
            "returned_inside_or": returned_inside_or,
            "lost_ema9": lost_ema9,
            "mfe": feats["mfe_r"] <= dr["cfg"].get("mfe_r_max", 0.30),
        }
        warning_triggered = all(v6_conditions.values())
        log_row.update({
            "current_r": feats["current_r"], "rsi": feats["rsi"], "rsi_delta": feats["rsi"] - entry_rsi,
            "mfe_r": feats["mfe_r"], "conditions": v6_conditions, "warning_triggered": warning_triggered,
            "result": "WARNING_TRIGGERED" if warning_triggered else "NO_WARNING",
        })
        dr["checkpoint_log"].append(log_row)
        if not warning_triggered:
            # Standard V4.2 behavior continues untouched - the recovery
            # engine never activates for this trade at all.
            dr["next_checkpoint_idx"] = len(offsets)
            return
        dr["warning_fired"] = True
        dr["warning_snapshot"] = feats
        dr["prev_snapshot"] = feats
        dr["interval_high"] = feats["price"]
        dr["interval_low"] = feats["price"]
        # state passes through EARLY_WARNING -> MONITORING_RECOVERY within
        # this same call, per the spec's own "immediately transition".
        dr["state"] = _V10_STATE_MONITORING_RECOVERY
        return

    # ---------------------------------------------- 15/20/30/45m checkpoints
    warn, prev = dr["warning_snapshot"], dr["prev_snapshot"]
    delta_r_from_warning = feats["current_r"] - warn["current_r"]
    delta_r_from_prev = feats["current_r"] - prev["current_r"]
    rsi_delta_from_entry = feats["rsi"] - entry_rsi
    rsi_change_from_prev = feats["rsi"] - prev["rsi"]
    warn_vwap_dist, prev_vwap_dist, cur_vwap_dist = warn.get("vwap_dist_pct"), prev.get("vwap_dist_pct"), feats.get("vwap_dist_pct")

    this_interval_high, this_interval_low = dr["interval_high"], dr["interval_low"]
    prev_interval_high, prev_interval_low = dr["prev_interval_high"], dr["prev_interval_low"]
    # First post-warning checkpoint (15m) has no PRIOR interval to compare
    # against yet (the warning bar itself is the interval's own start) -
    # "not enough history" is reported as False, never fabricated True.
    higher_high = bool(prev_interval_high is not None and this_interval_high is not None and this_interval_high > prev_interval_high)
    higher_low = bool(prev_interval_low is not None and this_interval_low is not None and this_interval_low > prev_interval_low)

    r1 = delta_r_from_warning >= 0.35
    r2 = delta_r_from_prev >= 0.20
    r3 = (feats["rsi"] - warn["rsi"]) >= 5
    r4 = feats["above_ema9"] is True
    reclaimed_vwap = bool(feats["above_vwap"] is True and warn["above_vwap"] is not True)
    vwap_improved_since_warning = bool(warn_vwap_dist is not None and cur_vwap_dist is not None and (cur_vwap_dist - warn_vwap_dist) >= 0.20)
    r5 = bool(reclaimed_vwap or vwap_improved_since_warning)
    # Long-only: "moved back above OR High" and "no longer inside OR and
    # exited to upside" are the SAME condition (there's no way to exit
    # the OR "to the upside" other than crossing or_high) - collapsed here
    # deliberately, not an oversight.
    r6 = bool(feats["price"] > dr["or_high"])
    r7 = bool(higher_high and higher_low)
    r8 = (feats["mfe_r"] - warn["mfe_r"]) >= 0.30
    recovery_score = sum(1 for x in (r1, r2, r3, r4, r5, r6, r7, r8) if x)

    d1 = feats["current_r"] <= -1.25
    d2 = delta_r_from_warning <= -0.30
    d3 = feats["mfe_r"] <= 0.40
    d4 = bool(feats["above_ema9"] is False and feats["above_ema20"] is False)
    vwap_not_improving_vs_prev = bool(
        prev_vwap_dist is None or cur_vwap_dist is None or cur_vwap_dist <= prev_vwap_dist
    )
    d5 = bool(feats["above_vwap"] is False and vwap_not_improving_vs_prev)
    d6 = bool(rsi_delta_from_entry <= -7 and rsi_change_from_prev <= 0)
    d7 = bool(not higher_high and not higher_low)
    d8 = dr["down_volume_since_warning"] > dr["up_volume_since_warning"]
    deterioration_score = sum(1 for x in (d1, d2, d3, d4, d5, d6, d7, d8) if x)

    persistent_failure_confirmed_now = bool(
        recovery_score <= 2 and deterioration_score >= 5
        and feats["current_r"] <= -1.25 and feats["mfe_r"] <= 0.40 and not pos.get("trail_activated")
    )
    recovered_now = bool(recovery_score >= 4 and delta_r_from_warning >= 0.35)

    action = "NONE"
    if checkpoint_minutes == 15:
        if recovery_score >= 4:
            dr["recovery_detected_at_15m"] = True
        # Observational only - the spec's own "Do not exit. Do not
        # tighten the stop." for this checkpoint.
    elif checkpoint_minutes == 20:
        if recovered_now:
            dr["state"], dr["recovery_confirmed_by_score"], action = _V10_STATE_RECOVERED, True, "RECOVERED"
        elif persistent_failure_confirmed_now:
            dr["persistent_failure_candidate_at_20m"] = True
            action = "CANDIDATE_FLAGGED"
    elif checkpoint_minutes == 30:
        if recovered_now:
            dr["state"], dr["recovery_confirmed_by_score"], action = _V10_STATE_RECOVERED, True, "RECOVERED"
        elif dr["persistent_failure_candidate_at_20m"] and persistent_failure_confirmed_now:
            dr["state"] = _V10_STATE_PERSISTENT_FAILURE
            _v10_apply_persistent_failure_stop(pos, dr, side, bar_ts)
            action = "PERSISTENT_FAILURE_STOP_TIGHTENED"
    elif checkpoint_minutes == 45:
        if recovery_score >= 4:  # final checkpoint's own recovery test omits the delta_r_from_warning gate (see spec)
            dr["state"], dr["recovery_confirmed_by_score"], action = _V10_STATE_RECOVERED, True, "RECOVERED"
        elif persistent_failure_confirmed_now:
            dr["state"] = _V10_STATE_PERSISTENT_FAILURE
            _v10_apply_persistent_failure_stop(pos, dr, side, bar_ts)
            action = "PERSISTENT_FAILURE_STOP_TIGHTENED"
        else:
            # "Otherwise: Keep original V4.2 management, Stop V10
            # monitoring" - operationally identical to RECOVERED (both
            # just mean "use the untouched original stop from here on"),
            # so reused rather than inventing a 9th state outside the
            # spec's own required list - recovery_confirmed_by_score
            # stays False so the audit/classification layer can still
            # tell the two apart (an R1-R8-confirmed recovery vs this
            # default fallthrough).
            dr["state"], action = _V10_STATE_RECOVERED, "NO_ACTION_FINAL_CHECKPOINT"

    log_row.update({
        "current_r": feats["current_r"], "delta_r_from_prev": delta_r_from_prev, "delta_r_from_warning": delta_r_from_warning,
        "rsi": feats["rsi"], "rsi_change_from_prev": rsi_change_from_prev, "rsi_change_from_warning": feats["rsi"] - warn["rsi"],
        "mfe_r": feats["mfe_r"], "mae_r": feats["mae_r"],
        "above_ema9": feats["above_ema9"], "above_ema20": feats["above_ema20"], "ema9_above_ema20":
            (feats["ema9"] > feats["ema20"]) if feats["ema9"] is not None and feats["ema20"] is not None else None,
        "above_vwap": feats["above_vwap"], "vwap_dist_pct": cur_vwap_dist,
        "price_inside_opening_range": bool(dr["or_low"] <= feats["price"] <= dr["or_high"]),
        "price_above_opening_range_high": bool(feats["price"] > dr["or_high"]),
        "higher_high_since_prev_checkpoint": higher_high, "higher_low_since_prev_checkpoint": higher_low,
        "up_volume_since_warning": dr["up_volume_since_warning"], "down_volume_since_warning": dr["down_volume_since_warning"],
        "r1": r1, "r2": r2, "r3": r3, "r4": r4, "r5": r5, "r6": r6, "r7": r7, "r8": r8, "recovery_score": recovery_score,
        "d1": d1, "d2": d2, "d3": d3, "d4": d4, "d5": d5, "d6": d6, "d7": d7, "d8": d8, "deterioration_score": deterioration_score,
        "state_before": _V10_STATE_MONITORING_RECOVERY, "state_after": dr["state"], "action_taken": action,
    })
    dr["checkpoint_log"].append(log_row)

    dr["prev_snapshot"] = feats
    dr["prev_interval_high"], dr["prev_interval_low"] = this_interval_high, this_interval_low
    dr["interval_high"], dr["interval_low"] = None, None


def _v10_hard_stop_exit_reason(pos: dict) -> str:
    """"hard_stop" for a plain/untightened stop (V4.2's own original
    -2.5R, matching every other strategy's own convention so downstream
    exit-reason-keyed analytics keep working unchanged) - a DISTINCT
    "v10_persistent_failure_stop" only when THIS exact hard_stop_price is
    the one V10's own PERSISTENT_FAILURE action tightened (see the
    spec's own "use a distinct exit reason", mirrored from V6's own
    reasoning for why v8/v9 do NOT get a separate string - here the spec
    explicitly asks for one, so it gets one)."""
    dr = pos.get("dynamic_recovery")
    if dr is not None and dr.get("state") == _V10_STATE_PERSISTENT_FAILURE and dr.get("stop_changed"):
        return "v10_persistent_failure_stop"
    return "hard_stop"


def _v10_audit_record(pos: dict, exit_reason: str | None, fill_price: float | None, exit_ts) -> dict | None:
    """Builds the V10 audit record for one closed trade, from whatever
    state _evaluate_v10_recovery already accumulated on pos["dynamic_
    recovery"] over the position's life - called once, right before
    EVERY trades.append({...}) call in simulate_orb_strategy (same "every
    exit path reports identically" discipline _v6_audit_record already
    establishes). None for any non-V10 strategy (dynamic_recovery never
    set on pos at all)."""
    dr = pos.get("dynamic_recovery")
    if dr is None:
        return None

    if not dr["warning_evaluated"] and dr["state"] == _V10_STATE_NORMAL:
        # The ONLY way _evaluate_v10_recovery can have never reached even
        # the 10m warning check is the trade closing before any bar ever
        # reached entry_ts + 10 minutes.
        dr["state"] = _V10_STATE_CLOSED_BEFORE_EVALUATION

    warn = dr.get("warning_snapshot") or {}
    adjusted_stop_hit = bool(dr.get("stop_changed") and exit_reason == "v10_persistent_failure_stop")

    return {
        "v10_warning_evaluated": dr["warning_evaluated"],
        "v10_warning_triggered": dr["warning_fired"],
        "v10_state": dr["state"],
        "v10_recovery_confirmed_by_score": dr.get("recovery_confirmed_by_score", False),
        "v10_recovery_detected_at_15m": dr.get("recovery_detected_at_15m", False),
        "v10_persistent_failure_candidate_at_20m": dr.get("persistent_failure_candidate_at_20m", False),
        "10m_current_r": warn.get("current_r"), "10m_rsi": warn.get("rsi"),
        "10m_rsi_delta": (warn["rsi"] - dr["entry_rsi"]) if warn.get("rsi") is not None and dr.get("entry_rsi") is not None else None,
        "10m_mfe_r": warn.get("mfe_r"), "10m_mae_r": warn.get("mae_r"),
        "10m_above_ema9": warn.get("above_ema9"), "10m_above_ema20": warn.get("above_ema20"), "10m_above_vwap": warn.get("above_vwap"),
        "checkpoints_evaluated": len(dr["checkpoint_log"]),
        "checkpoint_log": dr["checkpoint_log"],
        "stop_before_v10_r": dr.get("stop_before_r"), "stop_before_v10_price": dr.get("stop_before_price"),
        "requested_v10_stop_r": dr.get("requested_stop_r"), "requested_v10_stop_price": dr.get("requested_stop_price"),
        "stop_after_v10_r": dr.get("stop_after_r"), "stop_after_v10_price": dr.get("stop_after_price"),
        "v10_stop_changed": bool(dr.get("stop_changed")),
        "v10_stop_change_timestamp": dr["stop_change_ts"].isoformat() if dr.get("stop_change_ts") is not None else None,
        "adjusted_stop_hit": adjusted_stop_hit,
        "adjusted_stop_hit_timestamp": exit_ts.isoformat() if adjusted_stop_hit and hasattr(exit_ts, "isoformat") else (exit_ts if adjusted_stop_hit else None),
        "adjusted_stop_fill_price": fill_price if adjusted_stop_hit else None,
        "v10_same_bar_ambiguity": False,
        "v10_execution_order_convention": _V10_EXECUTION_ORDER_CONVENTION,
    }


def _es_day_groups(es_intraday: pd.DataFrame | None) -> dict:
    """Same per-day split as day_groups_by_symbol, but for the one shared
    ES series every symbol's own tagging reads from (see _es_filter_pass)
    - computed once per simulate_* call, not per symbol. {} (not None)
    when no ES data was supplied, so every caller can just call .get(day)
    without a separate None-check."""
    if es_intraday is None or es_intraday.empty:
        return {}
    return dict(tuple(es_intraday.groupby(es_intraday.index.date)))


def _es_filter_pass(strategy_rules: dict, es_day_groups: dict, day, bar_ts, side: str) -> bool | None:
    """None (not applicable / not evaluable) unless the strategy actually
    opts in via rules["es_vwap_filter"] (see the 8 named presets this was
    added to) AND ES's own bars for `day` were supplied - a strategy
    without the flag, or a backtest run with no es_intraday at all,
    always tags every entry None so "before" and "after" filter stats
    come out identical (see src/trade_diagnostics.py's own es_filter_
    report, which treats None the same as "not rejected"). Otherwise the
    real gate decision - see src/es_filter.py's own docstrings for the
    VWAP/direction math and the fail-open behavior on an early-session
    bar with no computable VWAP yet."""
    if not strategy_rules.get("es_vwap_filter") or not es_day_groups:
        return None
    es_today = es_day_groups.get(day)
    if es_today is None:
        return None
    es_bars_so_far = es_today[es_today.index <= bar_ts]
    direction = es_filter.compute_market_direction(es_bars_so_far)
    return es_filter.check(direction, side)["allowed"]


def _quality_filters_pass(entry_ctx: dict, detail: dict, quality_cfg: dict) -> tuple[bool, str | None]:
    """Entry-quality gate for a strategy that opts in via rules["quality_
    filters"] (currently ORB Long v5 only - see EXTRA_STRATEGY_PRESETS'
    own v5 comment in src/db.py) - every other ORB strategy has no such
    key, so this function is never even called for them (see the call
    site's own `if quality_cfg:` guard). Reads entirely off entry_ctx
    (src/entry_metrics.py's own already-computed output for this exact
    candidate) and detail (orb.evaluate_orb_entry's own result) - never
    recomputes anything, so the number a filter gates on is always
    identical to the number that ends up stored/reported for the trade.

    Returns (passed, name-of-first-failing-filter) - only the FIRST
    filter (in the fixed order below) that a candidate fails is charged
    in filter_stats at the call site, so a candidate that would have
    failed multiple filters isn't double-counted against all of them;
    which filter "actually" rejected it is inherently ambiguous once
    more than one would - reporting only the first is a simple,
    deterministic convention, not a claim that the others didn't also
    apply. Checking every configured sub-key is independently optional -
    a key not present in quality_cfg is simply not checked, so an
    ablation variant (this same rules_json with one sub-key omitted -
    see analyze_v5_ablation.py) reuses this exact function unchanged."""
    if "pullbacks_max" in quality_cfg:
        pb = entry_ctx.get("pullbacks_before_entry")
        if pb is None or not (0 <= pb <= quality_cfg["pullbacks_max"]):
            return False, "pullbacks"
    if quality_cfg.get("es_above_vwap_required"):
        if entry_ctx.get("es_above_vwap") is not True:
            return False, "es_direction"
    if "es_vwap_dist_pct_min" in quality_cfg or "es_vwap_dist_pct_max" in quality_cfg:
        dist = entry_ctx.get("es_vwap_dist_pct")
        lo = quality_cfg.get("es_vwap_dist_pct_min", float("-inf"))
        hi = quality_cfg.get("es_vwap_dist_pct_max", float("inf"))
        if dist is None or not (lo <= dist <= hi):
            return False, "es_vwap_distance"
    if "atr_pct_min" in quality_cfg or "atr_pct_max" in quality_cfg:
        atr_pct = detail.get("atr_pct")
        lo = quality_cfg.get("atr_pct_min", float("-inf"))
        hi = quality_cfg.get("atr_pct_max", float("inf"))
        if atr_pct is None or not (lo <= atr_pct <= hi):
            return False, "atr"
    if "breakout_atr_ratio_min" in quality_cfg or "breakout_atr_ratio_max" in quality_cfg:
        ratio = entry_ctx.get("breakout_candle_range_atr_ratio")
        lo = quality_cfg.get("breakout_atr_ratio_min", float("-inf"))
        hi = quality_cfg.get("breakout_atr_ratio_max", float("inf"))
        if ratio is None or not (lo <= ratio <= hi):
            return False, "breakout_atr_ratio"
    return True, None


def _quality_score(entry_ctx: dict, detail: dict, conditions: dict) -> tuple[int, dict]:
    """Score-based counterpart to _quality_filters_pass (currently ORB
    Long v5.1 only - see EXTRA_STRATEGY_PRESETS' own v5.1 comment in
    src/db.py): each of up to 5 independently-optional conditions below
    contributes at most 1 point (same "key not present in `conditions` ->
    not checked at all" convention as _quality_filters_pass), returning
    (total score, {condition_name: bool}) rather than a single pass/fail -
    the caller decides its own min-score threshold, and the per-condition
    dict is what the Quality Score Analysis reporting needs to explain
    WHY a trade scored what it did, not just the number.

    Same fail-closed convention as _quality_filters_pass: missing/None
    data scores that condition as failed (0), never skipped - this is a
    backtest-only research scoring system, not a live safety gate, so
    "can't verify -> don't award the point" is the more defensible
    default."""
    checks = {}
    if "es_vwap_dist_pct_min" in conditions or "es_vwap_dist_pct_max" in conditions:
        dist = entry_ctx.get("es_vwap_dist_pct")
        lo = conditions.get("es_vwap_dist_pct_min", float("-inf"))
        hi = conditions.get("es_vwap_dist_pct_max", float("inf"))
        checks["es_vwap_distance"] = dist is not None and lo <= dist <= hi
    if "atr_pct_min" in conditions or "atr_pct_max" in conditions:
        atr_pct = detail.get("atr_pct")
        lo = conditions.get("atr_pct_min", float("-inf"))
        hi = conditions.get("atr_pct_max", float("inf"))
        checks["atr"] = atr_pct is not None and lo <= atr_pct <= hi
    if "breakout_atr_ratio_min" in conditions or "breakout_atr_ratio_max" in conditions:
        ratio = entry_ctx.get("breakout_candle_range_atr_ratio")
        lo = conditions.get("breakout_atr_ratio_min", float("-inf"))
        hi = conditions.get("breakout_atr_ratio_max", float("inf"))
        checks["breakout_atr_ratio"] = ratio is not None and lo <= ratio <= hi
    if conditions.get("es_or_direction_bullish"):
        checks["es_or_direction"] = entry_ctx.get("es_or_direction") == "Bullish"
    if conditions.get("es_trend_strength_positive"):
        strength = entry_ctx.get("es_trend_strength")
        checks["es_trend_strength"] = strength is not None and strength > 0
    return sum(checks.values()), checks


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
    es_intraday: pd.DataFrame | None = None,
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
    es_day_groups = _es_day_groups(es_intraday)

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
                    if bar_ts in intraday_by_symbol[symbol].index:
                        _update_excursion(pos, intraday_by_symbol[symbol].loc[bar_ts])
                    trades.append({
                        "id": trade_id, "symbol": symbol, "side": close_action,
                        "fill_price": float(intraday_by_symbol[symbol].loc[bar_ts, "Close"])
                        if bar_ts in intraday_by_symbol[symbol].index else pos["entry_price"],
                        "size": pos["qty"], "timestamp_iso": bar_ts.isoformat(),
                        "exit_reason": "eod_close", "commission": commission_per_trade,
                        "mfe_price": pos["mfe_price"], "mae_price": pos["mae_price"],
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
                _update_excursion(pos, bar)

                stop_hit = _find_stop_out(intraday.loc[[bar_ts]], pos["stop_price"], side)
                if stop_hit is not None:
                    trade_id += 1
                    trades.append({
                        "id": trade_id, "symbol": symbol, "side": close_action,
                        "fill_price": pos["stop_price"], "size": pos["qty"],
                        "timestamp_iso": bar_ts.isoformat(),
                        "exit_reason": "stop_loss" if pos["state"] == "pre_breakeven" else "trailing_stop",
                        "commission": commission_per_trade,
                        "mfe_price": pos["mfe_price"], "mae_price": pos["mae_price"],
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
                        "es_filter_pass": _es_filter_pass(strategy_rules, es_day_groups, day, bar_ts, side),
                    })
                    open_positions[symbol] = {
                        "side": side, "entry_price": price, "initial_stop": initial_stop,
                        "stop_price": initial_stop, "qty": size, "state": "pre_breakeven",
                        "mfe_price": price, "mae_price": price,
                    }
                    entries_today += 1

        # Fallback for the rare case the force_close_time tick was never
        # reached this day (e.g. a holiday-shortened session that ends
        # before force_close_et) — the loop above already handles the
        # normal case. Step 1 already ran (and updated mfe_price/mae_price)
        # for every bar_ts this day's own loop reached, including its very
        # last one, since only the force_close_time branch above skips
        # Step 1 for its own tick - this fallback never fires alongside it.
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
                "mfe_price": pos["mfe_price"], "mae_price": pos["mae_price"],
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


def _staged_trail_exit_reason(pos: dict, exit_cfg: dict) -> str:
    """Maps a staged_trail position's own tracked active_stop_type (see
    the "Detailed Exit Reason Classification" investigation this same
    conversation ran) into the specific label a stop-out should carry,
    instead of the old blanket "the position wasn't pre_breakeven, so
    call it trailing_stop" rule - which conflated a stop-out at the flat
    protective level (never actually trailed) with one at a genuinely
    moved dynamic trail level.

    Deliberately gated on exit_cfg carrying "profit_lock_offset_R" (ORB
    Long v2 / ORB Short v2 today, not their Fade siblings or anything
    else sharing this same staged_trail management_style) - every OTHER
    staged_trail strategy keeps the exact old "stop_loss"/"trailing_stop"
    labels, unconditionally, so this function can never change output
    for a strategy the investigation wasn't asked to touch. pos["state"]/
    ["active_stop_type"] are tracked unconditionally for every staged_
    trail position regardless of this gate (harmless bookkeeping, and the
    new lifecycle fields carried on every trade dict stay meaningful even
    for the Fade variants' PDF/dashboard columns), only the exit_reason
    STRING this returns is conditional."""
    if "profit_lock_offset_R" not in exit_cfg:
        return "stop_loss" if pos["state"] == "pre_breakeven" else "trailing_stop"
    if pos["active_stop_type"] == "initial_stop":
        return "initial_stop_loss"
    if pos["active_stop_type"] == "trailing_stop":
        return "staged_trailing_stop"
    return "profit_lock_stop"


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
    es_intraday: pd.DataFrame | None = None,
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
        # confluence_ok is only actually evaluated once volatility_ok and
        # confirmed have already both passed (see orb.evaluate_orb_entry's
        # own comment - it can't change the pass outcome otherwise, so
        # computing it on every miss was pure waste). That means its rate
        # here is a true conditional/funnel one - "of the checks that
        # reached it, how many passed" - not an independent rate over ALL
        # evaluations like or_formed/confirmed/volatility_ok are;
        # confluence_evaluated is that denominator (how many evaluations
        # actually reached it at all), read by backtest.html's own
        # renderFilterStats instead of the default `evaluations` count.
        filter_stats["confluence_ok"] = 0
        filter_stats["confluence_evaluated"] = 0

    if not intraday_by_symbol:
        return {"trades": [], "skipped_symbols": skipped, "filter_stats": filter_stats}

    all_days = sorted(set().union(*[
        set(_trading_days(intraday, start_date, end_date)) for intraday in intraday_by_symbol.values()
    ]))
    day_groups_by_symbol = {
        symbol: dict(tuple(intraday.groupby(intraday.index.date)))
        for symbol, intraday in intraday_by_symbol.items()
    }
    es_day_groups = _es_day_groups(es_intraday)

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
        # daily_derived_cache_by_symbol memoizes ATR per symbol for today
        # (see orb.evaluate_orb_entry's own daily_derived_cache docstring,
        # same convention as _evaluate_filters_from_bars' one below) - ATR
        # depends only on daily_slice_by_symbol[symbol], never on the tick,
        # so recomputing it fresh on every one of a symbol's ~78 evaluate
        # calls today was pure waste. A fresh dict per (symbol, day), same
        # lifetime as daily_slice_by_symbol.
        daily_derived_cache_by_symbol: dict[str, dict] = {}

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
                    if bar_ts in intraday_by_symbol[symbol].index:
                        _update_excursion(pos, intraday_by_symbol[symbol].loc[bar_ts])
                    trades.append({
                        "id": trade_id, "symbol": symbol, "side": close_action,
                        "fill_price": float(intraday_by_symbol[symbol].loc[bar_ts, "Close"])
                        if bar_ts in intraday_by_symbol[symbol].index else pos["entry_price"],
                        "size": pos["qty"], "timestamp_iso": bar_ts.isoformat(),
                        "exit_reason": "eod_close", "commission": commission_per_trade,
                        "mfe_price": pos["mfe_price"], "mae_price": pos["mae_price"],
                        "profit_lock_activated": pos.get("profit_lock_activated", False),
                        "profit_lock_activated_at_r": pos.get("profit_lock_activated_at_r"),
                        "trail_activated": pos.get("trail_activated", False),
                        "trail_activated_at_r": pos.get("trail_activated_at_r"),
                        "v6_audit": _v6_audit_record(pos, "eod_close", None, bar_ts),
                        "v10_audit": _v10_audit_record(pos, "eod_close", None, bar_ts),
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
                _update_excursion(pos, intraday.loc[bar_ts])

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
                            "mfe_price": pos["mfe_price"], "mae_price": pos["mae_price"],
                        })
                        del open_positions[symbol]
                    continue

                if management_style == "scaled_exit_immediate_trail":
                    # ORB v4: no profit-lock/breakeven stage at all - a v4
                    # position only ever holds one of two stop levels: (1)
                    # the initial (deliberately widened via risk.initial_
                    # stop_r_multiplier) stop, until the partial-profit
                    # target is touched, or (2) a genuinely trailing stop
                    # (same orb.low_of_last_n_bars/high_of_last_n_bars
                    # candidate source as v2/v3's own trailing), activated
                    # on the SAME bar the partial fires - immediately, not
                    # gated behind a separate R threshold, per the spec's
                    # "Immediately activate Trailing Stop on the remaining
                    # half". Stop-out checked first each bar (same
                    # convention as every other management_style here),
                    # against whichever of the two levels is currently in
                    # effect and whatever qty currently remains (already
                    # correctly shrunk below once the partial fires).
                    stop_hit = _find_stop_out(this_bar, pos["stop_price"], side)
                    if stop_hit is not None:
                        trade_id += 1
                        trades.append({
                            "id": trade_id, "symbol": symbol, "side": close_action,
                            "fill_price": pos["stop_price"], "size": pos["qty"],
                            "timestamp_iso": bar_ts.isoformat(),
                            "exit_reason": "staged_trailing_stop" if pos["partial_taken"] else "initial_stop_loss",
                            "commission": commission_per_trade,
                            "mfe_price": pos["mfe_price"], "mae_price": pos["mae_price"],
                            "trail_activated": pos["trail_activated"], "trail_activated_at_r": pos["trail_activated_at_r"],
                        })
                        del open_positions[symbol]
                        continue

                    if not pos["partial_taken"]:
                        partial_hit = _find_target_out(this_bar, pos["partial_target_price"], side)
                        if partial_hit is not None:
                            partial_qty = pos["partial_qty"]
                            if partial_qty > 0:
                                trade_id += 1
                                trades.append({
                                    "id": trade_id, "symbol": symbol, "side": close_action,
                                    "fill_price": pos["partial_target_price"], "size": partial_qty,
                                    "timestamp_iso": bar_ts.isoformat(),
                                    "exit_reason": "partial_profit_take", "commission": commission_per_trade,
                                    "mfe_price": pos["mfe_price"], "mae_price": pos["mae_price"],
                                })
                                pos["qty"] -= partial_qty
                            pos["partial_taken"] = True
                            # "Immediately activate Trailing Stop" is a
                            # state, not just this bar's own trail_decision
                            # outcome - stamped True/at_r the instant the
                            # partial fires (flows into the "Protection"
                            # column/exit_reason_breakdown via trail_
                            # activated(_at_r) exactly like v2/v3's own),
                            # even on a bar whose candidate doesn't yet
                            # improve on the wide stop (still degenerate
                            # for one more bar, see below - trailing is
                            # active, just hasn't moved the stop yet).
                            pos["trail_activated"] = True
                            pos["trail_activated_at_r"] = exit_cfg.get("partial_trigger_R", 1.15)
                            # Trail candidate off the same 2-bar source
                            # v2/v3 already use - price has already run
                            # partial_trigger_R in our favor by now, so the
                            # candidate is almost always valid (better than
                            # the wide initial stop) and an improvement
                            # (better than the current stop), but cycle.
                            # _trailing_stop_decision's own validity/
                            # improvement checks still gate it exactly like
                            # v2/v3 - a degenerate candidate (e.g. too few
                            # bars yet) just leaves the wide stop in place
                            # for one more bar rather than forcing a bad
                            # level.
                            recent_bars = intraday[intraday.index <= bar_ts].tail(2)
                            candidate = (
                                orb.low_of_last_n_bars(recent_bars, 2) if side == "long"
                                else orb.high_of_last_n_bars(recent_bars, 2)
                            )
                            trail_decision = cycle._trailing_stop_decision(pos, candidate)
                            if trail_decision["action"] == "trail_stop":
                                pos["stop_price"] = trail_decision["new_stop_price"]
                    else:
                        recent_bars = intraday[intraday.index <= bar_ts].tail(2)
                        candidate = (
                            orb.low_of_last_n_bars(recent_bars, 2) if side == "long"
                            else orb.high_of_last_n_bars(recent_bars, 2)
                        )
                        trail_decision = cycle._trailing_stop_decision(pos, candidate)
                        if trail_decision["action"] == "trail_stop":
                            pos["stop_price"] = trail_decision["new_stop_price"]
                    continue

                if management_style == "no_stop_delayed_trail":
                    # ORB v4.1: NO initial-stop exit check at all - unlike
                    # every other management_style here, this position is
                    # NEVER closed for adverse price movement (see the
                    # spec's own "Do NOT exit the position because of
                    # adverse price movement"). pos["initial_stop"] is
                    # still computed at entry (same orb.py stop calc/
                    # risk.initial_stop_r_multiplier widening v4 itself
                    # uses - "position sizing rules" stay unchanged per
                    # the spec) but is used ONLY to normalize MFE into R
                    # below and as _trailing_stop_decision's own validity
                    # floor (a trail candidate must still be on the
                    # profitable side of where the original stop would
                    # have been) - never as a live exit trigger.
                    #
                    # ORB v4.2 (opt-in via exit_cfg["hard_stop_R"], see
                    # Step 2's own setup below and EXTRA_STRATEGY_PRESETS'
                    # v4.2 comment in src/db.py) is the SAME state machine
                    # with exactly one extra guard layered in below - v4.1
                    # itself never sets this key, so pos never carries
                    # "hard_stop_price" and this whole branch is a no-op
                    # for it, byte-for-byte the same behavior as before.
                    #
                    # Before trailing activates: no stop-out check at all
                    # UNLESS a hard_stop_price is set (v4.2 only), just
                    # watch MFE (already intrabar High/Low via _update_
                    # excursion above, per the spec's own "use actual
                    # intrabar high/low movement, not candle close"). Once
                    # MFE first reaches trailing_trigger_R, activate - from
                    # that bar on this is IDENTICAL to v4's own trailing
                    # (same orb.low_of_last_n_bars/high_of_last_n_bars(2)
                    # candidate source, same cycle._trailing_stop_decision
                    # gate) with no partial take at all (the full position
                    # trails together, not v4's own half) - same "leave
                    # the wide/no stop alone for one more bar rather than
                    # forcing a bad level" reasoning as v4's own comment
                    # above when the very first candidate isn't valid/
                    # improving yet.
                    if pos["trail_activated"]:
                        # Trailing Priority (V6/V10 alike): hard_stop_price is
                        # never even checked once trailing is active, so
                        # calling these here (before the trailing-stop check
                        # below, which never touches hard_stop_price) carries
                        # no look-ahead risk - only here to let each function
                        # correctly transition to its own "trailing is/became
                        # active" terminal state instead of staying PENDING
                        # forever once a trade reaches this branch.
                        _evaluate_v6_risk_event(pos, intraday, bar_ts, side)
                        _evaluate_v10_recovery(pos, intraday, bar_ts, side)
                        stop_hit = _find_stop_out(this_bar, pos["stop_price"], side)
                        if stop_hit is not None:
                            trade_id += 1
                            trades.append({
                                "id": trade_id, "symbol": symbol, "side": close_action,
                                "fill_price": pos["stop_price"], "size": pos["qty"],
                                "timestamp_iso": bar_ts.isoformat(),
                                "exit_reason": "trailing_stop", "commission": commission_per_trade,
                                "mfe_price": pos["mfe_price"], "mae_price": pos["mae_price"],
                                "trail_activated": True, "trail_activated_at_r": pos["trail_activated_at_r"],
                                "v6_audit": _v6_audit_record(pos, "trailing_stop", pos["stop_price"], bar_ts),
                                "v10_audit": _v10_audit_record(pos, "trailing_stop", pos["stop_price"], bar_ts),
                            })
                            del open_positions[symbol]
                            continue
                        recent_bars = intraday[intraday.index <= bar_ts].tail(2)
                        candidate = (
                            orb.low_of_last_n_bars(recent_bars, 2) if side == "long"
                            else orb.high_of_last_n_bars(recent_bars, 2)
                        )
                        trail_decision = cycle._trailing_stop_decision(pos, candidate)
                        if trail_decision["action"] == "trail_stop":
                            pos["stop_price"] = trail_decision["new_stop_price"]
                    else:
                        # v4.2's hard stop (exit_cfg["hard_stop_R"], e.g.
                        # 2.5) - checked first, same "stop-out check
                        # before anything else" convention every other
                        # management_style branch here already uses,
                        # since a bar that BOTH breaches -2.5R AND crosses
                        # the 1.20R MFE trigger (an extreme-range bar) is
                        # ambiguous about which happened first intrabar -
                        # resolved conservatively (the hard stop wins),
                        # matching this whole feature's own point ("prevent
                        # catastrophic losses"). "hard_stop_price" is only
                        # ever present on pos when exit_cfg actually
                        # carries hard_stop_R (v4.2 only - see Step 2's
                        # own setup below); absent for v4.1 (and everyone
                        # else), so this check is a no-op there.
                        if "hard_stop_price" in pos:
                            hard_stop_hit = _find_stop_out(this_bar, pos["hard_stop_price"], side)
                            if hard_stop_hit is not None:
                                trade_id += 1
                                # exit_reason stays "hard_stop" whether this was
                                # the original -2.5R level or a v8/v9 TIGHTENED
                                # one - never a new exit_reason string, so
                                # anything downstream keyed off exit_reason ==
                                # "hard_stop" (e.g. src.telemetry_engine.py's
                                # own _rule_outcome_category) keeps working
                                # unchanged for v8/v9 trades too; v6_audit (see
                                # EXTRA_STRATEGY_PRESETS' own v8/v9 comment in
                                # src/db.py) is how a report tells the two
                                # apart. V10 is the ONE exception - its own spec
                                # explicitly asks for a distinct exit_reason
                                # when ITS OWN confirmed-persistent-failure stop
                                # is what actually fired (see _v10_hard_stop_
                                # exit_reason's own docstring); a no-op "hard_
                                # stop" for every other strategy, V10 included
                                # before/without a confirmed persistent failure.
                                hard_stop_exit_reason = _v10_hard_stop_exit_reason(pos)
                                trades.append({
                                    "id": trade_id, "symbol": symbol, "side": close_action,
                                    "fill_price": pos["hard_stop_price"], "size": pos["qty"],
                                    "timestamp_iso": bar_ts.isoformat(),
                                    "exit_reason": hard_stop_exit_reason, "commission": commission_per_trade,
                                    "mfe_price": pos["mfe_price"], "mae_price": pos["mae_price"],
                                    "trail_activated": False, "trail_activated_at_r": None,
                                    "v6_audit": _v6_audit_record(pos, "hard_stop", pos["hard_stop_price"], bar_ts),
                                    "v10_audit": _v10_audit_record(pos, hard_stop_exit_reason, pos["hard_stop_price"], bar_ts),
                                })
                                del open_positions[symbol]
                                continue

                        # V6/V10 evaluated HERE - strictly AFTER this bar's
                        # own hard-stop check just above already ran against
                        # the PRE-existing (untightened) level, never before
                        # it. This is what actually makes the "no look-ahead,
                        # only takes effect starting the NEXT bar" claim in
                        # _V6_EXECUTION_ORDER_CONVENTION/_V10_EXECUTION_ORDER_
                        # CONVENTION true - an EARLIER version of this file
                        # called both functions unconditionally before the
                        # whole management_style dispatch (so a same-bar
                        # tightening WAS visible to that same bar's own hard-
                        # stop check, a real look-ahead bug caught while
                        # building V10 and fixed here for V6/V8/V9 too, not
                        # just V10). trail_activated is still read correctly
                        # as "not yet activated this bar" by both functions,
                        # since the mfe_r/trailing-activation check below has
                        # not run yet at this point either.
                        _evaluate_v6_risk_event(pos, intraday, bar_ts, side)
                        _evaluate_v10_recovery(pos, intraday, bar_ts, side)

                        initial_risk = (pos["initial_stop"] - pos["entry_price"]) if side == "short" else (pos["entry_price"] - pos["initial_stop"])
                        mfe_r = (
                            ((pos["entry_price"] - pos["mfe_price"]) if side == "short" else (pos["mfe_price"] - pos["entry_price"]))
                            / initial_risk
                        ) if initial_risk > 0 else 0.0
                        trailing_trigger_r_v41 = exit_cfg.get("trailing_trigger_R", 1.20)
                        if mfe_r >= trailing_trigger_r_v41:
                            pos["trail_activated"] = True
                            pos["trail_activated_at_r"] = trailing_trigger_r_v41
                            # ORB v8/v9 only: records WHEN trailing activated
                            # (see EXTRA_STRATEGY_PRESETS' own v8/v9 comment in
                            # src/db.py) - _v6_audit_record's own "trail_
                            # activation_timestamp"/"trail_activated_after_v6"
                            # fields need this. An unread extra key for v4.1/
                            # v4.2/v4.3 (they never carry "dynamic_risk_
                            # reduction", so nothing ever reads this field for
                            # them) - never spread into the trade record itself.
                            pos["trail_activation_ts"] = bar_ts
                            recent_bars = intraday[intraday.index <= bar_ts].tail(2)
                            candidate = (
                                orb.low_of_last_n_bars(recent_bars, 2) if side == "long"
                                else orb.high_of_last_n_bars(recent_bars, 2)
                            )
                            trail_decision = cycle._trailing_stop_decision(pos, candidate)
                            if trail_decision["action"] == "trail_stop":
                                pos["stop_price"] = trail_decision["new_stop_price"]
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
                        "exit_reason": _staged_trail_exit_reason(pos, exit_cfg),
                        "commission": commission_per_trade,
                        "mfe_price": pos["mfe_price"], "mae_price": pos["mae_price"],
                        "profit_lock_activated": pos["profit_lock_activated"],
                        "profit_lock_activated_at_r": pos["profit_lock_activated_at_r"],
                        "trail_activated": pos["trail_activated"],
                        "trail_activated_at_r": pos["trail_activated_at_r"],
                    })
                    del open_positions[symbol]
                    continue

                price = float(this_bar["Close"].iloc[0])
                initial_risk = (pos["initial_stop"] - pos["entry_price"]) if side == "short" else (pos["entry_price"] - pos["initial_stop"])
                r_multiple = ((pos["entry_price"] - price) if side == "short" else (price - pos["entry_price"])) / initial_risk if initial_risk > 0 else 0.0

                if pos["state"] == "pre_breakeven":
                    # profit_lock_offset_R (ORB Long/Short v2 only - see
                    # cycle._profit_lock_decision's own docstring) triggers
                    # off MFE (this bar's High/Low, already folded into
                    # pos["mfe_price"] by _update_excursion above) instead
                    # of r_multiple's Close-only price - everything else
                    # (the trailing gate below) is untouched, still Close-
                    # based, per the spec's own "do not change the existing
                    # staged trail rules" for that stage.
                    if "profit_lock_offset_R" in exit_cfg:
                        mfe_r = (((pos["entry_price"] - pos["mfe_price"]) if side == "short"
                                  else (pos["mfe_price"] - pos["entry_price"])) / initial_risk if initial_risk > 0 else 0.0)
                        decision = cycle._profit_lock_decision(pos, exit_cfg, mfe_r)
                    else:
                        decision = cycle._breakeven_decision(pos, exit_cfg, r_multiple)
                    if decision["action"] == "breakeven_flip":
                        pos["stop_price"] = decision["new_stop_price"]
                        pos["state"] = decision["new_state"]
                        pos["active_stop_type"] = "protective_stop"
                        pos["profit_lock_activated"] = True
                        pos["profit_lock_activated_at_r"] = exit_cfg["breakeven_trigger_R"]

                if pos["state"].startswith("post_breakeven") and r_multiple >= trailing_trigger_r:
                    recent_bars = intraday[intraday.index <= bar_ts].tail(2)
                    candidate = (
                        orb.low_of_last_n_bars(recent_bars, 2) if side == "long" else orb.high_of_last_n_bars(recent_bars, 2)
                    )
                    trail_decision = cycle._trailing_stop_decision(pos, candidate)
                    if trail_decision["action"] == "trail_stop":
                        pos["stop_price"] = trail_decision["new_stop_price"]
                        # First real trail update - the active stop is now a
                        # genuinely dynamic level, not the flat protective
                        # one anymore (see _staged_trail_exit_reason). Only
                        # set trail_activated_at_r once, the first time -
                        # re-triggering "hold" on later ticks where the
                        # candidate doesn't improve shouldn't re-stamp it.
                        pos["active_stop_type"] = "trailing_stop"
                        if not pos["trail_activated"]:
                            pos["trail_activated"] = True
                            pos["trail_activated_at_r"] = trailing_trigger_r

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
                        daily_derived_cache=daily_derived_cache_by_symbol.setdefault(symbol, {}),
                    )
                    if "error" in detail:
                        filter_stats["insufficient_data"] += 1
                        continue
                    filter_stats["evaluations"] += 1
                    tracked_conditions = ("or_formed", "confirmed", "volatility_ok", "confluence_ok") if has_confluence else ("or_formed", "confirmed", "volatility_ok")
                    for cond in tracked_conditions:
                        if detail.get(cond):
                            filter_stats[cond] += 1
                    # confluence_ok is None (not True/False) when it was
                    # never evaluated (see orb.evaluate_orb_entry's own
                    # comment) - confluence_evaluated counts the ones that
                    # WERE, so confluence_ok's own rate can be read against
                    # the right denominator instead of `evaluations`.
                    if has_confluence and detail.get("confluence_ok") is not None:
                        filter_stats["confluence_evaluated"] += 1
                    if not detail.get("pass"):
                        continue

                    price = detail["price"]
                    initial_stop = detail["initial_stop"]
                    target_price = detail["target_price"]
                    r = (initial_stop - price) if side == "short" else (price - initial_stop)
                    if r <= 0:
                        continue

                    # Point-in-time entry context (see src/entry_metrics.py's
                    # own module docstring for the point-in-time-safety
                    # contract) - computed here (before position sizing/the
                    # quality-filter gate just below) rather than after, so
                    # the SAME computed dict both gates the entry (for a
                    # strategy that opts in via rules["quality_filters"] -
                    # currently ORB Long v5 only) AND ends up stored/
                    # reported for the trade - never two separate
                    # computations that could silently drift apart.
                    # today_bars_full/intraday are each sliced to <= bar_ts
                    # here (they otherwise span the whole day/every date
                    # respectively) - es_bars_so_far mirrors _es_filter_
                    # pass's own identical slice just below, computed once
                    # here since that function doesn't hand its own slice
                    # back.
                    es_today = es_day_groups.get(day)
                    es_bars_so_far = es_today[es_today.index <= bar_ts] if es_today is not None else None
                    entry_ctx = entry_metrics.compute_entry_metrics(
                        side, price, bar_ts, initial_stop,
                        today_bars_full[today_bars_full.index <= bar_ts],
                        intraday[intraday.index <= bar_ts],
                        daily_slice, detail, es_bars_so_far,
                        prior_day_bars=prior_day_bars_by_symbol[symbol],
                    )

                    quality_cfg = strategy_rules.get("quality_filters")
                    if quality_cfg:
                        quality_ok, failed_filter = _quality_filters_pass(entry_ctx, detail, quality_cfg)
                        if not quality_ok:
                            filter_stats[f"quality_reject_{failed_filter}"] = filter_stats.get(f"quality_reject_{failed_filter}", 0) + 1
                            continue

                    # Score-based layer (currently ORB Long v5.1 only, see
                    # EXTRA_STRATEGY_PRESETS' own v5.1 comment in
                    # src/db.py) - runs AFTER the mandatory quality_cfg
                    # gate above (only a candidate that already cleared
                    # every mandatory filter gets scored at all), and is
                    # itself independently optional the same way -
                    # absent for every strategy except v5.1, so this can
                    # only ever narrow v5.1's own trade set. The score is
                    # attached to entry_ctx (so **entry_ctx below carries
                    # it onto the trade record, for the Quality Score
                    # Analysis reporting) whether or not this candidate
                    # ends up rejected by min_score - a rejected one just
                    # never reaches trades.append at all.
                    score_cfg = strategy_rules.get("quality_score_filters")
                    if score_cfg:
                        score, score_checks = _quality_score(entry_ctx, detail, score_cfg.get("conditions", {}))
                        entry_ctx["quality_score"] = score
                        entry_ctx["quality_score_detail"] = score_checks
                        if score < score_cfg.get("min_score", 0):
                            filter_stats["quality_score_reject"] = filter_stats.get("quality_score_reject", 0) + 1
                            continue

                    # position_size_multiplier (currently ORB Long/Short v4
                    # only, at 2.0 - see EXTRA_STRATEGY_PRESETS' own v4
                    # comment in src/db.py) scales UP size_by_risk only -
                    # size_by_cap (max_position_size_pct_of_portfolio, the
                    # account-level guardrail) is computed unchanged and
                    # still wins via min() below if the doubled risk-based
                    # size would exceed it, so the portfolio-pct cap is
                    # never bypassed by this multiplier.
                    risk_dollars = portfolio_value * (max_risk_pct / 100)
                    size_multiplier = strategy_rules["risk"].get("position_size_multiplier", 1.0)
                    size_by_risk = math.floor(risk_dollars * size_multiplier / r)
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
                        "es_filter_pass": _es_filter_pass(strategy_rules, es_day_groups, day, bar_ts, side),
                        **entry_ctx,
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
                        "mfe_price": price, "mae_price": price,
                        # Lifecycle state for _staged_trail_exit_reason - set
                        # unconditionally for every position (fixed_target_
                        # no_trail positions never read/update these, so
                        # they just stay at their initial values), not only
                        # profit_lock_offset_R strategies, so the trade
                        # dict's own lifecycle fields (below) stay
                        # meaningful for every staged_trail strategy's own
                        # report columns even where the exit_reason label
                        # itself isn't remapped.
                        "active_stop_type": "initial_stop",
                        "profit_lock_activated": False, "profit_lock_activated_at_r": None,
                        "trail_activated": False, "trail_activated_at_r": None,
                    }
                    if management_style == "scaled_exit_immediate_trail":
                        # ORB v4: scaled exit, no profit-lock/breakeven stage
                        # at all (see the Step-1 loop's own branch below) -
                        # partial_target_price is a real fill level (like
                        # fixed_target_no_trail's own target_price above),
                        # touched via _find_target_out, not an MFE-only
                        # theoretical level - a partial CLOSE needs an
                        # actual, fillable price. partial_qty is computed
                        # once here off the ORIGINAL size (not recomputed
                        # off pos["qty"] later, which shrinks after the
                        # partial fires) - 0 (skip the split, just activate
                        # trailing on the full remaining qty when touched)
                        # for a position too small to meaningfully halve.
                        partial_trigger_r = exit_cfg.get("partial_trigger_R", 1.15)
                        partial_pct = exit_cfg.get("partial_pct", 0.5)
                        partial_target = (
                            price + partial_trigger_r * r if side == "long" else price - partial_trigger_r * r
                        )
                        partial_qty = math.floor(size * partial_pct)
                        open_positions[symbol]["partial_target_price"] = partial_target
                        open_positions[symbol]["partial_qty"] = partial_qty if 1 <= partial_qty < size else 0
                        open_positions[symbol]["partial_taken"] = False
                    if management_style == "no_stop_delayed_trail":
                        # ORB v4.2 only (exit_cfg["hard_stop_R"], e.g. 2.5)
                        # - a real fillable stop level, same shape as
                        # fixed_target_no_trail's own stop_price/v4's own
                        # partial_target_price above, computed once here
                        # off the SAME initial risk distance `r` position
                        # sizing already used (never recomputed off a
                        # moving reference later). Absent entirely (key
                        # never set on pos) when exit_cfg has no hard_
                        # stop_R - v4.1's own rules_json never sets it, so
                        # this is a pure no-op for v4.1 (see the Step-1
                        # loop's own "hard_stop_price" in pos check).
                        hard_stop_r = exit_cfg.get("hard_stop_R")
                        if hard_stop_r is not None:
                            hard_stop_price = (
                                price + hard_stop_r * r if side == "short" else price - hard_stop_r * r
                            )
                            open_positions[symbol]["hard_stop_price"] = hard_stop_price
                        # ORB v8/v9 only (exit_cfg["dynamic_risk_reduction"] -
                        # see EXTRA_STRATEGY_PRESETS' own v8/v9 comment in src/
                        # db.py). Captures everything _evaluate_v6_risk_event/
                        # _dynamic_risk_reduction_check need once, at entry,
                        # rather than recomputing it later: scheduled_ts (v6_
                        # scheduled_evaluation_timestamp = entry_ts + trigger_
                        # offset_minutes, computed once here per the spec's
                        # own exact formula), or_high/or_low (from `detail`,
                        # the SAME opening range the entry signal itself used
                        # - never re-derived), entry RSI/EMA9 (same windowed-
                        # lookback convention orb._trend_confluence_ok already
                        # uses for entry-time RSI/EMA - see _dynamic_risk_
                        # reduction_check's own docstring), and initial_hard_
                        # stop_r (the strategy's own configured hard_stop_R,
                        # e.g. 2.5 - "Stop Before V6 R" is always this exact
                        # value until/unless V6 actually changes it). None
                        # entirely (key never set on pos) when exit_cfg has no
                        # dynamic_risk_reduction - v4/v4.1/v4.2/v4.3/v5/v5.1's
                        # own rules_json never set it, so this is a pure no-op
                        # for them (see _evaluate_v6_risk_event's own guard).
                        dynamic_cfg = exit_cfg.get("dynamic_risk_reduction")
                        if dynamic_cfg is not None:
                            entry_window = intraday[intraday.index <= bar_ts]["Close"].iloc[-orb._CONFLUENCE_LOOKBACK_BARS:]
                            entry_rsi_series = orb._compute_rsi_series(entry_window, dynamic_cfg.get("rsi_period", 14))
                            entry_ema_series = orb._compute_ema_series(entry_window, dynamic_cfg.get("ema_period", 9))
                            entry_rsi = (
                                float(entry_rsi_series.iloc[-1])
                                if not entry_rsi_series.empty and not pd.isna(entry_rsi_series.iloc[-1]) else None
                            )
                            entry_ema9 = (
                                float(entry_ema_series.iloc[-1])
                                if not entry_ema_series.empty and not pd.isna(entry_ema_series.iloc[-1]) else None
                            )
                            open_positions[symbol]["dynamic_risk_reduction"] = {
                                "cfg": dynamic_cfg, "entry_ts": bar_ts,
                                "scheduled_ts": bar_ts + pd.Timedelta(minutes=dynamic_cfg.get("trigger_offset_minutes", 10)),
                                "or_high": detail.get("or_high"), "or_low": detail.get("or_low"),
                                "entry_rsi": entry_rsi,
                                "entry_above_ema9": (price > entry_ema9) if entry_ema9 is not None else None,
                                "initial_hard_stop_r": exit_cfg.get("hard_stop_R"),
                                "checked": False, "actual_ts": None, "reason": None,
                            }
                        # ORB v10 only (exit_cfg["dynamic_recovery"] - see
                        # EXTRA_STRATEGY_PRESETS' own v10 comment in src/db.py,
                        # "Build ORB Long V10 - Dynamic Recovery State Engine
                        # After Early-Failure Warning"). Mutually exclusive
                        # with dynamic_risk_reduction above in practice (V10
                        # never sets that key, V8/V9 never set this one), but
                        # nothing here actually depends on that - each is
                        # simply absent (None) on `pos` for every strategy
                        # that doesn't opt into it, same "opt-in key, absent =
                        # untouched" discipline as everywhere else in this
                        # file. entry_rsi/entry_above_ema9 computed the exact
                        # same windowed-lookback way as dynamic_risk_
                        # reduction's own (V10's 10-minute check reuses the
                        # unchanged V6 rule verbatim, just as a warning instead
                        # of an immediate trigger - see _evaluate_v10_
                        # recovery's own idx==0 branch).
                        recovery_cfg = exit_cfg.get("dynamic_recovery")
                        if recovery_cfg is not None:
                            v10_entry_window = intraday[intraday.index <= bar_ts]["Close"].iloc[-orb._CONFLUENCE_LOOKBACK_BARS:]
                            v10_entry_rsi_series = orb._compute_rsi_series(v10_entry_window, recovery_cfg.get("rsi_period", 14))
                            v10_entry_ema_series = orb._compute_ema_series(v10_entry_window, recovery_cfg.get("ema9_period", 9))
                            v10_entry_rsi = (
                                float(v10_entry_rsi_series.iloc[-1])
                                if not v10_entry_rsi_series.empty and not pd.isna(v10_entry_rsi_series.iloc[-1]) else None
                            )
                            v10_entry_ema9 = (
                                float(v10_entry_ema_series.iloc[-1])
                                if not v10_entry_ema_series.empty and not pd.isna(v10_entry_ema_series.iloc[-1]) else None
                            )
                            open_positions[symbol]["dynamic_recovery"] = {
                                "cfg": recovery_cfg, "entry_ts": bar_ts,
                                "checkpoints": recovery_cfg.get("checkpoint_offsets_minutes", list(_V10_CHECKPOINT_OFFSETS_MINUTES)),
                                "next_checkpoint_idx": 0, "state": _V10_STATE_NORMAL,
                                "warning_evaluated": False, "warning_fired": False,
                                "warning_snapshot": None, "prev_snapshot": None,
                                "or_high": detail.get("or_high"), "or_low": detail.get("or_low"),
                                "entry_rsi": v10_entry_rsi,
                                "entry_above_ema9": (price > v10_entry_ema9) if v10_entry_ema9 is not None else None,
                                "initial_hard_stop_r": exit_cfg.get("hard_stop_R"),
                                "up_volume_since_warning": 0.0, "down_volume_since_warning": 0.0,
                                "interval_high": None, "interval_low": None,
                                "prev_interval_high": None, "prev_interval_low": None,
                                "persistent_failure_candidate_at_20m": False,
                                "recovery_detected_at_15m": False, "recovery_confirmed_by_score": False,
                                "stop_before_r": None, "stop_before_price": None,
                                "requested_stop_r": None, "requested_stop_price": None,
                                "stop_after_r": None, "stop_after_price": None,
                                "stop_changed": False, "stop_change_ts": None,
                                "checkpoint_log": [],
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
                "mfe_price": pos["mfe_price"], "mae_price": pos["mae_price"],
                "profit_lock_activated": pos.get("profit_lock_activated", False),
                "profit_lock_activated_at_r": pos.get("profit_lock_activated_at_r"),
                "trail_activated": pos.get("trail_activated", False),
                "trail_activated_at_r": pos.get("trail_activated_at_r"),
                "v6_audit": _v6_audit_record(pos, "eod_close", None, day_bars.index[-1]),
                "v10_audit": _v10_audit_record(pos, "eod_close", None, day_bars.index[-1]),
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
    es_intraday: pd.DataFrame | None = None,
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
    es_day_groups = _es_day_groups(es_intraday)

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
                    if bar_ts in intraday_by_symbol[symbol].index:
                        _update_excursion(pos, intraday_by_symbol[symbol].loc[bar_ts])
                    trades.append({
                        "id": trade_id, "symbol": symbol, "side": close_action,
                        "fill_price": float(intraday_by_symbol[symbol].loc[bar_ts, "Close"])
                        if bar_ts in intraday_by_symbol[symbol].index else pos["entry_price"],
                        "size": pos["qty"], "timestamp_iso": bar_ts.isoformat(),
                        "exit_reason": "eod_close", "commission": commission_per_trade,
                        "mfe_price": pos["mfe_price"], "mae_price": pos["mae_price"],
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
                _update_excursion(pos, intraday.loc[bar_ts])
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
                        "mfe_price": pos["mfe_price"], "mae_price": pos["mae_price"],
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
                    "es_filter_pass": po["es_filter_pass"],
                })
                open_positions[symbol] = {
                    "side": side, "entry_price": po["limit_price"], "initial_stop": po["initial_stop"],
                    "stop_price": po["initial_stop"], "target_price": po["target_price"], "qty": po["qty"],
                    "state": "pre_breakeven",
                    "mfe_price": po["limit_price"], "mae_price": po["limit_price"],
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
                    # Computed at PLACEMENT time (this tick), not at fill -
                    # matches the live engine's own gate-before-placing
                    # semantics (a real resting order that would have been
                    # rejected here is never placed live either).
                    "es_filter_pass": _es_filter_pass(strategy_rules, es_day_groups, day, bar_ts, side),
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
                "mfe_price": pos["mfe_price"], "mae_price": pos["mae_price"],
            })

    return {"trades": trades, "skipped_symbols": skipped, "filter_stats": filter_stats}
