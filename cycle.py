"""One tick of the trading cycle, for a given mode ('paper' or 'live'). Runs
every 5 minutes from the always-on service (see run_service.py) — one
scheduler instance per mode, each connecting to its own IB Gateway process.
On each tick: checks market hours, handles any pending emergency
flatten-all request from the dashboard, reconciles stop-outs, manages open
positions (breakeven flip, partial profit, swing trailing stop) — always,
regardless of the enabled flag, so an open position never goes unmanaged —
then, only if the bot is enabled from the dashboard, scans the watchlist for
new entries under EACH direction's own active strategy (long and short
trade independently — see db.py's per-direction "active strategy" model;
either can be off with no active strategy for that side, in which case
that side simply doesn't open new positions). Force-closes everything
before the close.

Long and short are exact mirrors of each other throughout this file: a
long buys low expecting a breakout up (stop below entry, sells to close);
a short sells high expecting a breakdown down (stop above entry, buys to
close). Every position-touching function branches on pos["side"] (or an
explicit `side` argument before a position exists yet).

Time gate (America/New_York):
    Sat/Sun                        -> "weekend"   (exit <1s)
    before 9:35 or after 16:00     -> "too_early" / "closed" (exit <1s)
    15:30-15:51                    -> "manage_only" (no new entries)
    15:51-16:00                    -> "force_close"
    9:35-15:30                     -> "ok" (full cycle: manage + scan)

9:35 is the earliest ANY strategy is allowed to enter (5 min after the
actual 9:30 open) - it's a floor, not a per-strategy setting. Each side's
own active strategy still gates itself further via its rules JSON's
time_filter (earliest_entry_et/latest_entry_et), checked inside
entry_scan — so a strategy configured for the default 10:05 start still
only enters from 10:05, even though the cycle itself is already running
"ok" from 9:35 onward for whichever other strategy wants to start earlier.
"""
import argparse
import math
import subprocess
import sys
import traceback
from datetime import datetime, time as dt_time, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd
import yfinance as yf
from dotenv import dotenv_values
from ib_async import Stock, StopOrder

from src import db, mode_config
from src.ibkr_client import IBKRClient
from src.notify import notify

PROJECT_DIR = Path(__file__).resolve().parent
ET = ZoneInfo("America/New_York")
SUBPROCESS_TIMEOUT = 40  # margin above ibkr_client.SETTLED_STATUSES_TIMEOUT (20s) plus connect/qualify/disconnect overhead

TOO_EARLY_END = dt_time(9, 35)  # earliest any strategy's own time_filter may request entries from - see entry_scan
MANAGE_ONLY_AFTERNOON_START = dt_time(15, 30)
FORCE_CLOSE_START = dt_time(15, 51)
CLOSED_START = dt_time(16, 0)

ACCOUNT_REFRESH_CLIENT_ID = 5  # dedicated id so this never collides with the cycle (2) or trade.py (3) connections

# Used by manage_position only when a held position's side no longer has an
# active strategy (deactivated or deleted after the position was opened) —
# a position must never go unmanaged just because its strategy is gone.
# Matches rules.json's original defaults.
_FALLBACK_EXIT_CFG = {
    "partial_profit_trigger_R": 0.75,
    "partial_profit_fraction": 0.3333,
    "breakeven_trigger_R": 1.0,
}


# ---------------------------------------------------------------- Step 1 ---
def time_gate(now_et: datetime | None = None) -> str:
    now_et = now_et or datetime.now(ET)
    if now_et.weekday() >= 5:
        return "weekend"
    t = now_et.time()
    if t < TOO_EARLY_END:
        return "too_early"
    if t >= CLOSED_START:
        return "closed"
    if t >= FORCE_CLOSE_START:
        return "force_close"
    if t >= MANAGE_ONLY_AFTERNOON_START:
        return "manage_only"
    return "ok"


def log_decision(account_id: int, mode: str, entry: dict):
    entry = dict(entry)
    event = entry.pop("event")
    db.log_decision(account_id, mode, event, **entry)


def _env() -> dict:
    return dotenv_values(PROJECT_DIR / ".env")


def _connect(env: dict, account_id: int, mode: str, client_id: int) -> IBKRClient:
    return IBKRClient(
        env.get("IBKR_HOST", "127.0.0.1"),
        mode_config.ibkr_port(env, account_id, mode),
        client_id,
    )


# ---------------------------------------------------------------- Step 3 ---
def check_stop_outs(account_id: int, mode: str, ib, positions: list[dict]) -> list[dict]:
    if not positions:
        return positions

    cutoff = datetime.now(ET) - timedelta(hours=1)
    fills = ib.fills()
    stopped_symbols = set()

    for pos in positions:
        stop_order_id = pos.get("stop_order_id")
        if stop_order_id is None:
            continue
        side = pos.get("side", "long")
        for fill in fills:
            fill_time = fill.time
            if fill_time.tzinfo is None:
                fill_time = fill_time.replace(tzinfo=ZoneInfo("UTC"))
            if fill_time.astimezone(ET) < cutoff:
                continue
            if fill.execution.orderId == stop_order_id:
                if side == "short":
                    pnl = (pos["entry_price"] - fill.execution.avgPrice) * pos["qty"]
                else:
                    pnl = (fill.execution.avgPrice - pos["entry_price"]) * pos["qty"]
                notify(f"[{mode.upper()}] STOP {pos['symbol']}", f"exit ${fill.execution.avgPrice:.2f}, P&L ${pnl:+.2f}", "default")
                log_decision(account_id, mode, {"event": "stop_out", "symbol": pos["symbol"], "side": side, "fill_price": fill.execution.avgPrice, "pnl": pnl})
                stopped_symbols.add(pos["symbol"])
                db.remove_position(account_id, mode, pos["symbol"])

    return [p for p in positions if p["symbol"] not in stopped_symbols]


# --------------------------------------------------------- order helpers ---
def _qualify(ib, symbol: str):
    (contract,) = ib.qualifyContracts(Stock(symbol, "SMART", "USD"))
    return contract


def _find_order(ib, order_id: int):
    for trade in ib.trades():
        if trade.order.orderId == order_id:
            return trade.order
    return None


def _cancel_stop(ib, order_id: int | None):
    if order_id is None:
        return
    order = _find_order(ib, order_id)
    if order is not None:
        ib.cancelOrder(order)


def _place_stop(ib, symbol: str, quantity: int, stop_price: float, side: str) -> int:
    """A long's protective stop is a SELL (closes if price falls); a
    short's is a BUY (closes/covers if price rises)."""
    contract = _qualify(ib, symbol)
    action = "SELL" if side == "long" else "BUY"
    order = StopOrder(action, quantity, round(stop_price, 2))
    trade = ib.placeOrder(contract, order)
    ib.sleep(1)
    return trade.order.orderId


def _broker_position(ib, symbol: str) -> dict | None:
    """This symbol's real signed quantity/avg-cost at the broker right now,
    independent of anything this bot's own DB thinks - None if flat.
    trade.py only waits ibkr_client.SETTLED_STATUSES_TIMEOUT seconds for an
    order to reach a final status before giving up and reporting failure —
    but the order itself isn't cancelled just because our client stopped
    watching it, so a market order can still fill (or a close can still
    go through) after that deadline. This is the ground truth to check
    before assuming a "not filled" subprocess result means nothing
    happened at the broker."""
    for p in ib.positions():
        if p.contract.symbol == symbol and p.position != 0:
            return {"qty": p.position, "avg_cost": float(p.avgCost)}
    return None


def _market_close(account_id: int, mode: str, ib, symbol: str, quantity: int, side: str) -> bool:
    """Closes (or trims) a position at market — SELL for a long, BUY for a
    short — regardless of whether this is a full close or a partial."""
    action = "SELL" if side == "long" else "BUY"
    proc = subprocess.run(
        [sys.executable, str(PROJECT_DIR / "trade.py"), "--mode", mode, "--account-id", str(account_id),
         "--symbol", symbol, "--side", action, "--size", str(quantity)],
        capture_output=True, text=True, timeout=SUBPROCESS_TIMEOUT,
    )
    log_decision(account_id, mode, {"event": "market_close_subprocess", "symbol": symbol, "side": side, "action": action, "qty": quantity, "stdout": proc.stdout})
    return proc.returncode == 0


def _get_5min_bars(symbol: str) -> pd.DataFrame | None:
    try:
        bars = yf.Ticker(symbol.replace(" ", "-")).history(period="2d", interval="5m")
        return bars if not bars.empty else None
    except Exception:
        return None


# How far back to fetch for each selectable chart interval, chosen to stay
# comfortably under yfinance's per-interval lookback limits (1m: 7d max;
# 2m-30m: 60d max; 60m/1h: 730d max; 1d+: effectively unlimited) while still
# giving a chart with a reasonable amount of history for that granularity.
CHART_INTERVAL_PERIODS = {
    "1m": "5d", "5m": "5d", "15m": "1mo", "30m": "1mo", "1h": "3mo", "1d": "6mo",
}


def get_chart_bars(symbol: str, interval: str = "5m") -> pd.DataFrame | None:
    """OHLC bars for the dashboard's candlestick chart, at the requested
    interval (one of CHART_INTERVAL_PERIODS' keys — same fetch shape as
    _evaluate_entry_filters's intraday history for the intraday intervals).
    Exposed unprefixed since web/app.py (which never talks to IBKR
    directly, but yfinance needs no broker connection) calls this directly
    rather than through a subprocess. Daily bars skip pre/post market,
    since yfinance doesn't extend that session concept to daily data."""
    if interval not in CHART_INTERVAL_PERIODS:
        interval = "5m"
    try:
        bars = yf.Ticker(symbol.replace(" ", "-")).history(
            period=CHART_INTERVAL_PERIODS[interval], interval=interval, prepost=(interval != "1d"),
        )
        if bars.empty:
            return None
        bars.index = bars.index.tz_convert(ET)
        return bars
    except Exception:
        return None


def get_prior_close(symbol: str) -> float | None:
    """Yesterday's close for `symbol` — same daily-bar convention as
    _evaluate_entry_filters's D1/D2 (once the market is open, yfinance's
    last daily row is always today's still-forming bar, so .iloc[-2] is
    the prior completed session). Used for the dashboard's daily $ P&L on
    real holdings, exposed unprefixed for the same reason as get_chart_bars."""
    try:
        daily = yf.Ticker(symbol.replace(" ", "-")).history(period="5d", interval="1d")
        if len(daily) < 2:
            return None
        return float(daily["Close"].iloc[-2])
    except Exception:
        return None


def _find_latest_swing_low(bars: pd.DataFrame) -> float | None:
    lows = bars["Low"].to_numpy()
    n = len(lows)
    for i in range(n - 3, 1, -1):
        if (lows[i] < lows[i - 1] and lows[i] < lows[i - 2]
                and lows[i] < lows[i + 1] and lows[i] < lows[i + 2]):
            return float(lows[i])
    return None


def _find_latest_swing_high(bars: pd.DataFrame) -> float | None:
    """Mirror of _find_latest_swing_low, for a short's trailing stop —
    trails just above the most recent local high instead of just below
    the most recent local low."""
    highs = bars["High"].to_numpy()
    n = len(highs)
    for i in range(n - 3, 1, -1):
        if (highs[i] > highs[i - 1] and highs[i] > highs[i - 2]
                and highs[i] > highs[i + 1] and highs[i] > highs[i + 2]):
            return float(highs[i])
    return None


def _current_price(symbol: str) -> float | None:
    bars = _get_5min_bars(symbol)
    if bars is None or bars.empty:
        return None
    return float(bars["Close"].iloc[-1])


def get_extended_hours_quote(symbol: str) -> dict:
    """Pre-market or after-hours price for the dashboard's Account Holdings
    table - _current_price/_get_5min_bars deliberately fetch regular-session
    bars only (prepost=False), so outside 9:30-16:00 ET that "Current"
    column is frozen at the last regular close, not the live extended-hours
    price. Returns {"session": "pre"|"post"|None, "price": float|None,
    "change_pct": float|None, "ref_price": float|None} - all four are None
    during regular hours (nothing extended to show) or on a fetch failure.
    change_pct/ref_price use yesterday's regular close for "pre" (the
    natural pre-market reference) and TODAY's regular-session close for
    "post" (after-hours moves off the day's actual close, not yesterday's).
    change_pct is the SYMBOL's raw move, not any position's P&L - a rising
    price means nothing about gain/loss for a short position. ref_price is
    exposed so a caller with pos["qty"] (already signed - negative for a
    short) can compute (price - ref_price) * qty, naturally flipping to a
    loss when a short's price rises, same as daily_pnl/unrealized_pnl do."""
    now_et = datetime.now(ET)
    empty = {"session": None, "price": None, "change_pct": None, "ref_price": None}
    try:
        bars = yf.Ticker(symbol.replace(" ", "-")).history(period="1d", interval="1m", prepost=True)
    except Exception:
        return dict(empty)
    if bars.empty:
        return dict(empty)
    bars.index = bars.index.tz_convert(ET)
    last_ts = bars.index[-1]
    if last_ts.date() != now_et.date():
        return dict(empty)
    last_price = float(bars["Close"].iloc[-1])
    last_time = last_ts.time()

    if last_time < dt_time(9, 30):
        session = "pre"
        ref = get_prior_close(symbol)
    elif last_time >= dt_time(16, 0):
        session = "post"
        regular = bars[bars.index.time < dt_time(16, 0)]
        ref = float(regular["Close"].iloc[-1]) if not regular.empty else get_prior_close(symbol)
    else:
        return dict(empty)

    change_pct = ((last_price - ref) / ref * 100) if ref else None
    return {"session": session, "price": last_price, "change_pct": change_pct, "ref_price": ref}


def _compute_rsi_series(closes: pd.Series, period: int) -> pd.Series:
    """Wilder's RSI (the standard definition — smoothed via an EWMA with
    alpha=1/period) at every bar of `closes`, not just the latest — NaN
    wherever there isn't yet `period` bars of history. _compute_rsi and the
    dashboard's chart RSI line both build on this so they always agree."""
    delta = closes.diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    avg_gain = gain.ewm(alpha=1 / period, min_periods=period, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1 / period, min_periods=period, adjust=False).mean()
    rs = avg_gain / avg_loss
    rsi = 100 - (100 / (1 + rs))
    return rsi.mask(avg_loss == 0, 100.0)


def _compute_rsi(closes: pd.Series, period: int) -> float | None:
    """RSI as of the most recent bar of `closes` — None if there isn't
    enough history yet to have a meaningful value."""
    if len(closes) < period + 1:
        return None
    value = _compute_rsi_series(closes, period).iloc[-1]
    return float(value) if pd.notna(value) else None


def get_chart_rsi(bars: pd.DataFrame, period: int = 14) -> list[dict]:
    """RSI(period) at every bar of `bars` (as returned by get_chart_bars),
    for the dashboard's chart modal — same _compute_rsi_series math used
    for the Watchlist table's RSI column, so the two always agree. Bars
    without enough history yet for a value are omitted rather than sent
    as null, so the chart's RSI line only starts once it's meaningful."""
    series = _compute_rsi_series(bars["Close"], period)
    return [
        {"time": int(ts.timestamp()), "value": round(float(v), 2)}
        for ts, v in series.items() if pd.notna(v)
    ]


def get_chart_volume(bars: pd.DataFrame) -> list[dict]:
    """Per-bar volume for the dashboard's chart modal, same bars as
    get_chart_bars/get_chart_rsi."""
    return [{"time": int(ts.timestamp()), "value": float(v)} for ts, v in bars["Volume"].items()]


def get_sma(symbol: str, period: int) -> float | None:
    """The `period`-day SMA of daily closes as of yesterday's close —
    same computation _evaluate_entry_filters uses for D2 (SMA200 on the
    long-side default/aggressive/RSI presets) and the overextension check
    on Short Parabolic Reversal (SMA50), exposed for the dashboard's chart
    modal to show the actual threshold a strategy's D2 checks against.
    Excludes today's still-forming bar, matching D2's own convention."""
    try:
        daily = yf.Ticker(symbol.replace(" ", "-")).history(period="260d" if period >= 200 else "80d", interval="1d")
        if len(daily) < period + 1:
            return None
        return float(daily["Close"].iloc[-(period + 1):-1].mean())
    except Exception:
        return None


# ---------------------------------------------------------------- Step 4 ---
def _breakeven_or_partial_decision(pos: dict, exit_cfg: dict, r_multiple: float) -> dict:
    """Pure decision logic for manage_position's "pre_breakeven" stage —
    shared with backtest_engine.py's exit simulator so both replay the
    exact same thresholds instead of risking two copies drifting apart.
    Only meaningful while pos["state"] == "pre_breakeven"; returns
    {"action": "hold"} otherwise-uninteresting ticks."""
    if r_multiple >= exit_cfg["breakeven_trigger_R"]:
        return {"action": "breakeven_flip", "new_stop_price": pos["entry_price"], "new_state": "post_breakeven_no_partial"}
    if r_multiple >= exit_cfg["partial_profit_trigger_R"]:
        side = pos.get("side", "long")
        entry = pos["entry_price"]
        close_qty = min(math.ceil(pos["qty"] * exit_cfg["partial_profit_fraction"]), pos["qty"])
        new_stop_price = entry * 1.01 if side == "short" else entry * 0.99
        return {"action": "partial_profit", "close_qty": close_qty, "new_stop_price": new_stop_price, "new_state": "post_breakeven_partial_done"}
    return {"action": "hold"}


def _trailing_stop_decision(pos: dict, swing_stop_candidate: float | None) -> dict:
    """Pure decision logic for manage_position's "post_breakeven" trailing
    stage — same sharing rationale as _breakeven_or_partial_decision. The
    caller computes swing_stop_candidate from whichever bars source is in
    play (live 5m bars via _find_latest_swing_low/high, or a backtest's
    historical bars) — this function only decides whether it's valid and
    whether it improves on the current stop."""
    if swing_stop_candidate is None:
        return {"action": "hold"}
    side = pos.get("side", "long")
    initial_stop = pos["initial_stop"]
    candidate_valid = (swing_stop_candidate < initial_stop) if side == "short" else (swing_stop_candidate > initial_stop)
    if not candidate_valid:
        return {"action": "hold"}
    current_stop = pos.get("stop_price", initial_stop)
    improves = (swing_stop_candidate < current_stop) if side == "short" else (swing_stop_candidate > current_stop)
    if improves:
        return {"action": "trail_stop", "new_stop_price": swing_stop_candidate}
    return {"action": "hold"}


def manage_position(account_id: int, mode: str, ib, pos: dict, rules: dict) -> dict:
    """rules must be the exit config for pos["side"] — see run_cycle, which
    picks the right (long or short) active strategy per position, falling
    back to a safe default if that side no longer has an active strategy
    (a position stays managed even if its strategy was deactivated or
    deleted after it was opened). The two stages below are deliberately
    separate `if`s, not `if`/`elif` — a breakeven flip and a trailing-stop
    check can both fire on the same tick, since the state mutates in
    between (see _breakeven_or_partial_decision/_trailing_stop_decision,
    the pure decision logic both stages and backtest_engine.py share)."""
    exit_cfg = rules["exit"]
    side = pos.get("side", "long")
    price = _current_price(pos["symbol"])
    if price is None:
        return pos

    entry = pos["entry_price"]
    if side == "short":
        initial_risk = pos["initial_stop"] - entry
    else:
        initial_risk = entry - pos["initial_stop"]
    if initial_risk <= 0:
        return pos
    r_multiple = ((entry - price) if side == "short" else (price - entry)) / initial_risk
    pos["r_multiple"] = r_multiple

    if pos["state"] == "pre_breakeven":
        decision = _breakeven_or_partial_decision(pos, exit_cfg, r_multiple)
        if decision["action"] == "breakeven_flip":
            _cancel_stop(ib, pos.get("stop_order_id"))
            pos["stop_order_id"] = _place_stop(ib, pos["symbol"], pos["qty"], decision["new_stop_price"], side)
            pos["stop_price"] = decision["new_stop_price"]
            pos["state"] = decision["new_state"]
            notify(f"[{mode.upper()}] BE {pos['symbol']}", f"stop -> ${entry:.2f}", "default")
            log_decision(account_id, mode, {"event": "breakeven_flip", "symbol": pos["symbol"], "side": side, "new_stop": entry})
        elif decision["action"] == "partial_profit":
            close_qty = decision["close_qty"]
            if _market_close(account_id, mode, ib, pos["symbol"], close_qty, side):
                remaining = pos["qty"] - close_qty
                _cancel_stop(ib, pos.get("stop_order_id"))
                pos["qty"] = remaining
                if remaining > 0:
                    pos["stop_order_id"] = _place_stop(ib, pos["symbol"], remaining, decision["new_stop_price"], side)
                    pos["stop_price"] = decision["new_stop_price"]
                pos["state"] = decision["new_state"]
                notify(f"[{mode.upper()}] PARTIAL {pos['symbol']}", f"closed {close_qty}/{close_qty + remaining} @ ${price:.2f}", "default")
                log_decision(account_id, mode, {"event": "partial_profit", "symbol": pos["symbol"], "side": side, "closed": close_qty, "price": price})

    if pos["state"].startswith("post_breakeven"):
        bars = _get_5min_bars(pos["symbol"])
        swing_stop_candidate = None
        if bars is not None and len(bars) > 5:
            swing = _find_latest_swing_high(bars) if side == "short" else _find_latest_swing_low(bars)
            if swing is not None:
                swing_stop_candidate = (swing + 0.01) if side == "short" else (swing - 0.01)
        decision = _trailing_stop_decision(pos, swing_stop_candidate)
        if decision["action"] == "trail_stop":
            _cancel_stop(ib, pos.get("stop_order_id"))
            pos["stop_order_id"] = _place_stop(ib, pos["symbol"], pos["qty"], decision["new_stop_price"], side)
            old_stop = pos.get("stop_price", pos["initial_stop"])
            pos["stop_price"] = decision["new_stop_price"]
            notify(f"[{mode.upper()}] TRAIL {pos['symbol']}", f"stop ${old_stop:.2f} -> ${decision['new_stop_price']:.2f}", "default")
            log_decision(account_id, mode, {"event": "trail_stop", "symbol": pos["symbol"], "side": side, "old": old_stop, "new": decision["new_stop_price"]})

    if pos["qty"] > 0:
        db.upsert_position(account_id, mode, pos)
    return pos


# ---------------------------------------------------------------- Step 6 ---
def force_close_all(account_id: int, mode: str, ib, positions: list[dict]):
    if not positions:
        return

    held = [p for p in positions if p.get("hold_overnight")]
    to_close = [p for p in positions if not p.get("hold_overnight")]
    for pos in held:
        # One-shot opt-out (see db.set_hold_overnight) - reset it now so it
        # only ever skips today's close, never silently forever, and leave
        # the position's stop and DB tracking completely untouched so it
        # carries into tomorrow exactly as it stands now.
        db.set_hold_overnight(account_id, mode, pos["symbol"], False)
        log_decision(account_id, mode, {"event": "force_close_skipped", "symbol": pos["symbol"], "side": pos.get("side", "long"), "qty": pos["qty"], "reason": "hold_overnight"})
        notify(
            f"[{mode.upper()}] Held overnight: {pos['symbol']}",
            "EOD force-close skipped by request - stop stays active, will force-close normally tomorrow unless held again",
            "default",
        )

    if not to_close:
        return

    notify(
        f"[{mode.upper()}] EOD Force Close",
        f"flattening {len(to_close)} position(s)" + (f" ({len(held)} held overnight by request)" if held else ""),
        "high",
    )
    for pos in to_close:
        side = pos.get("side", "long")
        _cancel_stop(ib, pos.get("stop_order_id"))
        closed = _market_close(account_id, mode, ib, pos["symbol"], pos["qty"], side)
        if not closed:
            # Same delayed-fill race as entry_scan, on the way out: trade.py
            # may have given up waiting before the close actually confirmed.
            # Check the broker directly before trusting "not filled".
            closed = _broker_position(ib, pos["symbol"]) is None
        log_decision(account_id, mode, {"event": "force_close", "symbol": pos["symbol"], "side": side, "qty": pos["qty"], "confirmed": closed})
        if closed:
            db.remove_position(account_id, mode, pos["symbol"])
        else:
            # Genuinely still open, and its stop was just cancelled above to
            # make way for the close attempt — re-arm it immediately so the
            # position isn't left unprotected, keep it tracked so the next
            # cycle retries closing it, and alert loudly since force-close
            # failing outright needs a human to look.
            pos["stop_order_id"] = _place_stop(ib, pos["symbol"], pos["qty"], pos["stop_price"], side)
            db.upsert_position(account_id, mode, pos)
            notify(
                f"[{mode.upper()}] FORCE CLOSE FAILED: {pos['symbol']}",
                f"still holding {pos['qty']} after EOD close attempt - stop re-armed at ${pos['stop_price']:.2f}, will retry next cycle",
                "high",
            )


# ---------------------------------------------------------------- Step 8 ---
def _evaluate_filters_from_bars(daily: pd.DataFrame, intraday: pd.DataFrame, rules: dict, side: str) -> dict:
    """The actual D1-D3/I1-I3 decision logic, pulled out of
    _evaluate_entry_filters as a pure function: no data fetching, no
    wall-clock "now" — the day being evaluated is whatever the LAST date
    in `intraday`'s index is, and `daily` must already end at that day's
    prior trading day (i.e. daily.iloc[-2] is "yesterday" relative to the
    evaluation point). This is what lets backtest_engine.py replay the
    EXACT same decision logic against historical bars instead of live
    ones — _evaluate_entry_filters (below) is now just "fetch fresh
    yfinance data ending at the real current moment, then call this" - the
    live bot and the backtester share this one implementation rather than
    risking two copies quietly drifting apart.

    Long and short are exact mirrors: D1 breaks the prior day's high (long)
    or low (short); D2 wants the prior close on the trend side of the
    200-day SMA; D3 wants a gap in the trade's direction; I1/I2 want a new
    premarket/intraday extreme in the trade's direction; I3 (relative
    volume) is direction-agnostic."""
    daily_filters = rules["daily_filters"]
    intraday_filters = rules["intraday_filters"]

    if len(daily) < 201:
        return {"pass": False, "side": side, "error": "not enough daily history"}
    prior_day = daily.iloc[-2]
    sma200 = daily["Close"].iloc[-201:-1].mean()

    if intraday.empty:
        return {"pass": False, "side": side, "error": "no intraday data"}

    current_price = float(intraday["Close"].iloc[-1])
    as_of_date = intraday.index[-1].date()
    today_bars = intraday[intraday.index.date == as_of_date]
    if today_bars.empty:
        return {"pass": False, "side": side, "error": "no bars for today yet"}

    premarket_bars = today_bars[today_bars.index.time < dt_time(9, 30)]
    regular_bars = today_bars[today_bars.index.time >= dt_time(9, 30)]

    prior_close = float(prior_day["Close"])
    gap_pct = (current_price - prior_close) / prior_close * 100 if prior_close else 0.0
    rsi_value = None  # only set when this strategy's I2 is RSI-based (see below) - None otherwise

    if side == "long":
        stop_ref = float(regular_bars["Low"].min()) if not regular_bars.empty else float(today_bars["Low"].min())
        d1 = current_price > float(prior_day["High"])  # above yesterday's high
        d2 = float(prior_day["Close"]) > float(sma200)  # yesterday's close above the 200-day SMA
        d3 = gap_pct >= daily_filters["D3_min_gap_pct_from_prior_close"]  # gap up >= threshold
        premarket_extreme = float(premarket_bars["High"].max()) if not premarket_bars.empty else float("-inf")
        i1 = current_price > premarket_extreme  # above today's premarket high
        if "I2_rsi_above" in intraday_filters:
            rsi_value = _compute_rsi(intraday["Close"], intraday_filters.get("I2_rsi_period", 14))
            i2 = rsi_value is not None and rsi_value > intraday_filters["I2_rsi_above"]  # RSI above threshold
        else:
            extreme_so_far = float(today_bars["High"].iloc[:-1].max()) if len(today_bars) > 1 else float("-inf")
            i2 = current_price > extreme_so_far  # new high-of-day
    else:
        stop_ref = float(regular_bars["High"].max()) if not regular_bars.empty else float(today_bars["High"].max())
        d1 = current_price < float(prior_day["Low"])  # below yesterday's low
        if "D2_prior_close_pct_above_sma50_min" in daily_filters:
            sma50 = daily["Close"].iloc[-51:-1].mean()
            ext_pct = (float(prior_day["Close"]) - float(sma50)) / float(sma50) * 100 if sma50 else 0.0
            d2 = ext_pct >= daily_filters["D2_prior_close_pct_above_sma50_min"]  # overextended above the 50-day SMA
        else:
            d2 = float(prior_day["Close"]) < float(sma200)  # yesterday's close below the 200-day SMA
        d3 = gap_pct <= -daily_filters["D3_min_gap_pct_down_from_prior_close"]  # gap down >= threshold
        premarket_extreme = float(premarket_bars["Low"].min()) if not premarket_bars.empty else float("inf")
        i1 = current_price < premarket_extreme  # below today's premarket low
        if "I2_rsi_below" in intraday_filters:
            rsi_value = _compute_rsi(intraday["Close"], intraday_filters.get("I2_rsi_period", 14))
            i2 = rsi_value is not None and rsi_value < intraday_filters["I2_rsi_below"]  # RSI below threshold (rolled over)
        else:
            extreme_so_far = float(today_bars["Low"].iloc[:-1].min()) if len(today_bars) > 1 else float("inf")
            i2 = current_price < extreme_so_far  # new low-of-day

    # I3: relative volume vs lookback-day average >= threshold (direction-agnostic)
    lookback = intraday_filters["I3_rvol_lookback_days"]
    avg_daily_volume = float(daily["Volume"].iloc[-(lookback + 1):-1].mean())
    today_volume_so_far = float(today_bars["Volume"].sum())
    rvol = today_volume_so_far / avg_daily_volume if avg_daily_volume else 0.0
    i3 = rvol >= intraday_filters["I3_rvol_min"]

    passed = bool(d1 and d2 and d3 and i1 and i2 and i3)
    return {
        "pass": passed, "side": side,
        "D1": bool(d1), "D2": bool(d2), "D3": bool(d3),
        "I1": bool(i1), "I2": bool(i2), "I3": bool(i3),
        "price": current_price, "rvol": rvol, "gap_pct": gap_pct,
        "stop_ref": stop_ref, "rsi": rsi_value,
    }


def _evaluate_entry_filters(account_id: int, mode: str, ticker: str, rules: dict, side: str) -> dict:
    """Live wrapper around _evaluate_filters_from_bars: fetches fresh
    yfinance data ending at the real current moment, then hands it to the
    shared pure decision logic. Always returns a detail dict with a "pass"
    bool; on a full pass it also carries "price"/"stop_ref" for sizing
    (stop_ref is the low of day for a long's stop, the high of day for a
    short's), and whenever all six filters could be computed it carries
    each one's individual result (D1..I3) plus
    "price"/"gap_pct"/"rvol"/"rsi" (the last is None unless this side's
    active strategy uses an RSI-based I2) — this detail is what the
    dashboard's Watchlist table shows, so entry_scan (which stops early
    once daily trade/position caps are hit) isn't the only place this gets
    computed. Uses yfinance only (same free/keyless data source as
    morning_prefilter.py), no IBKR connection needed."""
    yahoo_symbol = ticker.replace(" ", "-")
    try:
        daily = yf.Ticker(yahoo_symbol).history(period="260d", interval="1d")
        intraday = yf.Ticker(yahoo_symbol).history(period="5d", interval="5m", prepost=True)
        if not intraday.empty:
            intraday.index = intraday.index.tz_convert(ET)
        detail = _evaluate_filters_from_bars(daily, intraday, rules, side)
        detail["side"] = side
        event = "filter_eval" if "error" not in detail else "filter_eval_error"
        log_decision(account_id, mode, {"event": event, "symbol": ticker, **detail})
        return detail
    except Exception as exc:  # noqa: BLE001 - one bad ticker must not kill the scan
        log_decision(account_id, mode, {"event": "filter_eval_error", "symbol": ticker, "side": side, "error": str(exc)})
        return {"pass": False, "side": side, "error": str(exc)}


def _within_entry_window(rules: dict, now_et: datetime | None = None) -> bool:
    """Whether this strategy's own time_filter.earliest_entry_et/
    latest_entry_et currently allow a new entry — the per-strategy floor
    under the global one (cycle.TOO_EARLY_END): a strategy configured for
    the default 10:05 start stays gated to 10:05 even once the cycle
    itself is already running "ok" earlier for another strategy that
    asked for an earlier window. Missing/unparseable bounds are treated as
    no constraint on that side, so an older strategy row without this
    field just keeps behaving as if the whole "ok" window applies to it."""
    now_et = now_et or datetime.now(ET)
    t = now_et.time()
    tf = rules.get("time_filter", {})
    for key, cmp in (("earliest_entry_et", lambda bound: t < bound), ("latest_entry_et", lambda bound: t >= bound)):
        raw = tf.get(key)
        if not raw:
            continue
        try:
            hour, minute = (int(part) for part in raw.split(":", 1))
            if cmp(dt_time(hour, minute)):
                return False
        except (ValueError, TypeError):
            continue
    return True


def _strategy_universe(rules: dict) -> str:
    """The watchlist universe tag this strategy's candidates must carry
    (see db.get_watchlist/replace_watchlist) - "default" (the S&P 500 scan)
    unless the strategy's rules_json restricts itself to a named custom
    universe via universe_filters.custom_universe."""
    return rules.get("universe_filters", {}).get("custom_universe") or "default"


def entry_scan(account_id: int, mode: str, ib, positions: list[dict], rules: dict, env: dict, side: str) -> list[dict]:
    """Scans this side's ('long' or 'short') watchlist for new entries
    under its own active strategy. positions holds ALL open positions
    (both sides) — run_cycle chains a long scan then a short scan over the
    same growing list, so each sees what the other already opened this
    cycle. Concurrent-position and daily-entry caps are enforced per side
    (this side's own count against this side's own rules), not pooled
    across both directions."""
    if not _within_entry_window(rules):
        return positions

    risk = mode_config.risk_params(env, account_id, mode)
    if db.count_todays_entries(account_id, mode, side) >= risk["max_trades_per_day"]:
        return positions

    max_concurrent = rules["risk"]["max_concurrent_positions"]
    side_positions = [p for p in positions if p.get("side", "long") == side]
    if len(side_positions) >= max_concurrent:
        return positions

    watchlist = [row["symbol"] for row in db.get_watchlist(account_id, mode, direction=side, universe=_strategy_universe(rules))]
    if not watchlist:
        return positions

    # != 0 (not just > 0) so an existing short in the real account also
    # blocks a duplicate/conflicting entry, not just existing longs.
    held_symbols = {p.contract.symbol for p in ib.positions() if p.position != 0}
    held_symbols |= {p["symbol"] for p in positions}

    portfolio_value = risk["portfolio_value"]
    max_risk_pct = risk["max_risk_pct"]
    max_position_pct = rules["risk"]["max_position_size_pct_of_portfolio"] / 100
    action = "BUY" if side == "long" else "SELL"

    for ticker in watchlist:
        if len(side_positions) >= max_concurrent:
            break
        if ticker in held_symbols:
            continue
        if db.count_todays_entries(account_id, mode, side) >= risk["max_trades_per_day"]:
            break

        signal = _evaluate_entry_filters(account_id, mode, ticker, rules, side)
        if not signal.get("pass"):
            continue

        price = signal["price"]
        stop_ref = signal["stop_ref"]
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

        proc = subprocess.run(
            [sys.executable, str(PROJECT_DIR / "trade.py"), "--mode", mode, "--account-id", str(account_id),
             "--symbol", ticker, "--side", action, "--size", str(size)],
            capture_output=True, text=True, timeout=SUBPROCESS_TIMEOUT,
        )
        log_decision(account_id, mode, {"event": "entry_attempt", "symbol": ticker, "side": side, "qty": size, "price": price, "stdout": proc.stdout})
        if proc.returncode != 0:
            # trade.py reported "not filled", but that only means the order
            # hadn't reached a final status within its own polling window —
            # check the broker directly before assuming nothing happened
            # (a delayed-but-real fill here would otherwise leave a real
            # position completely untracked, with no stop and invisible to
            # the EOD force-close).
            broker_pos = _broker_position(ib, ticker)
            if broker_pos is None:
                continue
            fill_qty, fill_price = abs(broker_pos["qty"]), broker_pos["avg_cost"]
            log_decision(account_id, mode, {"event": "delayed_fill_recovered", "symbol": ticker, "side": side, "qty": fill_qty, "price": fill_price})
            notify(
                f"[{mode.upper()}] Delayed fill recovered: {ticker}",
                f"order timed out waiting for a fill confirmation, but the broker shows {fill_qty} shares @ ${fill_price:.2f} - now tracked with a stop",
                "high",
            )
        else:
            fill_qty, fill_price = size, price

        stop_order_id = _place_stop(ib, ticker, fill_qty, initial_stop, side)
        new_position = {
            "symbol": ticker,
            "side": side,
            "entry_price": fill_price,
            "entry_time_iso": datetime.now(ET).isoformat(timespec="seconds"),
            "qty": fill_qty,
            "initial_stop": initial_stop,
            "stop_price": initial_stop,
            "stop_order_id": stop_order_id,
            "state": "pre_breakeven",
            "r_multiple": 0.0,
        }
        db.upsert_position(account_id, mode, new_position)
        positions.append(new_position)
        side_positions.append(new_position)
        held_symbols.add(ticker)
        notify(f"[{mode.upper()}] {action} {ticker}", f"@ ${fill_price:.2f}, stop ${initial_stop:.2f}, qty {fill_qty}", "default")
        log_decision(account_id, mode, {"event": "entry", "symbol": ticker, "side": side, "price": fill_price, "stop": initial_stop, "qty": fill_qty})

    return positions


def scan_watchlist_filters(account_id: int):
    """Evaluates every watchlist symbol's D1-D3/I1-I3 filters, for whichever
    side(s) currently have an active strategy, and stores a snapshot for
    the dashboard's Watchlist table — independent of entry_scan, which
    stops early once the day's trade/position caps are hit and so doesn't
    necessarily check every symbol. Pure yfinance, no IBKR connection
    needed. Mode-agnostic like morning_prefilter (paper and live share the
    same watchlist and market data), so this runs once and writes the same
    snapshot to both modes — see run_service.py, which schedules it from
    the paper instance only. A side with no active strategy is skipped —
    there's no criteria to check its candidates against."""
    status = time_gate()
    if status in ("weekend", "too_early", "closed"):
        return

    results = []
    for side in db.DIRECTIONS:
        rules = db.get_active_rules(account_id, side)
        if rules is None:
            continue
        for row in db.get_watchlist(account_id, "paper", direction=side, universe=_strategy_universe(rules)):
            detail = _evaluate_entry_filters(account_id, "paper", row["symbol"], rules, side)
            results.append({"symbol": row["symbol"], "gap_pct": row["gap_pct"], **detail})

    for mode in db.MODES:
        db.update_watchlist_filters(account_id, mode, results)


# -------------------------------------------------------------------- main
def run_cycle(account_id: int, mode: str):
    """Runs one tick of the trading cycle for the given account+mode. Safe
    to call every 5 minutes all day — it self-gates on market hours."""
    status = time_gate()
    if status in ("weekend", "too_early", "closed"):
        return status

    ibkr = None
    try:
        env = _env()
        long_rules = db.get_active_rules(account_id, "long")
        short_rules = db.get_active_rules(account_id, "short")
        client_id = int(env.get("IBKR_CLIENT_ID", 2))

        try:
            ibkr = _connect(env, account_id, mode, client_id)
        except Exception:
            import time as _time
            _time.sleep(5)
            ibkr = _connect(env, account_id, mode, client_id)

        ib = ibkr.ib
        positions = db.get_open_positions(account_id, mode)

        # Emergency "flatten everything now" request from the dashboard takes
        # priority over the normal cycle, but position management always runs
        # first regardless of the enabled flag, per Step 3/4 below.
        if db.consume_flatten_request(account_id, mode):
            positions = check_stop_outs(account_id, mode, ib, positions)
            force_close_all(account_id, mode, ib, positions)
            db.record_cycle_run(account_id, mode, "flattened_on_request")
            return "flattened_on_request"

        positions = check_stop_outs(account_id, mode, ib, positions)  # Step 3
        rules_by_side = {"long": long_rules, "short": short_rules}
        positions = [
            manage_position(account_id, mode, ib, p, rules_by_side.get(p.get("side", "long")) or {"exit": _FALLBACK_EXIT_CFG})
            for p in positions
        ]  # Step 4

        if status == "force_close":  # Step 6
            force_close_all(account_id, mode, ib, positions)
            db.record_cycle_run(account_id, mode, status)
            return status

        if status == "manage_only":  # Step 7
            db.record_cycle_run(account_id, mode, status)
            return status

        if db.is_bot_enabled(account_id, mode):  # Step 8 — each direction scans under its own active strategy
            if long_rules is not None:
                positions = entry_scan(account_id, mode, ib, positions, long_rules, env, "long")
            if short_rules is not None:
                positions = entry_scan(account_id, mode, ib, positions, short_rules, env, "short")
        else:
            log_decision(account_id, mode, {"event": "entries_paused", "reason": "bot_disabled"})

        db.record_cycle_run(account_id, mode, status)
        return status
    except Exception as exc:  # noqa: BLE001
        db.log_cycle_error(account_id, mode, traceback.format_exc())
        notify(f"[{mode.upper()}] Cycle CRASHED", str(exc)[:500], "high")
        db.record_cycle_run(account_id, mode, "error")
        if ibkr is not None:
            ibkr.disconnect()
        raise
    finally:
        if ibkr is not None:
            ibkr.disconnect()


def refresh_account_info(account_id: int, mode: str):
    """Pulls net liquidation / cash balance / buying power from IBKR and
    stores it in the DB for the dashboard. Runs on its own IBKR client id,
    independent of market hours, so the dashboard has something to show
    even outside the trading window."""
    env = _env()
    ibkr = _connect(env, account_id, mode, ACCOUNT_REFRESH_CLIENT_ID)
    try:
        ib = ibkr.ib
        ib.sleep(2)  # let account summary data populate after connecting
        values = {row.tag: row.value for row in ib.accountSummary()}
        db.update_account_info(
            account_id, mode,
            values.get("NetLiquidation", ""),
            values.get("TotalCashValue", ""),
            values.get("BuyingPower", ""),
        )

        # Every real holding in the account, independent of whether the bot
        # opened it or is tracking it in the positions table — lets the
        # dashboard show (and close) things the bot doesn't know about.
        broker_positions = [
            {"symbol": p.contract.symbol, "qty": p.position, "avg_cost": p.avgCost}
            for p in ib.positions() if p.position != 0
        ]
        db.update_broker_positions(account_id, mode, broker_positions)

        # Every resting stop/limit order in the account, independent of
        # which client ID placed it — reqAllOpenOrders (unlike openTrades
        # alone) pulls in orders from other sessions too: the bot's own
        # cycle connection, a manual TWS/Mobile order, etc. Lets the
        # dashboard show a holding's real protective orders even for a
        # symbol the bot never touched.
        ib.reqAllOpenOrders()
        ib.sleep(1)
        broker_orders = [
            {
                "symbol": t.contract.symbol,
                "order_type": t.order.orderType,
                "action": t.order.action,
                "qty": t.order.totalQuantity,
                "price": t.order.auxPrice if t.order.orderType == "STP" else t.order.lmtPrice,
                "order_id": t.order.orderId,
                "status": t.orderStatus.status,
            }
            for t in ib.openTrades()
            if t.orderStatus.status not in ("Cancelled", "ApiCancelled", "Filled", "Inactive")
        ]
        db.update_broker_orders(account_id, mode, broker_orders)
    finally:
        ibkr.disconnect()


def emergency_check(account_id: int, mode: str):
    """Cheap poll for a pending dashboard flatten-all request. Only opens an
    IBKR connection when the flag is actually set, so this is safe to call
    frequently (e.g. every 15-30s) from the service scheduler."""
    if not db.is_flatten_pending(account_id, mode):
        return

    env = _env()
    client_id = int(env.get("IBKR_CLIENT_ID", 2))
    ibkr = _connect(env, account_id, mode, client_id)
    try:
        if db.consume_flatten_request(account_id, mode):
            positions = db.get_open_positions(account_id, mode)
            positions = check_stop_outs(account_id, mode, ibkr.ib, positions)
            force_close_all(account_id, mode, ibkr.ib, positions)
            db.record_cycle_run(account_id, mode, "flattened_on_request")
    finally:
        ibkr.disconnect()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=db.MODES, default="paper")
    parser.add_argument("--account-id", type=int, default=None,
                         help="Defaults to the admin account when omitted (manual/dev use).")
    args = parser.parse_args()

    db.init_db(seed_rules_path=PROJECT_DIR / "rules.json")
    account_id = args.account_id if args.account_id is not None else db.get_default_account_id()
    try:
        run_cycle(account_id, args.mode)
    except Exception:
        sys.exit(1)


if __name__ == "__main__":
    main()
