"""One tick of the trading cycle (mode is always 'live' - see db.MODES'
own comment, paper trading has been removed). Runs on run_service.py's
own "cycle" job interval (CYCLE_INTERVAL_MINUTES there — 1 minute as of
2026-09-04, tightened from 5 to catch ORB "breakout" entries that only
fire on one exact bar, see that constant's own comment) from the
always-on service.
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
import json
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
from ib_async import LimitOrder, Stock, StopOrder

from src import db, es_filter, mode_config, orb, sst_swing, touch_turn
from src.ibkr_client import IBKRClient, belongs_to_account, cancel_order_any_client, scoped_positions
from src.notify import notify

PROJECT_DIR = Path(__file__).resolve().parent
ET = ZoneInfo("America/New_York")
# How far back _evaluate_entry_filters' own intraday yfinance fetch goes -
# needs to comfortably cover I3's rvol lookback (rules.json's own
# I3_rvol_lookback_days, 14 by default) in TRADING days, so calendar days
# padded for weekends/holidays (14 trading days is ~19-20 calendar days).
# backtest_engine.py's INTRADAY_LOOKBACK_DAYS reads this same constant
# rather than hardcoding its own copy, so the two can't silently drift
# out of sync the way "matches the live fetch" comments alone don't
# actually enforce. Well within yfinance's ~60-day 5-min-interval cap.
INTRADAY_FETCH_LOOKBACK_DAYS = 25
SUBPROCESS_TIMEOUT = 40  # margin above ibkr_client.SETTLED_STATUSES_TIMEOUT (20s) plus connect/qualify/disconnect overhead

TOO_EARLY_END = dt_time(9, 35)  # earliest any strategy's own time_filter may request entries from - see entry_scan
MANAGE_ONLY_AFTERNOON_START = dt_time(15, 30)
FORCE_CLOSE_START = dt_time(15, 51)
CLOSED_START = dt_time(16, 0)

ACCOUNT_REFRESH_CLIENT_ID = 5  # dedicated id so this never collides with the cycle (2) or trade.py (3) connections
SST_SWING_CLIENT_ID = 26  # sst_swing_live.py's own once-daily connection - dedicated so it never collides with
                          # the per-minute cycle (2) or a concurrent trade.py invocation (3); 6-18 and 25 (momentum)
                          # are already taken by other scripts - see this file's own git history for the full map

# Used by manage_position only when a held position's side no longer has an
# active strategy (deactivated or deleted after the position was opened) —
# a position must never go unmanaged just because its strategy is gone.
# Matches rules.json's original defaults.
_FALLBACK_EXIT_CFG = {
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
        account=mode_config.ibkr_account(env, account_id, mode),
    )


# ---------------------------------------------------------------- Step 3 ---
def check_stop_outs(account_id: int, mode: str, ib, positions: list[dict]) -> list[dict]:
    if not positions:
        return positions

    cutoff = datetime.now(ET) - timedelta(hours=1)
    fills = [f for f in ib.fills() if belongs_to_account(ib, f.execution.acctNumber)]
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


def check_virtual_stop_outs(account_id: int, strategy_id: int, strategy_label: str, positions: list[dict]) -> list[dict]:
    """check_stop_outs' counterpart for a strategy_run's simulated
    positions - there's no real broker order to have filled (no
    ib.fills() to check), so this instead directly compares each virtual
    position's own stop_price against the CURRENT price each tick: a long
    is stopped out once price <= stop_price, a short once price >=
    stop_price. Same per-cycle (not truly intrabar) sampling granularity
    every other live price check in this file already accepts.

    The simulated exit/fill price is the worse of (stop_price, the
    current tick's price) - i.e. exactly stop_price under normal
    conditions (mirrors a real STOP order filling at its trigger level),
    or the actual (worse) price if this tick's price already gapped
    through the stop, same as a real stop would slip in that scenario -
    never assumes a BETTER fill than what actually happened.

    Must run before manage_virtual_position each tick (mirrors check_stop_
    outs' own Step-3-before-Step-4 ordering in run_cycle) - a position
    that's already stopped out has nothing left to manage."""
    if not positions:
        return positions

    stopped_symbols = set()
    for pos in positions:
        side = pos.get("side", "long")
        price = _current_price(pos["symbol"])
        if price is None:
            continue
        stop_price = pos["stop_price"]
        stopped = (price <= stop_price) if side == "long" else (price >= stop_price)
        if not stopped:
            continue
        exit_price = min(price, stop_price) if side == "long" else max(price, stop_price)
        pnl = ((exit_price - pos["entry_price"]) if side == "long" else (pos["entry_price"] - exit_price)) * pos["qty"]
        db.record_virtual_trade(account_id, strategy_id, {
            "symbol": pos["symbol"], "side": side, "entry_price": pos["entry_price"],
            "entry_time_iso": pos["entry_time_iso"], "exit_price": exit_price,
            "exit_time_iso": datetime.now(ET).isoformat(timespec="seconds"), "qty": pos["qty"],
            "final_r": pos.get("r_multiple"), "pnl_dollars": pnl, "exit_reason": "stop_out",
        })
        db.remove_virtual_position(account_id, strategy_id, pos["symbol"])
        notify(f"[VIRTUAL] {strategy_label}: STOP {pos['symbol']}", f"exit ${exit_price:.2f}, P&L ${pnl:+.2f}", "default")
        log_decision(account_id, "live", {"event": "stop_out", "symbol": pos["symbol"], "side": side, "fill_price": exit_price, "pnl": pnl, "strategy_id": strategy_id, "virtual": True})
        stopped_symbols.add(pos["symbol"])

    return [p for p in positions if p["symbol"] not in stopped_symbols]


# --------------------------------------------------------- order helpers ---
def _qualify(ib, symbol: str):
    (contract,) = ib.qualifyContracts(Stock(symbol, "SMART", "USD"))
    return contract


def _cancel_stop(ib, order_id: int | None):
    """Cancels a resting stop order regardless of which IBKR API client
    originally placed it (e.g. a stop the dashboard's modify_stop.py or
    Account Holdings set, which this engine's own client id then needs to
    reposition/cancel automatically) - see cancel_order_any_client's own
    docstring for the real live incident (2026-09-10, CHTR) this fixes:
    the engine's own client id could see the order via reqAllOpenOrders
    but silently failed to actually cancel it via ib.trades()-based
    lookup + same-connection cancelOrder(), leaving a stale duplicate
    stop resting alongside the new one."""
    if order_id is None:
        return
    ib.reqAllOpenOrders()
    ib.sleep(1)
    match = next((t for t in ib.openTrades() if t.order.orderId == order_id and belongs_to_account(ib, t.order.account)), None)
    if match is not None:
        cancel_order_any_client(ib, match.order)


def _place_stop(ib, symbol: str, quantity: int, stop_price: float, side: str) -> int:
    """A long's protective stop is a SELL (closes if price falls); a
    short's is a BUY (closes/covers if price rises)."""
    contract = _qualify(ib, symbol)
    action = "SELL" if side == "long" else "BUY"
    order = StopOrder(action, quantity, round(stop_price, 2))
    if getattr(ib, "account", None):
        order.account = ib.account
    trade = ib.placeOrder(contract, order)
    ib.sleep(1)
    return trade.order.orderId


def _place_touch_turn_limit(ib, symbol: str, quantity: int, limit_price: float, side: str, good_till_et: datetime) -> int:
    """Places the REAL resting limit order Touch & Turn's retest entry
    needs (see src/touch_turn.py) - a BUY waiting at the opening candle's
    low for a long, a SELL waiting at its high for a short. Placed
    directly through the orchestrator's own `ib` connection, same as
    _place_stop just above - only ENTRY/EXIT orders that need synchronous
    fill-waiting go through the trade.py subprocess (see its own
    docstring); a stop or a resting limit order doesn't need that.

    Uses IBKR's own GTD (Good-Till-Date) time-in-force so the broker
    itself auto-cancels the order at good_till_et even if this bot's own
    polling loop is down when that time arrives -
    check_pending_touch_turn_orders still independently checks and
    cancels on its own schedule too, as a second line of defense (same
    "don't rely on a single mechanism for something this important"
    reasoning the protective stop order already gets)."""
    contract = _qualify(ib, symbol)
    action = "BUY" if side == "long" else "SELL"
    order = LimitOrder(action, quantity, round(limit_price, 2))
    order.tif = "GTD"
    order.goodTillDate = good_till_et.strftime("%Y%m%d %H:%M:%S US/Eastern")
    if getattr(ib, "account", None):
        order.account = ib.account
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
    for p in scoped_positions(ib):
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


def _fetch_sst_daily_bars(symbol: str) -> pd.DataFrame | None:
    """Daily OHLC for SST Swing (src.sst_swing.evaluate_sst_entry/
    trailing_stop_update) - 1y comfortably covers every lookback that
    module needs, including its 200-day SMA obstruction check
    (avoid_200sma_obstruction). Only ever called once/day (sst_swing_live.
    py's own scheduled job, run near 09:35 ET per G-SST-6 specifically so
    the prior session's bar is already closed), never from the per-minute
    cycle, so a fresh yfinance fetch per symbol per day is cheap enough -
    no caching needed the way the intraday-bar helpers above might
    eventually want.

    Drops today's row if yfinance already has one - same "once market is
    open, the last daily row is today's still-forming bar" fact
    get_prior_close's own comment notes, except here it's not a minor
    off-by-one, it would corrupt every rule in src/sst_swing.py (all of
    which treat the frame's LAST row as "the closed day being evaluated"
    - see evaluate_sst_entry's own docstring): a 5-minutes-old bar reads
    as an extreme-narrow-range day, silently poisoning classify_days'
    significant/inside classification and the DMI trigger's smoothing."""
    try:
        bars = yf.Ticker(symbol.replace(" ", "-")).history(period="1y", interval="1d")
        if bars.empty:
            return None
        today = datetime.now(ET).date()
        bars = bars[bars.index.date < today]
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


def get_last_price_quote(symbol: str) -> dict:
    """The most recent known price for `symbol`, whatever session it came
    from - regular, pre-market, or after-hours - unlike _current_price
    (regular session only, frozen outside 9:30-16:00 ET) or
    get_extended_hours_quote (only populated inside the pre/post windows,
    empty the rest of the time, including overnight when neither session
    is active). This is the "last price + change" a broker app's
    portfolio view always shows, 24/7, for the dashboard's Account
    Holdings table. Returns {"price": float|None, "change_pct":
    float|None, "ref_price": float|None} - all None on a fetch failure
    or if there's truly nothing cached yet (a brand new symbol with no
    trading history at all). change_pct/ref_price are relative to
    yesterday's regular close, same convention as daily_pnl - like
    get_extended_hours_quote, change_pct is the SYMBOL's raw move, not
    any position's P&L; a caller multiplies by a signed qty for that."""
    empty = {"price": None, "change_pct": None, "ref_price": None}
    try:
        bars = yf.Ticker(symbol.replace(" ", "-")).history(period="1d", interval="1m", prepost=True)
    except Exception:
        return dict(empty)
    if bars.empty:
        return dict(empty)
    price = float(bars["Close"].iloc[-1])
    ref = get_prior_close(symbol)
    change_pct = ((price - ref) / ref * 100) if ref else None
    return {"price": price, "change_pct": change_pct, "ref_price": ref}


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


def _compute_ema(closes: pd.Series, period: int) -> float | None:
    """Standard EMA (alpha = 2/(period+1), via pandas' own `span=` - not
    Wilder's alpha=1/period smoothing _compute_atr/_compute_rsi use, that
    convention is specific to those two indicators) as of the most recent
    bar of `closes`. None if there isn't enough history yet for a
    meaningful value. Same continues-across-the-session-boundary
    convention as _compute_rsi (closes is the whole multi-day intraday
    series, not reset each day) - standard practice for an intraday EMA,
    and keeps this consistent with how I2's RSI option already works."""
    if len(closes) < period:
        return None
    value = closes.ewm(span=period, adjust=False).mean().iloc[-1]
    return float(value) if pd.notna(value) else None


def _compute_atr(daily: pd.DataFrame, period: int = 14) -> float | None:
    """Wilder's ATR (Average True Range) as of the last COMPLETE trading
    day in `daily` — excludes daily.iloc[-1] (today, still in progress),
    same reasoning as the D2 filter's sma200/sma50 slicing just above: a
    "how much does this stock normally move" figure shouldn't include a
    partial day's not-yet-final range. None if there isn't enough history
    yet for a meaningful value (needs period+1 complete days - the extra
    one is consumed by the prior-close shift True Range itself needs)."""
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


def get_chart_ma_series(symbol: str, bars: pd.DataFrame, interval: str) -> dict:
    """20-day and 200-day moving-average lines for the dashboard's chart
    modal — an indicator that actually moves over time, unlike get_sma's
    single latest-value threshold (left untouched; it serves a separate
    purpose, showing what D2 checks against). On the daily interval this
    is the plain rolling mean of the visible closes themselves — the
    standard definition traders expect on a daily chart. On intraday
    intervals a day-based average can't move mid-session, so each
    trading day is held flat at the average through its prior close
    (today's own still-forming daily bar is excluded, same convention
    as get_sma), stepping only once a day actually closes."""
    empty = {"sma20_series": [], "sma200_series": []}
    try:
        daily = yf.Ticker(symbol.replace(" ", "-")).history(period="600d", interval="1d")
    except Exception:
        return empty
    if daily.empty:
        return empty

    def series_for(period):
        if len(daily) < period + 1:
            return []
        if interval == "1d":
            sma = bars["Close"].rolling(period).mean()
            return [
                {"time": int(ts.timestamp()), "value": round(float(v), 4)}
                for ts, v in sma.items() if pd.notna(v)
            ]
        by_date = {
            ts.date(): float(v)
            for ts, v in daily["Close"].rolling(period).mean().shift(1).items() if pd.notna(v)
        }
        return [
            {"time": int(ts.timestamp()), "value": round(by_date[ts.date()], 4)}
            for ts in bars.index if ts.date() in by_date
        ]

    return {"sma20_series": series_for(20), "sma200_series": series_for(200)}


# ---------------------------------------------------------------- Step 4 ---
def _breakeven_decision(pos: dict, exit_cfg: dict, r_multiple: float) -> dict:
    """Pure decision logic for manage_position's "pre_breakeven" stage —
    shared with backtest_engine.py's exit simulator so both replay the
    exact same threshold instead of risking two copies drifting apart.
    Only meaningful while pos["state"] == "pre_breakeven"; returns
    {"action": "hold"} otherwise-uninteresting ticks.

    No partial-profit stage anymore (removed after backtest data across
    every strategy showed its trades averaging barely more than a single
    trade's commission - $6.35 avg vs $13.32 for a trade that instead
    rode the trailing stop - so closing part of the position early was
    costing more in forgone upside than it banked in locked-in profit):
    the full position now holds untouched until r_multiple clears
    breakeven_trigger_R, then moves straight to trailing.

    exit_cfg["breakeven_trigger_R"] is absent for every "no_stop_delayed_
    trail" strategy (v4.1/v4.2/v4.3/V8/V9/V10 - see EXTRA_STRATEGY_
    PRESETS in src/db.py) since backtest_engine.py's own real simulator
    for that management_style never uses breakeven at all (MFE-triggered
    trailing instead - see its own docstring). manage_position has no
    live-engine implementation of that style yet (only "fixed_target_
    no_trail"/"staged_trail" are specially handled; everything else,
    including "no_stop_delayed_trail", falls through to this generic
    default path) - a REAL, currently-live bug found 2026-09-03: every
    open position under one of those strategies was hitting this as a
    bare exit_cfg["breakeven_trigger_R"] and crashing the ENTIRE cycle
    (management AND entry scanning) on every single tick, all day. This
    silently holds instead - no breakeven flip, no trailing, but the
    position's own real broker-side stop order (placed at entry) still
    protects it - rather than crashing the cycle every 5 minutes. Proper
    live support for this management_style (real hard-stop placement,
    MFE-triggered trailing) is a separate, larger task that needs its own
    careful implementation and testing before it touches live capital."""
    trigger = exit_cfg.get("breakeven_trigger_R")
    if trigger is None:
        return {"action": "hold"}
    if r_multiple >= trigger:
        return {"action": "breakeven_flip", "new_stop_price": pos["entry_price"], "new_state": "post_breakeven"}
    return {"action": "hold"}


def _profit_lock_decision(pos: dict, exit_cfg: dict, mfe_r: float) -> dict:
    """MFE-based variant of _breakeven_decision, for staged_trail strategies
    opted in via exit_cfg["profit_lock_offset_R"] (currently ORB Long v2 /
    ORB Short v2 only, NOT their Fade siblings - see ORB_v2_MFE_2R_Profit_
    Lock_025R.md). Investigation (this same conversation, before this
    change) found trades whose price wicked past +2R and reversed before
    any tick's close/current-price cleared it - _breakeven_decision never
    saw the wick (its r_multiple comes from Close/current price only) and
    the position rode its ORIGINAL stop the rest of the way down. This
    triggers off the position's own tracked MFE instead (pos["mfe_price"],
    the real intrabar high/low touch _update_excursion already maintains
    - the exact same number the trade diagnostics' MFE column reports),
    and locks in a small profit (entry +/- profit_lock_offset_R) rather
    than flat entry - a wick that proves +2R happened is worth banking
    something on, even though the position never actually traded there.

    Only meaningful while pos["state"] == "pre_breakeven" (same contract
    as _breakeven_decision), and only ever fires ONCE per position - the
    caller's state machine flips pos["state"] to "post_breakeven" the
    moment this returns "breakeven_flip" and never calls back in here for
    that position again, so there's no separate flag to track."""
    if mfe_r < exit_cfg["breakeven_trigger_R"]:
        return {"action": "hold"}
    side = pos.get("side", "long")
    entry = pos["entry_price"]
    initial_risk = (pos["initial_stop"] - entry) if side == "short" else (entry - pos["initial_stop"])
    offset = exit_cfg["profit_lock_offset_R"] * initial_risk
    new_stop = (entry - offset) if side == "short" else (entry + offset)
    return {"action": "breakeven_flip", "new_stop_price": new_stop, "new_state": "post_breakeven"}


def _trailing_stop_decision(pos: dict, swing_stop_candidate: float | None) -> dict:
    """Pure decision logic for manage_position's "post_breakeven" trailing
    stage — same sharing rationale as _breakeven_decision. The
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


class _PositionOps:
    """Execution side of position management - the shared DECISION logic
    (branch selection, r_multiple/MFE/MAE math, and calls into the pure
    _breakeven_decision/_trailing_stop_decision/orb.fixed_target_decision
    functions) lives in _manage_position_core below and is IDENTICAL for a
    real or virtual position; only what happens when a decision says
    "reposition the stop" or "close the position" differs - a real
    position touches IBKR, a virtual one only ever touches the DB. This
    split exists specifically so a live-money incident class (subtly
    different behavior between two independently-maintained copies of
    this logic) can't happen - there is exactly one copy of the decision
    logic, and this class's two subclasses are the only thing that
    changes between managing a real vs. a virtual position."""

    def reposition_stop(self, pos: dict, new_stop_price: float, side: str) -> int | None:
        raise NotImplementedError

    def close_position(self, pos: dict) -> bool:
        raise NotImplementedError

    def save(self, pos: dict):
        raise NotImplementedError

    def notify(self, title: str, body: str, priority: str = "default"):
        raise NotImplementedError

    def log(self, event: str, **fields):
        raise NotImplementedError


class _RealPositionOps(_PositionOps):
    """Unchanged real-money behavior - every call here is byte-identical
    to what manage_position itself used to do directly before this
    refactor (see test_no_stop_delayed_trail_live.py, which still passes
    unmodified against this class)."""

    def __init__(self, account_id: int, mode: str, ib):
        self.account_id, self.mode, self.ib = account_id, mode, ib

    def reposition_stop(self, pos, new_stop_price, side):
        _cancel_stop(self.ib, pos.get("stop_order_id"))
        return _place_stop(self.ib, pos["symbol"], pos["qty"], new_stop_price, side)

    def close_position(self, pos: dict) -> bool:
        side = pos.get("side", "long")
        _cancel_stop(self.ib, pos.get("stop_order_id"))
        closed = _market_close(self.account_id, self.mode, self.ib, pos["symbol"], pos["qty"], side)
        if not closed:
            closed = _broker_position(self.ib, pos["symbol"]) is None
        if closed:
            db.remove_position(self.account_id, self.mode, pos["symbol"])
        else:
            # Same delayed-fill race force_close_all already handles: the
            # stop was just cancelled to make way for the close attempt,
            # so re-arm it immediately rather than leaving the position
            # unprotected, and let the next cycle retry the close.
            pos["stop_order_id"] = _place_stop(self.ib, pos["symbol"], pos["qty"], pos["stop_price"], side)
        return closed

    def save(self, pos):
        db.upsert_position(self.account_id, self.mode, pos)

    def notify(self, title, body, priority="default"):
        notify(f"[{self.mode.upper()}] {title}", body, priority)

    def log(self, event, **fields):
        log_decision(self.account_id, self.mode, {"event": event, **fields})


class _VirtualPositionOps(_PositionOps):
    """Fully simulated (no IBKR connection, no real order ever placed) -
    see virtual_positions' own schema comment (src/db.py) for why. A stop
    reposition here just means the new level IS the simulated stop
    (pos["stop_price"] itself, set by the shared core same as for a real
    position) - there's no broker order id to track, so reposition_stop
    always returns None. close_position always succeeds immediately (no
    delayed-fill race to retry - a simulated fill happens the instant the
    decision logic says it should) and records the closed trade to
    virtual_trades before removing the open virtual_positions row."""

    def __init__(self, account_id: int, strategy_id: int, strategy_label: str):
        self.account_id, self.strategy_id, self.strategy_label = account_id, strategy_id, strategy_label

    def reposition_stop(self, pos, new_stop_price, side):
        return None

    def close_position(self, pos: dict) -> bool:
        side = pos.get("side", "long")
        exit_price = pos["target_price"]
        pnl = ((exit_price - pos["entry_price"]) if side == "long" else (pos["entry_price"] - exit_price)) * pos["qty"]
        db.record_virtual_trade(self.account_id, self.strategy_id, {
            "symbol": pos["symbol"], "side": side, "entry_price": pos["entry_price"],
            "entry_time_iso": pos["entry_time_iso"], "exit_price": exit_price,
            "exit_time_iso": datetime.now(ET).isoformat(timespec="seconds"), "qty": pos["qty"],
            "final_r": pos.get("r_multiple"), "pnl_dollars": pnl, "exit_reason": "target",
        })
        db.remove_virtual_position(self.account_id, self.strategy_id, pos["symbol"])
        return True

    def save(self, pos):
        db.upsert_virtual_position(self.account_id, self.strategy_id, pos)

    def notify(self, title, body, priority="default"):
        notify(f"[VIRTUAL] {self.strategy_label}: {title}", body, priority)

    def log(self, event, **fields):
        log_decision(self.account_id, "live", {"event": event, "strategy_id": self.strategy_id, "virtual": True, **fields})


def _manage_position_core(pos: dict, rules: dict, ops: _PositionOps) -> dict:
    """The full decision logic for managing one open position - shared
    verbatim between a real position (manage_position) and a simulated
    one (manage_virtual_position); see _PositionOps' own docstring for
    why this split exists. rules must be the exit config for pos["side"]
    — see run_cycle, which picks the right (long or short) active
    strategy per position, falling back to a safe default if that side no
    longer has an active strategy (a position stays managed even if its
    strategy was deactivated or deleted after it was opened). The two
    breakeven/trailing stages below are deliberately separate `if`s, not
    `if`/`elif` — a breakeven flip and a trailing-stop check can both fire
    on the same tick, since the state mutates in between (see
    _breakeven_decision/_trailing_stop_decision, the pure decision logic
    both stages and backtest_engine.py share).

    pos["no_bot_manage"] (set once, at creation, by check_price_triggers -
    see its own comment) short-circuits ALL of the above: no breakeven
    flip, no trailing stop, not even the purely-observational r_multiple/
    mae_price tracking every other position gets. A manually-triggered
    position is deliberately left alone entirely - its own initial
    protective stop (placed for real at fill time) is the only thing that
    ever touches it again, until a human closes it by hand. force_close_
    all's own held_no_manage partition is what keeps it out of the EOD
    sweep too - the two together are what "not bot-managed" actually
    means end to end."""
    if pos.get("no_bot_manage"):
        return pos
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

    # Worst price seen since entry, same per-cycle (not truly intrabar)
    # sampling as mfe_price above - tracked for EVERY position regardless
    # of management_style, purely observational (read only by the
    # Decision Intelligence Center dashboard and the V10 dynamic_recovery
    # live-parity replay's mae_r feature - see db.upsert_position's own
    # migration comment). Never influences any stop/trailing/exit
    # decision below.
    pos["mae_price"] = min(pos.get("mae_price") or entry, price) if side == "long" else max(pos.get("mae_price") or entry, price)

    if exit_cfg.get("management_style") == "fixed_target_no_trail":
        # ORB positions: no breakeven flip, no swing trailing - the broker-side
        # stop placed at entry (initial_stop, never moved) protects the
        # downside; this only ever watches for the fixed R:R target (see
        # orb.fixed_target_decision) and exits the WHOLE position there.
        decision = orb.fixed_target_decision(pos, price, side)
        if decision["action"] == "close_target":
            closed = ops.close_position(pos)
            if closed:
                ops.notify(f"TARGET {pos['symbol']}", f"closed @ target ~${pos['target_price']:.2f}", "default")
                ops.log("target_close", symbol=pos["symbol"], side=side, target=pos["target_price"])
                pos["qty"] = 0
            else:
                ops.notify(f"TARGET CLOSE FAILED: {pos['symbol']}", "still holding after target-close attempt - stop re-armed, will retry next cycle", "high")
        if pos["qty"] > 0:
            ops.save(pos)
        return pos

    if exit_cfg.get("management_style") == "staged_trail":
        # ORB v2 positions: original stop stays untouched up to
        # breakeven_trigger_R (2R by default), then flips to breakeven -
        # same _breakeven_decision every other strategy already uses, no
        # ORB-specific logic needed there. Trailing then only STARTS once
        # r_multiple also clears trailing_trigger_R (3R by default) - an
        # extra gate _trailing_stop_decision alone doesn't have - and
        # trails below the low (long) / above the high (short) of the
        # last 2 5-minute bars (orb.low_of_last_n_bars/high_of_last_n_bars),
        # not a swing-pivot detection like the legacy strategies' own
        # trailing (_find_latest_swing_low/high).
        if pos["state"] == "pre_breakeven":
            decision = _breakeven_decision(pos, exit_cfg, r_multiple)
            if decision["action"] == "breakeven_flip":
                pos["stop_order_id"] = ops.reposition_stop(pos, decision["new_stop_price"], side)
                pos["stop_price"] = decision["new_stop_price"]
                pos["state"] = decision["new_state"]
                ops.notify(f"BE {pos['symbol']}", f"stop -> ${entry:.2f}", "default")
                ops.log("breakeven_flip", symbol=pos["symbol"], side=side, new_stop=entry)

        trailing_trigger_r = exit_cfg.get("trailing_trigger_R", 3.0)
        if pos["state"].startswith("post_breakeven") and r_multiple >= trailing_trigger_r:
            bars = _get_5min_bars(pos["symbol"])
            candidate = None
            if bars is not None and len(bars) >= 2:
                candidate = orb.low_of_last_n_bars(bars, 2) if side == "long" else orb.high_of_last_n_bars(bars, 2)
            decision = _trailing_stop_decision(pos, candidate)
            if decision["action"] == "trail_stop":
                pos["stop_order_id"] = ops.reposition_stop(pos, decision["new_stop_price"], side)
                old_stop = pos.get("stop_price", pos["initial_stop"])
                pos["stop_price"] = decision["new_stop_price"]
                ops.notify(f"TRAIL {pos['symbol']}", f"stop ${old_stop:.2f} -> ${decision['new_stop_price']:.2f}", "default")
                ops.log("trail_stop", symbol=pos["symbol"], side=side, old=old_stop, new=decision["new_stop_price"])

        if pos["qty"] > 0:
            ops.save(pos)
        return pos

    if exit_cfg.get("management_style") == "no_stop_delayed_trail":
        # ORB v4.1/v4.2/v4.3/V8/V9/V10 - mirrors backtest_engine.py's own
        # "no_stop_delayed_trail" simulator (see its own docstring there,
        # and entry_scan's own comment for the real-stop-placement half of
        # this): no breakeven stage at all - the position holds under its
        # real resting stop (hard_stop_price if the strategy opted in via
        # exit_cfg["hard_stop_R"], else the tight initial_stop for v4.1)
        # until MFE clears exit_cfg["trailing_trigger_R"], at which point
        # it switches straight to the SAME swing-trailing
        # (_trailing_stop_decision) staged_trail already uses above - not
        # v2/v3's own breakeven-then-trail two-stage progression.
        #
        # hard_stop_price/mfe_price/trail_activated/trail_activated_at_r
        # are set at entry (see entry_scan) for anything opened after this
        # was added. A position already open in the DB from BEFORE this
        # branch existed (crash-looping since it fell through to the
        # generic default path below and hit exit_cfg["breakeven_
        # trigger_R"] missing - see this file's own git history) has none
        # of them - backfilled here from the SAME entry_price/initial_stop
        # this position was already sized against (never guessed), and its
        # real resting stop order is repositioned to match on the very
        # first cycle this runs against it.
        if "hard_stop_price" not in pos or pos["hard_stop_price"] is None:
            hard_stop_r = exit_cfg.get("hard_stop_R")
            if hard_stop_r is not None:
                backfilled_hard_stop = (entry + hard_stop_r * initial_risk) if side == "short" else (entry - hard_stop_r * initial_risk)
                old_stop = pos.get("stop_price", pos["initial_stop"])
                pos["stop_order_id"] = ops.reposition_stop(pos, backfilled_hard_stop, side)
                pos["hard_stop_price"] = backfilled_hard_stop
                pos["stop_price"] = backfilled_hard_stop
                ops.notify(
                    f"STOP CORRECTED {pos['symbol']}",
                    f"opened before this strategy's real hard-stop tracking existed - repositioning stop ${old_stop:.2f} -> ${backfilled_hard_stop:.2f} to match its intended {hard_stop_r}R hard stop",
                    "high",
                )
                ops.log("hard_stop_backfilled", symbol=pos["symbol"], side=side, old=old_stop, new=backfilled_hard_stop)
            else:
                pos["hard_stop_price"] = None
        if pos.get("mfe_price") is None:
            pos["mfe_price"] = entry
        pos.setdefault("trail_activated", False)
        pos.setdefault("trail_activated_at_r", None)

        # Best price seen since entry, per-cycle granularity (not truly
        # intrabar like backtest_engine.py's own _update_excursion - live
        # only samples price once per cycle, the same approximation every
        # other live MFE-adjacent read in this file already accepts).
        pos["mfe_price"] = max(pos["mfe_price"], price) if side == "long" else min(pos["mfe_price"], price)

        if pos["trail_activated"]:
            bars = _get_5min_bars(pos["symbol"])
            candidate = None
            if bars is not None and len(bars) >= 2:
                candidate = orb.low_of_last_n_bars(bars, 2) if side == "long" else orb.high_of_last_n_bars(bars, 2)
            decision = _trailing_stop_decision(pos, candidate)
            if decision["action"] == "trail_stop":
                pos["stop_order_id"] = ops.reposition_stop(pos, decision["new_stop_price"], side)
                old_stop = pos.get("stop_price", pos["initial_stop"])
                pos["stop_price"] = decision["new_stop_price"]
                ops.notify(f"TRAIL {pos['symbol']}", f"stop ${old_stop:.2f} -> ${decision['new_stop_price']:.2f}", "default")
                ops.log("trail_stop", symbol=pos["symbol"], side=side, old=old_stop, new=decision["new_stop_price"])
        else:
            mfe_r = ((entry - pos["mfe_price"]) if side == "short" else (pos["mfe_price"] - entry)) / initial_risk
            trailing_trigger_r = exit_cfg.get("trailing_trigger_R", 1.20)
            if mfe_r >= trailing_trigger_r:
                pos["trail_activated"] = True
                pos["trail_activated_at_r"] = trailing_trigger_r
                bars = _get_5min_bars(pos["symbol"])
                candidate = None
                if bars is not None and len(bars) >= 2:
                    candidate = orb.low_of_last_n_bars(bars, 2) if side == "long" else orb.high_of_last_n_bars(bars, 2)
                decision = _trailing_stop_decision(pos, candidate)
                new_stop_note = pos.get("stop_price", pos["initial_stop"])
                if decision["action"] == "trail_stop":
                    pos["stop_order_id"] = ops.reposition_stop(pos, decision["new_stop_price"], side)
                    pos["stop_price"] = decision["new_stop_price"]
                    new_stop_note = decision["new_stop_price"]
                ops.notify(f"TRAILING ACTIVATED {pos['symbol']}", f"MFE cleared {trailing_trigger_r}R, stop -> ${new_stop_note:.2f}", "default")
                ops.log("trail_activated", symbol=pos["symbol"], side=side, at_r=trailing_trigger_r, stop=new_stop_note)

        if pos["qty"] > 0:
            ops.save(pos)
        return pos

    if exit_cfg.get("management_style") == "sst_swing_trail":
        # SST Swing (see src/sst_swing.py, docs/sst_swing_spec.md): no
        # breakeven stage, no intraday trailing of any kind. The real stop
        # is a resting broker order and protects the downside on its own
        # between cycles - nothing here needs to poll price to keep it
        # safe. Rule 6's trailing_stop_update only ever moves the stop
        # once per day, on a new daily bar close, from sst_swing_live's
        # own once-a-day scheduled job (G-SST-6), never from this
        # per-minute loop - this branch exists purely so r_multiple/
        # mae_price (both observational, see their own comments above)
        # still get refreshed and saved every cycle like every other
        # position's, without any of the intraday stages below (staged
        # breakeven flips, swing-pivot trailing) ever mistakenly firing
        # against a position they were never designed for.
        if pos["qty"] > 0:
            ops.save(pos)
        return pos

    if pos["state"] == "pre_breakeven":
        decision = _breakeven_decision(pos, exit_cfg, r_multiple)
        if decision["action"] == "breakeven_flip":
            pos["stop_order_id"] = ops.reposition_stop(pos, decision["new_stop_price"], side)
            pos["stop_price"] = decision["new_stop_price"]
            pos["state"] = decision["new_state"]
            ops.notify(f"BE {pos['symbol']}", f"stop -> ${entry:.2f}", "default")
            ops.log("breakeven_flip", symbol=pos["symbol"], side=side, new_stop=entry)

    if pos["state"].startswith("post_breakeven"):
        bars = _get_5min_bars(pos["symbol"])
        swing_stop_candidate = None
        if bars is not None and len(bars) > 5:
            swing = _find_latest_swing_high(bars) if side == "short" else _find_latest_swing_low(bars)
            if swing is not None:
                swing_stop_candidate = (swing + 0.01) if side == "short" else (swing - 0.01)
        decision = _trailing_stop_decision(pos, swing_stop_candidate)
        if decision["action"] == "trail_stop":
            pos["stop_order_id"] = ops.reposition_stop(pos, decision["new_stop_price"], side)
            old_stop = pos.get("stop_price", pos["initial_stop"])
            pos["stop_price"] = decision["new_stop_price"]
            ops.notify(f"TRAIL {pos['symbol']}", f"stop ${old_stop:.2f} -> ${decision['new_stop_price']:.2f}", "default")
            ops.log("trail_stop", symbol=pos["symbol"], side=side, old=old_stop, new=decision["new_stop_price"])

    if pos["qty"] > 0:
        ops.save(pos)
    return pos


def manage_position(account_id: int, mode: str, ib, pos: dict, rules: dict) -> dict:
    """Manages one real, broker-backed position - see _manage_position_core
    for the actual decision logic (identical to manage_virtual_position's,
    just executed against IBKR instead of the DB alone)."""
    return _manage_position_core(pos, rules, _RealPositionOps(account_id, mode, ib))


def manage_virtual_position(account_id: int, strategy_id: int, strategy_label: str, pos: dict, rules: dict) -> dict:
    """Manages one simulated position for a 'virtual' strategy_run - same
    decision logic as manage_position (see _manage_position_core), just
    never touches IBKR: a stop "reposition" only ever updates pos itself,
    and a close is always immediate (no delayed-fill race to retry, since
    nothing was ever actually sent to a broker to fill)."""
    return _manage_position_core(pos, rules, _VirtualPositionOps(account_id, strategy_id, strategy_label))


# ---------------------------------------------------------------- Step 6 ---
def _is_swing_hold(pos: dict) -> bool:
    """True for a position whose OWN strategy (looked up fresh by its
    strategy_id - see _rules_for_position's docstring on why "the position's
    own strategy" and "whatever's active right now" are not the same thing)
    declares no_eod_force_close in its rules_json. SST Swing sets this: it
    holds days-to-weeks and is closed only by its own trailing stop, never
    by the EOD clock. Unlike hold_overnight (a one-shot, per-day, human
    toggle - see db.set_hold_overnight), this is a standing property of the
    position for as long as it stays open: nothing to reset, checked fresh
    every EOD. A position with no strategy_id (predates multi-strategy
    support, or was opened unattributed) is never a swing hold."""
    strategy_id = pos.get("strategy_id")
    if strategy_id is None:
        return False
    strategy = db.get_strategy(strategy_id)
    if strategy is None:
        return False
    return bool(json.loads(strategy["rules_json"]).get("no_eod_force_close"))


def force_close_all(account_id: int, mode: str, ib, positions: list[dict]):
    if not positions:
        return

    manual_held = [p for p in positions if p.get("hold_overnight")]
    swing_held = [p for p in positions if not p.get("hold_overnight") and _is_swing_hold(p)]
    no_manage_held = [p for p in positions if not p.get("hold_overnight") and not _is_swing_hold(p) and p.get("no_bot_manage")]
    held = manual_held + swing_held + no_manage_held
    to_close = [p for p in positions if p not in held]
    for pos in manual_held:
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
    for pos in swing_held:
        # Not one-shot - this position stays exempt every day it's open,
        # per its own strategy's rules, not a per-day human request.
        log_decision(account_id, mode, {"event": "force_close_skipped", "symbol": pos["symbol"], "side": pos.get("side", "long"), "qty": pos["qty"], "reason": "swing_hold"})
        notify(
            f"[{mode.upper()}] Swing hold: {pos['symbol']}",
            "EOD force-close skipped - this position's own strategy holds multi-day, closes only via its own trailing stop",
            "default",
        )
    for pos in no_manage_held:
        # Same "permanent, not a per-day toggle" reasoning as swing_held -
        # a manually-triggered position (see check_price_triggers' own
        # comment) stays exempt every day it's open, not just today.
        log_decision(account_id, mode, {"event": "force_close_skipped", "symbol": pos["symbol"], "side": pos.get("side", "long"), "qty": pos["qty"], "reason": "no_bot_manage"})
        notify(
            f"[{mode.upper()}] Not bot-managed: {pos['symbol']}",
            "EOD force-close skipped - manually-triggered position, the bot never manages or closes it - close by hand when ready",
            "default",
        )

    if not to_close:
        return

    notify(
        f"[{mode.upper()}] EOD Force Close",
        f"flattening {len(to_close)} position(s)" + (f" ({len(held)} held" + (f", {len(swing_held)} swing" if swing_held else "") + (f", {len(no_manage_held)} manual" if no_manage_held else "") + ")" if held else ""),
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


# Every strategy preset's exit.initial_stop_rule value. Two kinds:
#   - "session_extreme": a flat % offset off today's session low/high (the
#     original rules) - same distance regardless of how volatile the stock
#     actually is today.
#   - "atr_multiple": entry price offset by atr_multiplier * ATR(14) off
#     the last 14 COMPLETE trading days (see _compute_atr) - a wide-range
#     stock gets a wider stop, a tight one a tighter stop, instead of the
#     same flat 1% for both.
# Adding a new rule only ever means adding an entry here - both
# _evaluate_filters_from_bars (which reference price to read for a
# session_extreme rule) and _resolve_initial_stop (the offset actually
# applied) key off this same table, so the two can never quietly disagree
# about what a rule means.
INITIAL_STOP_RULES = {
    "lod_minus_1pct": {"kind": "session_extreme", "reference": "lod", "multiplier": 0.99},
    "hod_plus_1pct": {"kind": "session_extreme", "reference": "hod", "multiplier": 1.01},
    "atr_2x": {"kind": "atr_multiple", "atr_multiplier": 2.0},
}


def _initial_stop_rule(rules: dict, side: str) -> dict:
    """Looks up exit.initial_stop_rule in INITIAL_STOP_RULES. Missing or
    unrecognized (a typo, or a strategy predating this field) falls back to
    the side's own natural rule - lod_minus_1pct for a long, hod_plus_1pct
    for a short - so every existing strategy keeps behaving exactly as
    before rather than silently ending up with no stop logic at all."""
    default_rule = "lod_minus_1pct" if side == "long" else "hod_plus_1pct"
    rule_name = rules.get("exit", {}).get("initial_stop_rule", default_rule)
    return INITIAL_STOP_RULES.get(rule_name, INITIAL_STOP_RULES[default_rule])


def _initial_stop_reference(rules: dict, side: str) -> str:
    """The "lod"/"hod" half of a session_extreme rule, for
    _evaluate_filters_from_bars to know which session extreme to read
    stop_ref off of. An atr_multiple rule doesn't use stop_ref for its own
    math (see _resolve_initial_stop) but stop_ref is still computed and
    returned regardless - harmless, and keeps the detail dict's shape the
    same no matter which rule is active - so this still needs an answer:
    falls back to the side's own natural session extreme."""
    rule = _initial_stop_rule(rules, side)
    return rule.get("reference", "lod" if side == "long" else "hod")


def _resolve_initial_stop(detail: dict, rules: dict, side: str) -> float:
    """The offset half of exit.initial_stop_rule - turns the signal detail
    _evaluate_filters_from_bars already produced (stop_ref/price/atr, all
    per the same rule) into the actual initial stop price. Shared by
    entry_scan (live) and backtest_engine.simulate_strategy so a rule
    change can't quietly drift between the two.

    A session_extreme rule applies its % multiplier to stop_ref, unchanged
    from before. An atr_multiple rule instead offsets the ENTRY PRICE by
    atr_multiplier * ATR - if ATR couldn't be computed (not enough daily
    history), falls back to the side's own default session_extreme rule
    rather than leaving the position with no stop logic at all, same
    "never leave a position unmanaged" reasoning as the missing/
    unrecognized-rule fallback above."""
    rule = _initial_stop_rule(rules, side)
    if rule["kind"] == "atr_multiple":
        atr = detail.get("atr")
        if atr:
            price = detail["price"]
            return (price - rule["atr_multiplier"] * atr) if side == "long" else (price + rule["atr_multiplier"] * atr)
        rule = INITIAL_STOP_RULES["lod_minus_1pct" if side == "long" else "hod_plus_1pct"]
    return detail["stop_ref"] * rule["multiplier"]


# ---------------------------------------------------------------- Step 8 ---
def _evaluate_filters_from_bars(
    daily: pd.DataFrame, intraday: pd.DataFrame, rules: dict, side: str,
    prior_day_bars: dict | None = None, signal_side: str | None = None,
    daily_derived_cache: dict | None = None,
) -> dict:
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
    volume) is direction-agnostic.

    prior_day_bars is a purely-optional performance hook for I3: a
    {date: DataFrame} map of that symbol's earlier trading days' bars,
    already split out. When omitted (the live path, which only ever calls
    this once per symbol per real tick) I3 derives the same thing itself
    from `intraday` - same result either way, just slower to rederive on
    every call, which only matters for backtest_engine.py's per-simulated-
    tick calling pattern (hundreds of calls a day per symbol) - see its
    own precomputed prior_day_bars_by_symbol for why it bothers passing
    this in.

    daily_derived_cache is a purely-optional performance hook, exactly
    like prior_day_bars above: a dict this function memoizes SMA200/
    SMA50/ATR into, keyed by name. Safe because all three depend only on
    `daily`, never on `intraday`/the tick being evaluated - the live path
    (which passes nothing, one call per real tick) computes them fresh
    every time same as always; backtest_engine.py passes the SAME dict
    back in across every simulated tick of one (symbol, day), so the
    identical DataFrame only gets summed/EWM'd once instead of up to
    ~24 times for an identical result.

    signal_side decouples WHICH setup fires entry from WHICH side the
    trade actually executes on - e.g. detect a textbook long breakout
    (D1-D3/I1-I3 evaluated exactly as they'd read for a long strategy)
    but FADE it: short into the breakout instead of buying it. Defaults
    to `side` (every existing strategy's rules omit it, so this is a
    no-op for them - D1-D3/I1-I3 and the trade itself always agree,
    unchanged). Only the signal-detection booleans read signal_side;
    stop_ref (and therefore initial_stop/sizing/exits downstream) is keyed
    off exit.initial_stop_rule (see _initial_stop_reference),
    which itself defaults to the real trade direction (`side`) when the
    rule is absent/unrecognized - never signal_side either way: a fade
    short still needs a short's own stop (above price): same signal,
    opposite trade, stop/target sized for the trade actually being
    placed."""
    daily_filters = rules["daily_filters"]
    intraday_filters = rules["intraday_filters"]
    signal_side = signal_side or side
    cache = daily_derived_cache if daily_derived_cache is not None else {}

    if len(daily) < 201:
        return {"pass": False, "side": side, "error": "not enough daily history"}
    prior_day = daily.iloc[-2]
    if "sma200" not in cache:
        cache["sma200"] = daily["Close"].iloc[-201:-1].mean()
    sma200 = cache["sma200"]

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

    if signal_side == "long":
        d1 = current_price > float(prior_day["High"])  # above yesterday's high
        d2 = float(prior_day["Close"]) > float(sma200)  # yesterday's close above the 200-day SMA
        d3 = gap_pct >= daily_filters["D3_min_gap_pct_from_prior_close"]  # gap up >= threshold
        premarket_extreme = float(premarket_bars["High"].max()) if not premarket_bars.empty else float("-inf")
        i1 = current_price > premarket_extreme  # above today's premarket high
        if "I2_rsi_above" in intraday_filters:
            rsi_value = _compute_rsi(intraday["Close"], intraday_filters.get("I2_rsi_period", 14))
            i2 = rsi_value is not None and rsi_value > intraday_filters["I2_rsi_above"]  # RSI above threshold
        elif intraday_filters.get("I2_ema_above"):
            ema_value = _compute_ema(intraday["Close"], intraday_filters.get("I2_ema_period", 9))
            i2 = ema_value is not None and current_price > ema_value  # above the short EMA
        else:
            extreme_so_far = float(today_bars["High"].iloc[:-1].max()) if len(today_bars) > 1 else float("-inf")
            i2 = current_price > extreme_so_far  # new high-of-day
    else:
        d1 = current_price < float(prior_day["Low"])  # below yesterday's low
        if "D2_prior_close_pct_above_sma50_min" in daily_filters:
            if "sma50" not in cache:
                cache["sma50"] = daily["Close"].iloc[-51:-1].mean()
            sma50 = cache["sma50"]
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
        elif intraday_filters.get("I2_ema_below"):
            ema_value = _compute_ema(intraday["Close"], intraday_filters.get("I2_ema_period", 9))
            i2 = ema_value is not None and current_price < ema_value  # below the short EMA
        else:
            extreme_so_far = float(today_bars["Low"].iloc[:-1].min()) if len(today_bars) > 1 else float("inf")
            i2 = current_price < extreme_so_far  # new low-of-day

    # stop_ref's reference price comes from exit.initial_stop_rule (via
    # side's own default when the rule is absent/unrecognized - see
    # _initial_stop_reference) - always keyed off the real TRADE direction
    # (side), never signal_side - a faded short still needs a short's own
    # stop (above price), even when the entry signal itself was detected
    # using the long-style D1-D3/I1-I3 definitions above. Computed (and
    # returned) unconditionally, even for an atr_multiple rule that won't
    # actually use it - see _resolve_initial_stop.
    if _initial_stop_reference(rules, side) == "lod":
        stop_ref = float(regular_bars["Low"].min()) if not regular_bars.empty else float(today_bars["Low"].min())
    else:
        stop_ref = float(regular_bars["High"].max()) if not regular_bars.empty else float(today_bars["High"].max())
    if "atr" not in cache:
        cache["atr"] = _compute_atr(daily)
    atr_value = cache["atr"]

    # I3: relative volume >= threshold (direction-agnostic) - today's
    # volume-so-far against the AVERAGE volume accumulated by this same
    # time-of-day over the past I3_rvol_lookback_days trading days
    # (apples-to-apples: partial session vs partial session). Comparing
    # against those days' FULL-session volume instead (as this used to)
    # made the threshold nearly unreachable early in the session - it
    # required already having traded double a whole day's normal volume
    # within the first half hour - and progressively easier for no real
    # reason as the session went on, rather than a genuine "is trading
    # unusually busy right now" signal. Needs intraday to reach back at
    # least that many days - see INTRADAY_FETCH_LOOKBACK_DAYS.
    lookback = intraday_filters["I3_rvol_lookback_days"]
    as_of_time = intraday.index[-1].time()
    if prior_day_bars is not None:
        prior_dates = sorted(prior_day_bars.keys())[-lookback:]
        prior_volume_by_this_time = [
            float(prior_day_bars[d][prior_day_bars[d].index.time <= as_of_time]["Volume"].sum())
            for d in prior_dates
        ]
    else:
        prior_dates = sorted({d for d in intraday.index.date if d < as_of_date})[-lookback:]
        prior_volume_by_this_time = [
            float(intraday[(intraday.index.date == d) & (intraday.index.time <= as_of_time)]["Volume"].sum())
            for d in prior_dates
        ]
    avg_volume_by_this_time = (sum(prior_volume_by_this_time) / len(prior_volume_by_this_time)) if prior_volume_by_this_time else 0.0
    today_volume_so_far = float(today_bars["Volume"].sum())
    rvol = today_volume_so_far / avg_volume_by_this_time if avg_volume_by_this_time else 0.0
    i3 = rvol >= intraday_filters["I3_rvol_min"]

    passed = bool(d1 and d2 and d3 and i1 and i2 and i3)
    return {
        "pass": passed, "side": side, "signal_side": signal_side,
        "D1": bool(d1), "D2": bool(d2), "D3": bool(d3),
        "I1": bool(i1), "I2": bool(i2), "I3": bool(i3),
        "price": current_price, "rvol": rvol, "gap_pct": gap_pct,
        "stop_ref": stop_ref, "rsi": rsi_value, "atr": atr_value,
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
        intraday = yf.Ticker(yahoo_symbol).history(period=f"{INTRADAY_FETCH_LOOKBACK_DAYS}d", interval="5m", prepost=True)
        if not intraday.empty:
            intraday.index = intraday.index.tz_convert(ET)
        _track_yfinance_fetch_success(account_id, mode)
        detail = _evaluate_filters_from_bars(daily, intraday, rules, side, signal_side=rules.get("signal_side"))
        detail["side"] = side
        event = "filter_eval" if "error" not in detail else "filter_eval_error"
        log_decision(account_id, mode, {"event": event, "symbol": ticker, **detail})
        return detail
    except Exception as exc:  # noqa: BLE001 - one bad ticker must not kill the scan
        _track_yfinance_fetch_failure(account_id, mode, ticker, exc)
        log_decision(account_id, mode, {"event": "filter_eval_error", "symbol": ticker, "side": side, "error": str(exc)})
        return {"pass": False, "side": side, "error": str(exc)}


def _evaluate_orb_entry(account_id: int, mode: str, ticker: str, rules: dict, side: str) -> dict:
    """ORB's own live wrapper - same fetch shape as _evaluate_entry_filters
    (fresh yfinance data ending at the real current moment), handed to
    orb.evaluate_orb_entry instead of cycle's own D1-D3/I1-I3 pure logic.
    Dispatched from entry_scan whenever this strategy's rules carry an
    "opening_range" key. ORB doesn't need 200 days of daily history (no
    SMA200 filter) - only enough for ATR(14) - but fetching the same
    260-day window as the D1-D3 path is harmless and keeps this one fetch
    shape shared rather than adding a second, narrower one."""
    yahoo_symbol = ticker.replace(" ", "-")
    try:
        daily = yf.Ticker(yahoo_symbol).history(period="260d", interval="1d")
        intraday = yf.Ticker(yahoo_symbol).history(period=f"{INTRADAY_FETCH_LOOKBACK_DAYS}d", interval="5m", prepost=True)
        if not intraday.empty:
            intraday.index = intraday.index.tz_convert(ET)
        _track_yfinance_fetch_success(account_id, mode)
        detail = orb.evaluate_orb_entry(daily, intraday, rules, side, signal_side=rules.get("signal_side"))
        detail["side"] = side
        event = "orb_filter_eval" if "error" not in detail else "orb_filter_eval_error"
        log_decision(account_id, mode, {"event": event, "symbol": ticker, **detail})
        return detail
    except Exception as exc:  # noqa: BLE001 - one bad ticker must not kill the scan
        _track_yfinance_fetch_failure(account_id, mode, ticker, exc)
        log_decision(account_id, mode, {"event": "orb_filter_eval_error", "symbol": ticker, "side": side, "error": str(exc)})
        return {"pass": False, "side": side, "error": str(exc)}


def _evaluate_touch_turn_entry(account_id: int, mode: str, ticker: str, rules: dict, side: str) -> dict:
    """Touch & Turn's own live wrapper - same fetch shape as
    _evaluate_entry_filters/_evaluate_orb_entry (fresh yfinance data
    ending at the real current moment), handed to
    touch_turn.evaluate_touch_turn_entry instead. Dispatched from
    touch_turn_entry_scan (entry_scan itself skips any strategy whose
    rules carry an "opening_candle" key - see its own docstring)."""
    yahoo_symbol = ticker.replace(" ", "-")
    try:
        daily = yf.Ticker(yahoo_symbol).history(period="260d", interval="1d")
        intraday = yf.Ticker(yahoo_symbol).history(period=f"{INTRADAY_FETCH_LOOKBACK_DAYS}d", interval="5m", prepost=True)
        if not intraday.empty:
            intraday.index = intraday.index.tz_convert(ET)
        _track_yfinance_fetch_success(account_id, mode)
        detail = touch_turn.evaluate_touch_turn_entry(daily, intraday, rules, side)
        event = "touch_turn_eval" if "error" not in detail else "touch_turn_eval_error"
        log_decision(account_id, mode, {"event": event, "symbol": ticker, **detail})
        return detail
    except Exception as exc:  # noqa: BLE001 - one bad ticker must not kill the scan
        _track_yfinance_fetch_failure(account_id, mode, ticker, exc)
        log_decision(account_id, mode, {"event": "touch_turn_eval_error", "symbol": ticker, "side": side, "error": str(exc)})
        return {"side": side, "error": str(exc)}


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


ES_DATA_UNAVAILABLE_NOTIFY_COOLDOWN_MINUTES = 30  # don't re-warn every single scan tick while degraded


def _es_direction_for_scan(account_id: int, mode: str, ib, rules: dict) -> dict | None:
    """Fetched ONCE per entry_scan/touch_turn_entry_scan call (never per
    candidate symbol - a live IBKR request per watchlist ticker would be
    wasteful and pointless, since ES's own direction doesn't change
    symbol to symbol). None (and every candidate this scan fails open,
    per es_filter.check's own docstring) unless the strategy actually
    opted in (rules["es_vwap_filter"]) AND the account has explicitly
    enabled the gate (db.is_es_vwap_filter_enabled - off by default,
    since it needs real CME futures market-data entitlement this account
    may not have yet)."""
    if not rules.get("es_vwap_filter") or not db.is_es_vwap_filter_enabled(account_id, mode):
        return None
    direction = es_filter.fetch_live_direction(ib)
    if direction is None:
        _maybe_notify_es_data_unavailable(account_id, mode)
    return direction


def _maybe_notify_es_data_unavailable(account_id: int, mode: str):
    """Rate-limited so a genuinely missing CME entitlement (which fails
    EVERY scan, indefinitely, once the gate is enabled) doesn't flood the
    user's phone - one warning per ES_DATA_UNAVAILABLE_NOTIFY_COOLDOWN_
    MINUTES per account+mode, not one per scan tick."""
    key = f"{account_id}:{mode}:es_data_unavailable_last_notify"
    last = db.get_setting(key, "")
    now = datetime.now(ET)
    if last:
        try:
            if (now - datetime.fromisoformat(last)).total_seconds() < ES_DATA_UNAVAILABLE_NOTIFY_COOLDOWN_MINUTES * 60:
                return
        except ValueError:
            pass
    db.set_setting(key, now.isoformat(timespec="seconds"))
    notify(
        f"[{mode.upper()}] ES VWAP filter degraded",
        "Couldn't read ES futures price/VWAP (no market data, or a Gateway issue) - "
        "gated strategies are failing OPEN (trading unfiltered) until this clears. "
        "Confirm this account has CME futures market-data entitlements if this persists.",
        "high",
    )


# How many CONSECUTIVE real fetch exceptions (network error, HTTP 429,
# timeout - the `except Exception` branch of _evaluate_entry_filters/
# _evaluate_orb_entry/_evaluate_touch_turn_entry, never _evaluate_filters_
# from_bars/orb.evaluate_orb_entry's own "insufficient data" early-return,
# which is a normal per-symbol condition, not a data-source problem)
# across DIFFERENT tickers before this is treated as systemic (Yahoo
# rate-limiting/blocking this server) rather than one bad symbol. Reset to
# 0 by _track_yfinance_fetch_success on every successful fetch, so a
# single flaky ticker mixed in among otherwise-healthy scans never trips
# this - only a genuine unbroken run does. Tightened cycle cadence (see
# run_service.py's CYCLE_INTERVAL_MINUTES) means ~5x more yfinance calls
# per hour than before, which is exactly the scenario this exists to
# catch early.
YFINANCE_FAILURE_STREAK_THRESHOLD = 5
YFINANCE_DEGRADED_NOTIFY_COOLDOWN_MINUTES = 30


def _track_yfinance_fetch_success(account_id: int, mode: str):
    db.set_setting(f"{account_id}:{mode}:yfinance_failure_streak", "0")


def _track_yfinance_fetch_failure(account_id: int, mode: str, ticker: str, exc: Exception):
    key = f"{account_id}:{mode}:yfinance_failure_streak"
    streak = int(db.get_setting(key, "0") or "0") + 1
    db.set_setting(key, str(streak))
    if streak >= YFINANCE_FAILURE_STREAK_THRESHOLD:
        _maybe_notify_yfinance_degraded(account_id, mode, streak, ticker, exc)


def _maybe_notify_yfinance_degraded(account_id: int, mode: str, streak: int, ticker: str, exc: Exception):
    """Rate-limited the same way _maybe_notify_es_data_unavailable is - a
    genuinely rate-limited/blocked yfinance would otherwise fail every
    single scan tick indefinitely, flooding the user's phone."""
    key = f"{account_id}:{mode}:yfinance_degraded_last_notify"
    last = db.get_setting(key, "")
    now = datetime.now(ET)
    if last:
        try:
            if (now - datetime.fromisoformat(last)).total_seconds() < YFINANCE_DEGRADED_NOTIFY_COOLDOWN_MINUTES * 60:
                return
        except ValueError:
            pass
    db.set_setting(key, now.isoformat(timespec="seconds"))
    notify(
        f"[{mode.upper()}] Market data fetch degraded",
        f"{streak} consecutive yfinance failures across different symbols (latest: {ticker} - {exc}) - "
        "likely Yahoo rate-limiting or blocking this server. Entry scanning is effectively blind while "
        "this persists - if it doesn't clear on its own, consider raising run_service.py's own "
        "CYCLE_INTERVAL_MINUTES back up.",
        "high",
    )


def _already_logged_would_enter_today(account_id: int, mode: str, symbol: str, side: str) -> bool:
    """Dry-run mode's own de-dup guard (see entry_scan) - without this, a
    symbol that keeps qualifying would get a fresh 'would_enter' log line
    every single cycle tick all day, instead of once - mirroring how a
    REAL entry only ever happens once per symbol per day (entry_scan's own
    held_symbols check)."""
    today = datetime.now(ET).date().isoformat()
    for row in db.get_decision_log_for_symbol(account_id, mode, symbol):
        if row["event"] == "would_enter" and row["timestamp_iso"].startswith(today) and row["payload"].get("side") == side:
            return True
    return False


def _log_es_rejection(account_id: int, mode: str, strategy_name: str, side: str, ticker: str, gate: dict):
    """Structured decision-log entry for a trade the ES VWAP filter
    blocked - see db.log_decision (auto-stamps its own timestamp_iso, so
    that's not repeated here) and the ES_VWAP_Direction_Filter spec's own
    "TRADE REJECTED" log format, which this carries verbatim as
    individual fields rather than one pre-formatted text blob."""
    log_decision(account_id, mode, {
        "event": "entry_rejected", "reason": "es_vwap_filter", "symbol": ticker,
        "strategy": strategy_name, "direction": side.upper(),
        "es_price": gate.get("es_price"), "es_vwap": gate.get("es_vwap"), "es_detail": gate.get("reason"),
    })


def _strategy_can_enter(strategy_run: dict, open_count: int, open_notional: float, new_notional: float) -> tuple[bool, str | None]:
    """Both live_budget and live_max_positions are optional per strategy_run
    and, when set, BOTH enforced together - whichever limit this candidate
    would hit first blocks it (see strategy_runs' own schema comment,
    src/db.py). Neither set (both None, matching every strategy_run
    migrated from the old single-active-strategy-per-direction model)
    means unlimited, same as before multi-strategy support existed."""
    max_positions = strategy_run.get("live_max_positions")
    if max_positions is not None and open_count >= max_positions:
        return False, "strategy_max_positions_reached"
    budget = strategy_run.get("live_budget")
    if budget is not None and (open_notional + new_notional) > budget:
        return False, "strategy_budget_exceeded"
    return True, None


def entry_scan(account_id: int, mode: str, ib, positions: list[dict], rules: dict, env: dict, side: str,
                strategy_run: dict | None = None) -> list[dict]:
    """Scans this side's ('long' or 'short') watchlist for new entries
    under its own active strategy. positions holds ALL open positions
    (both sides, every strategy) — run_cycle chains one scan per active
    strategy_run over the same growing list, so each sees what every
    other one already opened this cycle; this is what naturally enforces
    "only one strategy can hold a given symbol at a time" (see positions'
    own PRIMARY KEY, unchanged by multi-strategy support - db.upsert_
    position's ON CONFLICT simply overwrites rather than creating a second
    row, so a second strategy's own db.upsert_position call for an already-
    held symbol would silently misattribute it - held_symbols below is
    what actually prevents that from ever being attempted). Concurrent-
    position and daily-entry caps are enforced per side (this side's own
    count against this side's own rules), not pooled across both
    directions.

    strategy_run (None for a legacy/pre-multi-strategy caller) adds a
    SECOND, independent gate on top of the side's own rules-based caps
    above - this strategy's own live_budget/live_max_positions (see
    _strategy_can_enter) - and tags every position this scan opens with
    strategy_id for attribution (budget accounting, routing to the right
    dashboard sheet). A candidate can be skipped for either reason
    (symbol already taken by another strategy vs. this strategy's own
    budget/cap exhausted) - see the strategy_entry_blocked log event for
    which.

    Touch & Turn strategies ("opening_candle" in rules) have their own
    separate scan (touch_turn_entry_scan, called alongside this one from
    run_cycle) - a resting broker-side limit order that can fill anywhere
    from the next tick to 90 minutes later, not an immediate market buy
    the instant a signal passes like every strategy this function DOES
    handle, so it's skipped here entirely rather than falling through to
    the classic D1-D3/I1-I3 evaluator below, which would either error or
    silently misread its unrelated rules.

    SST Swing (rules["strategy_type"] == "sst_swing", per G-SST-5 - an
    explicit marker rather than shape-sniffing like the other families,
    since it shares no rules keys with any of them) is skipped here for
    the same reason, plus one more: it trades off DAILY bars re-evaluated
    once per new close (G-SST-6), not this per-minute intraday scan, so
    it isn't even called from run_cycle's normal loop the way touch_turn_
    entry_scan is - see sst_swing_live.run_daily_scan, its own scheduled
    job."""
    if "opening_candle" in rules or rules.get("strategy_type") == "sst_swing":
        return positions
    if not _within_entry_window(rules):
        return positions

    risk = mode_config.risk_params(env, account_id, mode)
    if db.count_todays_entries(account_id, mode, side) >= risk["max_trades_per_day"]:
        return positions

    max_concurrent = rules["risk"]["max_concurrent_positions"]
    side_positions = [p for p in positions if p.get("side", "long") == side]
    if len(side_positions) >= max_concurrent:
        return positions

    strategy_id = strategy_run["strategy_id"] if strategy_run else None
    strategy_positions = [p for p in positions if p.get("strategy_id") == strategy_id] if strategy_run else []
    strategy_open_notional = sum(p["qty"] * p["entry_price"] for p in strategy_positions)
    if strategy_run:
        can_enter, reason = _strategy_can_enter(strategy_run, len(strategy_positions), strategy_open_notional, 0.0)
        if not can_enter:
            log_decision(account_id, mode, {"event": "strategy_entry_blocked", "side": side, "strategy_id": strategy_id, "reason": reason})
            return positions

    # A fade strategy's own filters (D1-D3/I1-I3, or ORB's confirm/gap/
    # retest conditions once signal_side reaches evaluate_orb_entry too)
    # are keyed to signal_side's gap direction, not the actual trade side
    # - a "Long Breakout Fade (Short)" needs GAP-UP candidates (direction
    # ="long" in the watchlist, tagged by morning_prefilter purely off
    # each symbol's own gap sign, never a strategy's trade direction) even
    # though it trades short. Querying by `side` here would silently hand
    # this scan zero usable candidates (gap-down symbols can never pass a
    # "gapped up 3%" filter), making the strategy inert without ever
    # erroring - rules.get("signal_side") is None (falls back to `side`,
    # unchanged behavior) for every non-fade strategy.
    watchlist_direction = rules.get("signal_side") or side
    watchlist = [row["symbol"] for row in db.get_watchlist(account_id, mode, direction=watchlist_direction, universe=_strategy_universe(rules))]
    if not watchlist:
        return positions

    es_direction = _es_direction_for_scan(account_id, mode, ib, rules)

    # != 0 (not just > 0) so an existing short in the real account also
    # blocks a duplicate/conflicting entry, not just existing longs.
    held_symbols = {p.contract.symbol for p in scoped_positions(ib) if p.position != 0}
    held_symbols |= {p["symbol"] for p in positions}

    portfolio_value = risk["portfolio_value"]
    max_risk_pct = risk["max_risk_pct"]
    max_position_pct = rules["risk"]["max_position_size_pct_of_portfolio"] / 100
    action = "BUY" if side == "long" else "SELL"

    for ticker in watchlist:
        if len(side_positions) >= max_concurrent:
            break
        if strategy_run and strategy_run.get("live_max_positions") is not None and len(strategy_positions) >= strategy_run["live_max_positions"]:
            break
        if ticker in held_symbols:
            continue
        if db.count_todays_entries(account_id, mode, side) >= risk["max_trades_per_day"]:
            break

        is_orb = "opening_range" in rules
        signal = _evaluate_orb_entry(account_id, mode, ticker, rules, side) if is_orb \
            else _evaluate_entry_filters(account_id, mode, ticker, rules, side)
        if not signal.get("pass"):
            continue

        price = signal["price"]
        # ORB's stop comes straight off the entry model itself (the gap
        # candle's or retest bar's own low/high) - not one of
        # INITIAL_STOP_RULES' generic session-extreme/ATR-multiple rules,
        # see orb.evaluate_orb_entry's own docstring.
        initial_stop = signal["initial_stop"] if is_orb else _resolve_initial_stop(signal, rules, side)
        r = (initial_stop - price) if side == "short" else (price - initial_stop)
        if r <= 0:
            continue

        risk_dollars = portfolio_value * (max_risk_pct / 100)
        size_by_risk = math.floor(risk_dollars / r)
        size_by_cap = math.floor(portfolio_value * max_position_pct / price)
        size = min(size_by_risk, size_by_cap)
        if size < 1:
            continue

        if rules.get("es_vwap_filter") and db.is_es_vwap_filter_enabled(account_id, mode):
            gate = es_filter.check(es_direction, side)
            if not gate["allowed"]:
                _log_es_rejection(account_id, mode, rules.get("strategy_name", "?"), side, ticker, gate)
                continue

        if strategy_run:
            can_enter, reason = _strategy_can_enter(strategy_run, len(strategy_positions), strategy_open_notional, size * price)
            if not can_enter:
                log_decision(account_id, mode, {"event": "strategy_entry_blocked", "symbol": ticker, "side": side, "strategy_id": strategy_id, "reason": reason})
                continue

        # Dry run: everything up to here ran exactly as it would for a real
        # entry (same filters, same ES gate, same computed stop/size) - this
        # is the one point that decides whether trade.py actually gets
        # called. Skipping straight to the next candidate (no
        # held_symbols/day-cap/side_positions bookkeeping) is deliberate -
        # nothing was actually risked, so nothing should be capped the way a
        # real entry would be; the goal is seeing every qualifying signal,
        # not simulating position-count limits.
        if db.is_dry_run(account_id, mode, side):
            if not _already_logged_would_enter_today(account_id, mode, ticker, side):
                notify(
                    f"[{mode.upper()}] DRY RUN {action} {ticker}",
                    f"@ ${price:.2f}, stop ${initial_stop:.2f}, qty {size} - would have entered, no order placed (dry-run mode)",
                    "default",
                )
                log_decision(account_id, mode, {"event": "would_enter", "symbol": ticker, "side": side, "price": price, "stop": initial_stop, "qty": size})
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

        # "no_stop_delayed_trail" strategies (v4.1/v4.2/v4.3/V8/V9/V10) that
        # opt into exit_cfg["hard_stop_R"] place their REAL resting stop at
        # that wider level, not at the tight ORB initial_stop - see
        # manage_position's own "no_stop_delayed_trail" branch docstring
        # for why (it's the strategy's own deliberately wider, more patient
        # stop, not a bug). v4.1 itself (no hard_stop_R at all) keeps the
        # tight initial_stop as its real resting stop - a deliberate, more
        # conservative departure from backtest_engine.py's own "never stops
        # for adverse movement" semantics for that specific case, since
        # resting NO protective stop at all is a materially different (and
        # unacceptable) live risk posture. initial_risk here is realized
        # off the ACTUAL fill_price, matching manage_position's own
        # r_multiple math - not backtest_engine.py's pre-fill signal price.
        order_stop_price = initial_stop
        extra_position_fields = {"mae_price": fill_price}
        if rules["exit"].get("management_style") == "no_stop_delayed_trail":
            extra_position_fields.update({"mfe_price": fill_price, "trail_activated": False, "trail_activated_at_r": None})
            hard_stop_r = rules["exit"].get("hard_stop_R")
            if hard_stop_r is not None:
                initial_risk = abs(fill_price - initial_stop)
                order_stop_price = (fill_price + hard_stop_r * initial_risk) if side == "short" else (fill_price - hard_stop_r * initial_risk)
                extra_position_fields["hard_stop_price"] = order_stop_price

        stop_order_id = _place_stop(ib, ticker, fill_qty, order_stop_price, side)
        new_position = {
            "symbol": ticker,
            "side": side,
            "entry_price": fill_price,
            "entry_time_iso": datetime.now(ET).isoformat(timespec="seconds"),
            "qty": fill_qty,
            "initial_stop": initial_stop,
            "stop_price": order_stop_price,
            "stop_order_id": stop_order_id,
            "state": "pre_breakeven",
            "r_multiple": 0.0,
            "strategy_id": strategy_id,
            **extra_position_fields,
        }
        if is_orb:
            new_position["target_price"] = signal["target_price"]
        db.upsert_position(account_id, mode, new_position)
        positions.append(new_position)
        side_positions.append(new_position)
        if strategy_run:
            strategy_positions.append(new_position)
            strategy_open_notional += fill_qty * fill_price
        held_symbols.add(ticker)
        notify(f"[{mode.upper()}] {action} {ticker}", f"@ ${fill_price:.2f}, stop ${initial_stop:.2f}, qty {fill_qty}", "default")
        log_decision(account_id, mode, {"event": "entry", "symbol": ticker, "side": side, "price": fill_price, "stop": initial_stop, "qty": fill_qty, "strategy_id": strategy_id})

    return positions


def virtual_entry_scan(account_id: int, mode: str, ib, rules: dict, env: dict, side: str, strategy_run: dict) -> None:
    """entry_scan's counterpart for a strategy_run with run_mode='virtual'
    (see strategy_run's own schema comment, src/db.py) - same filter
    evaluation, same sizing math, but on a pass this writes a simulated
    fill straight to virtual_positions instead of ever calling trade.py or
    touching a real broker order. ib is still needed (unlike everywhere
    else in the virtual path) purely for _es_direction_for_scan's own
    read-only ES futures market-data fetch, if this strategy opted into
    that filter - reading market data isn't a real order, so reusing the
    cycle's own already-open connection for it doesn't compromise "virtual
    needs no dedicated broker connection".

    Unlike entry_scan, this doesn't accept/mutate a shared cross-strategy
    positions list - each virtual strategy's own open positions
    (db.get_virtual_positions(account_id, strategy_id=...)) are what
    "already held" means here, entirely independent of every other
    strategy (real or virtual) that might also be holding the same
    symbol right now (see virtual_positions' own schema comment for why
    that's fine - no real shares are ever at stake). live_budget/
    live_max_positions don't apply to a virtual run - virtual_capital is
    its own notional cap instead, and position count is still bounded by
    the strategy's own rules["risk"]["max_concurrent_positions"], same
    field real trading already uses.

    Deliberately does NOT check db.count_todays_entries/max_trades_per_day
    (that table only ever records real fills) or db.is_dry_run (a
    different, orthogonal feature for a live-scoped side, not a
    strategy_run) - a virtual strategy's only throttle is its own
    concurrent-position cap and virtual_capital, same as a real account's
    own budget/position cap are its only throttle beyond the strategy's
    own rules.

    SST Swing (rules["strategy_type"] == "sst_swing") is skipped here too
    - see entry_scan's own docstring for why; its virtual-mode evaluation
    runs from sst_swing_live.run_daily_scan alongside the real path, not
    from this per-minute scan."""
    if "opening_candle" in rules or rules.get("strategy_type") == "sst_swing":
        return
    if not _within_entry_window(rules):
        return

    strategy_id, strategy_label = strategy_run["strategy_id"], strategy_run.get("strategy_name", "?")
    open_positions = db.get_virtual_positions(account_id, strategy_id=strategy_id)
    max_concurrent = rules["risk"]["max_concurrent_positions"]
    if len(open_positions) >= max_concurrent:
        return

    watchlist_direction = rules.get("signal_side") or side
    watchlist = [row["symbol"] for row in db.get_watchlist(account_id, mode, direction=watchlist_direction, universe=_strategy_universe(rules))]
    if not watchlist:
        return

    es_direction = _es_direction_for_scan(account_id, mode, ib, rules)
    held_symbols = {p["symbol"] for p in open_positions}
    open_notional = sum(p["qty"] * p["entry_price"] for p in open_positions)
    virtual_capital = strategy_run.get("virtual_capital")

    risk = mode_config.risk_params(env, account_id, mode)
    portfolio_value = risk["portfolio_value"]
    max_risk_pct = risk["max_risk_pct"]
    max_position_pct = rules["risk"]["max_position_size_pct_of_portfolio"] / 100
    action = "BUY" if side == "long" else "SELL"

    for ticker in watchlist:
        if len(open_positions) >= max_concurrent:
            break
        if ticker in held_symbols:
            continue

        is_orb = "opening_range" in rules
        signal = _evaluate_orb_entry(account_id, mode, ticker, rules, side) if is_orb \
            else _evaluate_entry_filters(account_id, mode, ticker, rules, side)
        if not signal.get("pass"):
            continue

        price = signal["price"]
        initial_stop = signal["initial_stop"] if is_orb else _resolve_initial_stop(signal, rules, side)
        r = (initial_stop - price) if side == "short" else (price - initial_stop)
        if r <= 0:
            continue

        risk_dollars = portfolio_value * (max_risk_pct / 100)
        size_by_risk = math.floor(risk_dollars / r)
        size_by_cap = math.floor(portfolio_value * max_position_pct / price)
        size = min(size_by_risk, size_by_cap)
        if size < 1:
            continue

        if rules.get("es_vwap_filter") and db.is_es_vwap_filter_enabled(account_id, mode):
            gate = es_filter.check(es_direction, side)
            if not gate["allowed"]:
                _log_es_rejection(account_id, mode, rules.get("strategy_name", "?"), side, ticker, gate)
                continue

        if virtual_capital is not None and (open_notional + size * price) > virtual_capital:
            log_decision(account_id, mode, {"event": "strategy_entry_blocked", "symbol": ticker, "side": side, "strategy_id": strategy_id, "reason": "virtual_capital_exceeded", "virtual": True})
            continue

        # No fill delay/slippage to simulate - this is the instant the
        # signal passed, at the signal's own price, exactly as if it had
        # filled immediately (the same assumption backtest_engine.py's own
        # simulator makes for a market-order entry).
        order_stop_price = initial_stop
        extra_position_fields = {"mae_price": price}
        if rules["exit"].get("management_style") == "no_stop_delayed_trail":
            extra_position_fields.update({"mfe_price": price, "trail_activated": False, "trail_activated_at_r": None})
            hard_stop_r = rules["exit"].get("hard_stop_R")
            if hard_stop_r is not None:
                initial_risk = abs(price - initial_stop)
                order_stop_price = (price + hard_stop_r * initial_risk) if side == "short" else (price - hard_stop_r * initial_risk)
                extra_position_fields["hard_stop_price"] = order_stop_price

        new_position = {
            "symbol": ticker,
            "side": side,
            "entry_price": price,
            "entry_time_iso": datetime.now(ET).isoformat(timespec="seconds"),
            "qty": size,
            "initial_stop": initial_stop,
            "stop_price": order_stop_price,
            "state": "pre_breakeven",
            "r_multiple": 0.0,
            **extra_position_fields,
        }
        if is_orb:
            new_position["target_price"] = signal["target_price"]
        db.upsert_virtual_position(account_id, strategy_id, new_position)
        open_positions.append(new_position)
        held_symbols.add(ticker)
        open_notional += size * price
        notify(f"[VIRTUAL] {strategy_label}: {action} {ticker}", f"@ ${price:.2f}, stop ${initial_stop:.2f}, qty {size}", "default")
        log_decision(account_id, mode, {"event": "entry", "symbol": ticker, "side": side, "price": price, "stop": initial_stop, "qty": size, "strategy_id": strategy_id, "virtual": True})


# --------------------------------------------------------- SST Swing ---
# Called only from sst_swing_live.py's own once-daily scheduled job
# (G-SST-6), never from run_cycle's per-minute loop - entry_scan/
# virtual_entry_scan both explicitly skip rules["strategy_type"] ==
# "sst_swing" (see their own docstrings) so this family is never
# double-evaluated. Reuses the same real order-placement primitives as
# entry_scan (trade.py subprocess, _place_stop, _broker_position delayed-
# fill recovery, _RealPositionOps/_VirtualPositionOps for the daily
# trailing-stop update below) rather than a second, parallel execution
# path, per the source prompt's own explicit instruction. Stop-OUT
# detection itself needs nothing new here: check_stop_outs (real) and
# check_virtual_stop_outs (virtual) already run every cycle for every
# open position regardless of strategy, generically comparing price
# against pos["stop_price"] - SST positions are covered automatically,
# see sst_manage_positions/sst_manage_virtual_positions below for the
# ONE thing that's actually SST-specific: moving that stop per Rule 6.
def sst_entry_scan(account_id: int, mode: str, ib, positions: list[dict], rules: dict, side: str,
                    strategy_run: dict) -> list[dict]:
    """SST Swing's real-money entry evaluation for one side of one
    strategy_run - entry_scan's counterpart, on DAILY bars against
    db.get_sst_watchlist(status="pass") ('review' rows need a human look
    first, per G-SST-4) instead of entry_scan's intraday watchlist/
    filters. Every candidate's signal is logged via log_decision
    regardless of pass/fail (G-SST-7 - "log every signal, even ones not
    taken"), unlike entry_scan which only logs on a pass."""
    strategy_id = strategy_run["strategy_id"]
    max_concurrent = rules["risk"]["max_concurrent_positions"]
    side_positions = [p for p in positions if p.get("side", "long") == side and p.get("strategy_id") == strategy_id]
    if len(side_positions) >= max_concurrent:
        return positions

    strategy_positions = [p for p in positions if p.get("strategy_id") == strategy_id]
    strategy_open_notional = sum(p["qty"] * p["entry_price"] for p in strategy_positions)
    can_enter, reason = _strategy_can_enter(strategy_run, len(strategy_positions), strategy_open_notional, 0.0)
    if not can_enter:
        log_decision(account_id, mode, {"event": "strategy_entry_blocked", "side": side, "strategy_id": strategy_id, "reason": reason})
        return positions

    held_symbols = {p.contract.symbol for p in scoped_positions(ib) if p.position != 0}
    held_symbols |= {p["symbol"] for p in positions}

    risk = mode_config.risk_params(_env(), account_id, mode)
    portfolio_value = risk["portfolio_value"]
    action = "BUY" if side == "long" else "SELL"

    for row in db.get_sst_watchlist(status="pass"):
        if len(side_positions) >= max_concurrent:
            break
        symbol = row["symbol"]
        if symbol in held_symbols:
            continue

        daily = _fetch_sst_daily_bars(symbol)
        if daily is None:
            continue
        signal = sst_swing.evaluate_sst_entry(daily, rules, side)
        log_decision(account_id, mode, {"event": "sst_signal", "symbol": symbol, "side": side, "strategy_id": strategy_id, **signal})
        if not signal.get("pass"):
            continue

        price, initial_stop = signal["entry_price"], signal["initial_stop"]
        size, skip_reason = sst_swing.size_for_risk(portfolio_value, price, initial_stop, rules)
        if size <= 0:
            log_decision(account_id, mode, {"event": "sst_entry_skipped", "symbol": symbol, "side": side, "strategy_id": strategy_id, "reason": skip_reason})
            continue

        can_enter, reason = _strategy_can_enter(strategy_run, len(strategy_positions), strategy_open_notional, size * price)
        if not can_enter:
            log_decision(account_id, mode, {"event": "strategy_entry_blocked", "symbol": symbol, "side": side, "strategy_id": strategy_id, "reason": reason})
            continue

        proc = subprocess.run(
            [sys.executable, str(PROJECT_DIR / "trade.py"), "--mode", mode, "--account-id", str(account_id),
             "--symbol", symbol, "--side", action, "--size", str(size)],
            capture_output=True, text=True, timeout=SUBPROCESS_TIMEOUT,
        )
        log_decision(account_id, mode, {"event": "sst_entry_attempt", "symbol": symbol, "side": side, "qty": size, "price": price, "stdout": proc.stdout})
        if proc.returncode != 0:
            # Same delayed-fill race as entry_scan - trade.py may have
            # given up waiting before the fill actually confirmed.
            broker_pos = _broker_position(ib, symbol)
            if broker_pos is None:
                continue
            fill_qty, fill_price = abs(broker_pos["qty"]), broker_pos["avg_cost"]
            log_decision(account_id, mode, {"event": "delayed_fill_recovered", "symbol": symbol, "side": side, "qty": fill_qty, "price": fill_price})
        else:
            fill_qty, fill_price = size, price

        stop_order_id = _place_stop(ib, symbol, fill_qty, initial_stop, side)
        new_position = {
            "symbol": symbol, "side": side, "entry_price": fill_price,
            "entry_time_iso": datetime.now(ET).isoformat(timespec="seconds"),
            "qty": fill_qty, "initial_stop": initial_stop, "stop_price": initial_stop,
            "stop_order_id": stop_order_id, "state": "sst_swing", "r_multiple": 0.0,
            "strategy_id": strategy_id, "mae_price": fill_price,
        }
        db.upsert_position(account_id, mode, new_position)
        positions.append(new_position)
        side_positions.append(new_position)
        strategy_positions.append(new_position)
        strategy_open_notional += fill_qty * fill_price
        held_symbols.add(symbol)
        notify(f"[{mode.upper()}] SST ENTRY {symbol}", f"{action} {fill_qty} @ ${fill_price:.2f}, stop ${initial_stop:.2f}", "default")
        log_decision(account_id, mode, {"event": "sst_entry", "symbol": symbol, "side": side, "price": fill_price, "stop": initial_stop, "qty": fill_qty, "strategy_id": strategy_id})

    return positions


def virtual_sst_entry_scan(account_id: int, mode: str, rules: dict, side: str, strategy_run: dict) -> None:
    """sst_entry_scan's virtual-mode counterpart - same signal evaluation
    and sizing, but writes a simulated fill straight to virtual_positions
    instead of ever calling trade.py. No `ib` parameter needed at all
    (unlike virtual_entry_scan, which still takes one for its optional ES
    futures filter) - SST Swing has no such filter."""
    strategy_id, strategy_label = strategy_run["strategy_id"], strategy_run.get("strategy_name", "?")
    open_positions = db.get_virtual_positions(account_id, strategy_id=strategy_id)
    max_concurrent = rules["risk"]["max_concurrent_positions"]
    if len(open_positions) >= max_concurrent:
        return

    held_symbols = {p["symbol"] for p in open_positions}
    open_notional = sum(p["qty"] * p["entry_price"] for p in open_positions)
    virtual_capital = strategy_run.get("virtual_capital")

    risk = mode_config.risk_params(_env(), account_id, mode)
    portfolio_value = risk["portfolio_value"]
    action = "BUY" if side == "long" else "SELL"

    for row in db.get_sst_watchlist(status="pass"):
        if len(open_positions) >= max_concurrent:
            break
        symbol = row["symbol"]
        if symbol in held_symbols:
            continue

        daily = _fetch_sst_daily_bars(symbol)
        if daily is None:
            continue
        signal = sst_swing.evaluate_sst_entry(daily, rules, side)
        log_decision(account_id, mode, {"event": "sst_signal", "symbol": symbol, "side": side, "strategy_id": strategy_id, "virtual": True, **signal})
        if not signal.get("pass"):
            continue

        price, initial_stop = signal["entry_price"], signal["initial_stop"]
        size, skip_reason = sst_swing.size_for_risk(portfolio_value, price, initial_stop, rules)
        if size <= 0:
            log_decision(account_id, mode, {"event": "sst_entry_skipped", "symbol": symbol, "side": side, "strategy_id": strategy_id, "virtual": True, "reason": skip_reason})
            continue

        if virtual_capital is not None and (open_notional + size * price) > virtual_capital:
            log_decision(account_id, mode, {"event": "strategy_entry_blocked", "symbol": symbol, "side": side, "strategy_id": strategy_id, "reason": "virtual_capital_exceeded", "virtual": True})
            continue

        new_position = {
            "symbol": symbol, "side": side, "entry_price": price,
            "entry_time_iso": datetime.now(ET).isoformat(timespec="seconds"),
            "qty": size, "initial_stop": initial_stop, "stop_price": initial_stop,
            "state": "sst_swing", "r_multiple": 0.0, "mae_price": price,
        }
        db.upsert_virtual_position(account_id, strategy_id, new_position)
        open_positions.append(new_position)
        held_symbols.add(symbol)
        open_notional += size * price
        notify(f"[VIRTUAL] {strategy_label}: SST ENTRY {symbol}", f"{action} {size} @ ${price:.2f}, stop ${initial_stop:.2f}", "default")
        log_decision(account_id, mode, {"event": "sst_entry", "symbol": symbol, "side": side, "price": price, "stop": initial_stop, "qty": size, "strategy_id": strategy_id, "virtual": True})


def sst_manage_positions(account_id: int, mode: str, ib, positions: list[dict]) -> None:
    """Rule 6's trailing-stop update for every open REAL SST Swing
    position - the only thing that's actually SST-specific about daily
    position management (stop-OUT detection is already fully generic,
    see this section's own header comment). Filters positions itself
    (rather than requiring the caller to pre-filter) so sst_swing_live.py
    can just pass every open position for the account, same as
    check_stop_outs/manage_position already do each cycle."""
    for pos in positions:
        rules = _rules_for_position(pos, {})
        if rules.get("strategy_type") != "sst_swing":
            continue
        daily = _fetch_sst_daily_bars(pos["symbol"])
        if daily is None:
            continue
        side = pos.get("side", "long")
        new_stop = sst_swing.trailing_stop_update(daily, side, pos["stop_price"], rules)
        if new_stop is None:
            continue
        ops = _RealPositionOps(account_id, mode, ib)
        old_stop = pos["stop_price"]
        pos["stop_order_id"] = ops.reposition_stop(pos, new_stop, side)
        pos["stop_price"] = new_stop
        ops.save(pos)
        ops.notify(f"SST TRAIL {pos['symbol']}", f"stop ${old_stop:.2f} -> ${new_stop:.2f}", "default")
        ops.log("sst_trail_stop", symbol=pos["symbol"], side=side, old=old_stop, new=new_stop)


def sst_manage_virtual_positions(account_id: int, strategy_run: dict) -> None:
    """sst_manage_positions' virtual-mode counterpart - Rule 6's trailing
    update against this strategy_run's own virtual_positions."""
    strategy_id = strategy_run["strategy_id"]
    strategy_label = strategy_run.get("strategy_name", "?")
    strategy = db.get_strategy(strategy_id)
    if strategy is None:
        return
    rules = json.loads(strategy["rules_json"])
    if rules.get("strategy_type") != "sst_swing":
        return

    for pos in db.get_virtual_positions(account_id, strategy_id=strategy_id):
        daily = _fetch_sst_daily_bars(pos["symbol"])
        if daily is None:
            continue
        side = pos.get("side", "long")
        new_stop = sst_swing.trailing_stop_update(daily, side, pos["stop_price"], rules)
        if new_stop is None:
            continue
        ops = _VirtualPositionOps(account_id, strategy_id, strategy_label)
        old_stop = pos["stop_price"]
        pos["stop_price"] = new_stop
        ops.save(pos)
        ops.notify(f"SST TRAIL {pos['symbol']}", f"stop ${old_stop:.2f} -> ${new_stop:.2f}", "default")
        ops.log("sst_trail_stop", symbol=pos["symbol"], side=side, old=old_stop, new=new_stop)


def touch_turn_entry_scan(account_id: int, mode: str, ib, rules: dict, env: dict, side: str,
                           strategy_run: dict | None = None):
    """Touch & Turn's own entry scan - the counterpart to entry_scan
    above, called alongside it from run_cycle for a strategy whose rules
    carry an "opening_candle" key (see entry_scan's own docstring for why
    that function skips these entirely). Unlike entry_scan, this doesn't
    buy anything or return/mutate `positions` - it only ever PLACES a
    resting limit order (see _place_touch_turn_limit); a fill is only
    confirmed later, by check_pending_touch_turn_orders, which is what
    actually creates the tracked position. db.has_pending_order_today
    (checked via db.create_pending_order's own atomic INSERT OR IGNORE)
    makes repeat calls across a single day's many 5-minute cycle ticks
    harmless no-ops once a symbol's already had an attempt today,
    regardless of that attempt's eventual outcome - this function has no
    memory of its own between calls.

    strategy_run (None for a legacy/pre-multi-strategy caller) works
    exactly like entry_scan's own - see that function's docstring -
    tagging the resting order (and later, once it fills, the position)
    with strategy_id and enforcing this strategy's own live_budget/
    live_max_positions on top of the side's rules-based caps below."""
    if "opening_candle" not in rules:
        return
    if not db.is_bot_enabled(account_id, mode):
        return

    risk = mode_config.risk_params(env, account_id, mode)
    if db.count_todays_entries(account_id, mode, side) >= risk["max_trades_per_day"]:
        return

    now_et = datetime.now(ET)
    session_open_et = datetime.combine(now_et.date(), dt_time(9, 30), tzinfo=ET)
    expiry_et = session_open_et + timedelta(minutes=rules["time_filter"]["entry_window_minutes"])
    if now_et >= expiry_et:
        return  # today's entry window has already closed - nothing new to attempt

    watchlist = [row["symbol"] for row in db.get_watchlist(account_id, mode, direction=side, universe=_strategy_universe(rules))]
    if not watchlist:
        return

    es_direction = _es_direction_for_scan(account_id, mode, ib, rules)

    held_symbols = {p.contract.symbol for p in scoped_positions(ib) if p.position != 0}
    held_symbols |= {p["symbol"] for p in db.get_open_positions(account_id, mode)}
    placed_date = now_et.date().isoformat()

    portfolio_value = risk["portfolio_value"]
    max_risk_pct = risk["max_risk_pct"]
    max_position_pct = rules["risk"]["max_position_size_pct_of_portfolio"] / 100
    max_concurrent = rules["risk"]["max_concurrent_positions"]
    # A resting order isn't a position yet, but it WILL become one the
    # moment it fills - counting it against max_concurrent_positions
    # alongside already-filled ones now (not just at fill time) keeps
    # total exposure bounded even if several symbols' orders all happen
    # to fill close together, which entry_scan's own held_symbols/
    # side_positions check achieves the same way for its immediate-fill
    # market orders.
    side_open_count = sum(1 for p in db.get_open_positions(account_id, mode) if p.get("side", "long") == side)
    side_pending_count = sum(1 for po in db.get_pending_orders(account_id, mode, "pending") if po["side"] == side)

    # Same strategy-scoped budget/cap gate as entry_scan - see its own
    # docstring. A resting order counts against the budget/cap alongside
    # already-filled positions now (not just at fill time), same reasoning
    # as max_concurrent_positions/side_open_count+side_pending_count above.
    strategy_id = strategy_run["strategy_id"] if strategy_run else None
    strategy_positions = [p for p in db.get_open_positions(account_id, mode) if p.get("strategy_id") == strategy_id] if strategy_run else []
    strategy_pending = [po for po in db.get_pending_orders(account_id, mode, "pending") if po.get("strategy_id") == strategy_id] if strategy_run else []
    strategy_open_count = len(strategy_positions) + len(strategy_pending)
    strategy_open_notional = (
        sum(p["qty"] * p["entry_price"] for p in strategy_positions)
        + sum(po["qty"] * po["limit_price"] for po in strategy_pending)
    )
    if strategy_run:
        can_enter, reason = _strategy_can_enter(strategy_run, strategy_open_count, strategy_open_notional, 0.0)
        if not can_enter:
            log_decision(account_id, mode, {"event": "strategy_entry_blocked", "side": side, "strategy_id": strategy_id, "reason": reason})
            return

    for ticker in watchlist:
        if side_open_count + side_pending_count >= max_concurrent:
            break
        if strategy_run and strategy_run.get("live_max_positions") is not None and strategy_open_count >= strategy_run["live_max_positions"]:
            break
        if ticker in held_symbols:
            continue
        if db.has_pending_order_today(account_id, mode, ticker, placed_date):
            continue

        signal = _evaluate_touch_turn_entry(account_id, mode, ticker, rules, side)
        if not signal.get("pass"):
            continue

        limit_price, initial_stop = signal["limit_price"], signal["initial_stop"]
        r = abs(limit_price - initial_stop)
        if r <= 0:
            continue

        risk_dollars = portfolio_value * (max_risk_pct / 100)
        size_by_risk = math.floor(risk_dollars / r)
        size_by_cap = math.floor(portfolio_value * max_position_pct / limit_price)
        size = min(size_by_risk, size_by_cap)
        if size < 1:
            continue

        if rules.get("es_vwap_filter") and db.is_es_vwap_filter_enabled(account_id, mode):
            gate = es_filter.check(es_direction, side)
            if not gate["allowed"]:
                _log_es_rejection(account_id, mode, rules.get("strategy_name", "?"), side, ticker, gate)
                continue

        if strategy_run:
            can_enter, reason = _strategy_can_enter(strategy_run, strategy_open_count, strategy_open_notional, size * limit_price)
            if not can_enter:
                log_decision(account_id, mode, {"event": "strategy_entry_blocked", "symbol": ticker, "side": side, "strategy_id": strategy_id, "reason": reason})
                continue

        # Dry run - see entry_scan's own comment on the same check. Touch &
        # Turn's real order is the resting limit placed below; skip that
        # (and its DB bookkeeping) the same way.
        if db.is_dry_run(account_id, mode, side):
            if not _already_logged_would_enter_today(account_id, mode, ticker, side):
                notify(
                    f"[{mode.upper()}] DRY RUN Touch&Turn {ticker}",
                    f"{side} limit @ ${limit_price:.2f}, stop ${initial_stop:.2f}, qty {size} - would have placed a resting order, none placed (dry-run mode)",
                    "default",
                )
                log_decision(account_id, mode, {"event": "would_enter", "symbol": ticker, "side": side, "price": limit_price, "stop": initial_stop, "qty": size})
            continue

        inserted = db.create_pending_order(account_id, mode, {
            "symbol": ticker, "placed_date": placed_date, "side": side,
            "limit_price": limit_price, "target_price": signal["target_price"], "initial_stop": initial_stop,
            "qty": size, "placed_at": now_et.isoformat(timespec="seconds"), "expires_at": expiry_et.isoformat(timespec="seconds"),
            "strategy_id": strategy_id,
        })
        if not inserted:
            continue  # another tick already claimed this symbol/day between the check above and here
        side_pending_count += 1
        if strategy_run:
            strategy_open_count += 1
            strategy_open_notional += size * limit_price

        try:
            order_id = _place_touch_turn_limit(ib, ticker, size, limit_price, side, expiry_et)
            db.set_pending_order_broker_id(account_id, mode, ticker, placed_date, order_id)
            notify(
                f"[{mode.upper()}] Touch&Turn order placed: {ticker}",
                f"{side} limit @ ${limit_price:.2f}, target ${signal['target_price']:.2f}, stop ${initial_stop:.2f}, "
                f"expires {expiry_et.strftime('%H:%M')} ET", "default",
            )
            log_decision(account_id, mode, {
                "event": "touch_turn_order_placed", "symbol": ticker, "side": side,
                "limit_price": limit_price, "target_price": signal["target_price"], "initial_stop": initial_stop, "qty": size,
            })
        except Exception as exc:  # noqa: BLE001 - one bad ticker must not kill the scan
            db.resolve_pending_order(account_id, mode, ticker, placed_date, "cancelled")
            log_decision(account_id, mode, {"event": "touch_turn_order_error", "symbol": ticker, "side": side, "error": str(exc)})


def check_pending_touch_turn_orders(account_id: int, mode: str, ib, positions: list[dict]) -> list[dict]:
    """Runs every cycle tick regardless of the bot's enabled flag (same
    reasoning as check_stop_outs/manage_position - see run_cycle's Step
    3/4) - a resting limit order can fill, or need cancelling, whether or
    not new entries are currently paused.

    Fill detection reads _broker_position, not an order-id/fill-price
    lookup - Touch & Turn only ever has at most one attempt per symbol
    per day (see touch_turn_entry_scan), so a newly-nonzero broker
    position in that exact symbol unambiguously means THIS order filled.
    On a fill, promotes it into a normal tracked position exactly like
    entry_scan's own tail does (broker-side protective stop via
    _place_stop, then db.upsert_position) - management_style:
    "fixed_target_no_trail" (see the Touch & Turn presets) then handles
    it identically to an ORB position from here on, via orb.
    fixed_target_decision, which is fully generic (just target_price/
    price/side) despite living in orb.py.

    Cancelling (via _cancel_stop, which despite its name is generic - find
    an order by id, cancel it) and marking 'expired' anything past its own
    expires_at that hasn't filled is mostly a backstop for IBKR's own GTD
    auto-cancel (see _place_touch_turn_limit) - not the primary mechanism,
    but this bot shouldn't just trust a single point of failure for
    something that leaves a resting order in the market."""
    now_et = datetime.now(ET)
    for po in db.get_pending_orders(account_id, mode, "pending"):
        symbol, side = po["symbol"], po["side"]
        broker_pos = _broker_position(ib, symbol)
        if broker_pos is not None:
            fill_qty, fill_price = abs(broker_pos["qty"]), broker_pos["avg_cost"]
            stop_order_id = _place_stop(ib, symbol, fill_qty, po["initial_stop"], side)
            new_position = {
                "symbol": symbol, "side": side, "entry_price": fill_price,
                "entry_time_iso": now_et.isoformat(timespec="seconds"), "qty": fill_qty,
                "initial_stop": po["initial_stop"], "stop_price": po["initial_stop"],
                "stop_order_id": stop_order_id, "state": "pre_breakeven", "r_multiple": 0.0,
                "target_price": po["target_price"], "strategy_id": po.get("strategy_id"),
            }
            db.upsert_position(account_id, mode, new_position)
            positions.append(new_position)
            db.resolve_pending_order(account_id, mode, symbol, po["placed_date"], "filled")
            notify(f"[{mode.upper()}] Touch&Turn FILLED: {symbol}", f"@ ${fill_price:.2f}, target ${po['target_price']:.2f}, stop ${po['initial_stop']:.2f}", "default")
            log_decision(account_id, mode, {"event": "touch_turn_fill", "symbol": symbol, "side": side, "price": fill_price, "qty": fill_qty, "strategy_id": po.get("strategy_id")})
            continue

        expires_at = datetime.fromisoformat(po["expires_at"])
        if now_et >= expires_at:
            _cancel_pending_touch_turn_order(account_id, mode, ib, po, "expired")

    return positions


def _cancel_pending_touch_turn_order(account_id: int, mode: str, ib, po: dict, status: str):
    """Shared by check_pending_touch_turn_orders' own expiry check and
    run_cycle's "flatten everything now" path - _cancel_stop despite its
    name is generic (find a broker order by id, cancel it), so it works
    equally well on a resting entry limit order."""
    if po.get("broker_order_id"):
        _cancel_stop(ib, po["broker_order_id"])
    db.resolve_pending_order(account_id, mode, po["symbol"], po["placed_date"], status)
    log_decision(account_id, mode, {"event": f"touch_turn_{status}", "symbol": po["symbol"], "side": po["side"]})


def virtual_touch_turn_entry_scan(account_id: int, mode: str, ib, rules: dict, env: dict, side: str, strategy_run: dict) -> None:
    """touch_turn_entry_scan's counterpart for a strategy_run with
    run_mode='virtual' - same signal evaluation/sizing, but records a
    simulated resting order (db.create_virtual_pending_order) instead of
    ever calling _place_touch_turn_limit. No broker_order_id, nothing to
    cancel at a broker - check_pending_virtual_touch_turn_orders detects a
    "fill" by comparing price against limit_price directly each tick (see
    its own docstring), the same no-broker-to-poll reasoning virtual_
    entry_scan/check_virtual_stop_outs already use elsewhere in the
    virtual path.

    held/count/budget scoping is entirely this strategy's own (virtual_
    positions + virtual_pending_orders for this strategy_id only) - same
    "independent of every other strategy, real or virtual" reasoning
    virtual_entry_scan's own docstring explains for why that's fine here
    too. virtual_capital is the notional cap (not live_budget/
    live_max_positions, which don't apply to a virtual run)."""
    if "opening_candle" not in rules:
        return

    strategy_id, strategy_label = strategy_run["strategy_id"], strategy_run.get("strategy_name", "?")
    now_et = datetime.now(ET)
    session_open_et = datetime.combine(now_et.date(), dt_time(9, 30), tzinfo=ET)
    expiry_et = session_open_et + timedelta(minutes=rules["time_filter"]["entry_window_minutes"])
    if now_et >= expiry_et:
        return

    watchlist = [row["symbol"] for row in db.get_watchlist(account_id, mode, direction=side, universe=_strategy_universe(rules))]
    if not watchlist:
        return

    es_direction = _es_direction_for_scan(account_id, mode, ib, rules)
    placed_date = now_et.date().isoformat()

    risk = mode_config.risk_params(env, account_id, mode)
    portfolio_value = risk["portfolio_value"]
    max_risk_pct = risk["max_risk_pct"]
    max_position_pct = rules["risk"]["max_position_size_pct_of_portfolio"] / 100
    max_concurrent = rules["risk"]["max_concurrent_positions"]

    open_positions = db.get_virtual_positions(account_id, strategy_id=strategy_id)
    pending = db.get_virtual_pending_orders(account_id, strategy_id, "pending")
    held_symbols = {p["symbol"] for p in open_positions} | {po["symbol"] for po in pending}
    open_count = len(open_positions) + len(pending)
    open_notional = (
        sum(p["qty"] * p["entry_price"] for p in open_positions)
        + sum(po["qty"] * po["limit_price"] for po in pending)
    )
    virtual_capital = strategy_run.get("virtual_capital")

    for ticker in watchlist:
        if open_count >= max_concurrent:
            break
        if ticker in held_symbols:
            continue
        if db.has_virtual_pending_order_today(account_id, strategy_id, ticker, placed_date):
            continue

        signal = _evaluate_touch_turn_entry(account_id, mode, ticker, rules, side)
        if not signal.get("pass"):
            continue

        limit_price, initial_stop = signal["limit_price"], signal["initial_stop"]
        r = abs(limit_price - initial_stop)
        if r <= 0:
            continue

        risk_dollars = portfolio_value * (max_risk_pct / 100)
        size_by_risk = math.floor(risk_dollars / r)
        size_by_cap = math.floor(portfolio_value * max_position_pct / limit_price)
        size = min(size_by_risk, size_by_cap)
        if size < 1:
            continue

        if rules.get("es_vwap_filter") and db.is_es_vwap_filter_enabled(account_id, mode):
            gate = es_filter.check(es_direction, side)
            if not gate["allowed"]:
                _log_es_rejection(account_id, mode, rules.get("strategy_name", "?"), side, ticker, gate)
                continue

        if virtual_capital is not None and (open_notional + size * limit_price) > virtual_capital:
            log_decision(account_id, mode, {"event": "strategy_entry_blocked", "symbol": ticker, "side": side, "strategy_id": strategy_id, "reason": "virtual_capital_exceeded", "virtual": True})
            continue

        inserted = db.create_virtual_pending_order(account_id, strategy_id, {
            "symbol": ticker, "placed_date": placed_date, "side": side,
            "limit_price": limit_price, "target_price": signal["target_price"], "initial_stop": initial_stop,
            "qty": size, "placed_at": now_et.isoformat(timespec="seconds"), "expires_at": expiry_et.isoformat(timespec="seconds"),
        })
        if not inserted:
            continue
        held_symbols.add(ticker)
        open_count += 1
        open_notional += size * limit_price
        notify(f"[VIRTUAL] {strategy_label}: Touch&Turn order placed: {ticker}",
               f"{side} limit @ ${limit_price:.2f}, target ${signal['target_price']:.2f}, stop ${initial_stop:.2f}, expires {expiry_et.strftime('%H:%M')} ET", "default")
        log_decision(account_id, mode, {
            "event": "touch_turn_order_placed", "symbol": ticker, "side": side,
            "limit_price": limit_price, "target_price": signal["target_price"], "initial_stop": initial_stop, "qty": size,
            "strategy_id": strategy_id, "virtual": True,
        })


def check_pending_virtual_touch_turn_orders(account_id: int, strategy_id: int, strategy_label: str) -> None:
    """check_pending_touch_turn_orders' counterpart for a strategy_run's
    simulated resting orders - no broker to poll for a fill, so this
    compares the current price against limit_price directly each tick: a
    long (resting BUY limit) fills once price <= limit_price, a short
    (resting SELL limit) once price >= limit_price - same direction
    convention check_virtual_stop_outs already uses for "has price
    reached this level". Unlike a stop order (which can slip WORSE on a
    gap), a limit order has price protection and never fills worse than
    its own limit - so the simulated fill price is the BETTER of (limit_
    price, current price): min() for a long, max() for a short.

    Must run every tick regardless of run_mode gating elsewhere (same
    "an already-resting order still needs watching" reasoning check_
    pending_touch_turn_orders' own docstring gives for the real path)."""
    now_et = datetime.now(ET)
    for po in db.get_virtual_pending_orders(account_id, strategy_id, "pending"):
        symbol, side = po["symbol"], po["side"]
        price = _current_price(symbol)
        if price is not None:
            touched = (price <= po["limit_price"]) if side == "long" else (price >= po["limit_price"])
            if touched:
                fill_price = min(price, po["limit_price"]) if side == "long" else max(price, po["limit_price"])
                new_position = {
                    "symbol": symbol, "side": side, "entry_price": fill_price,
                    "entry_time_iso": now_et.isoformat(timespec="seconds"), "qty": po["qty"],
                    "initial_stop": po["initial_stop"], "stop_price": po["initial_stop"],
                    "state": "pre_breakeven", "r_multiple": 0.0, "target_price": po["target_price"],
                }
                db.upsert_virtual_position(account_id, strategy_id, new_position)
                db.resolve_virtual_pending_order(account_id, strategy_id, symbol, po["placed_date"], "filled")
                notify(f"[VIRTUAL] {strategy_label}: Touch&Turn FILLED: {symbol}",
                       f"@ ${fill_price:.2f}, target ${po['target_price']:.2f}, stop ${po['initial_stop']:.2f}", "default")
                log_decision(account_id, "live", {"event": "touch_turn_fill", "symbol": symbol, "side": side, "price": fill_price, "qty": po["qty"], "strategy_id": strategy_id, "virtual": True})
                continue

        expires_at = datetime.fromisoformat(po["expires_at"])
        if now_et >= expires_at:
            db.resolve_virtual_pending_order(account_id, strategy_id, symbol, po["placed_date"], "expired")
            log_decision(account_id, "live", {"event": "touch_turn_expired", "symbol": symbol, "side": side, "strategy_id": strategy_id, "virtual": True})


def check_price_triggers(account_id: int, mode: str, ib, positions: list[dict]) -> list[dict]:
    """The real STP order behind a user-set "buy line"/"sell line" is
    placed directly by place_price_trigger.py (a dashboard-spawned
    subprocess, same as trade.py/open_position.py), not by this
    orchestrator - this function only ever watches for that order's fill
    or cancellation. Runs every cycle tick regardless of the bot's enabled
    flag (same
    reasoning as check_stop_outs/manage_position/check_pending_touch_turn_
    orders - see run_cycle's Step 3/4) - a resting entry-trigger order can
    fill, or need cleaning up after a broker-side cancel, whether or not
    new entries are currently paused.

    Fill detection reads the order's OWN status/avgFillPrice/filled qty
    straight off ib.trades() (not _broker_position's net-quantity check,
    which touch_turn's equivalent uses) - a user could set a buy/sell line
    on a symbol they already hold (manually or via the bot), where a net-
    position check would misattribute the pre-existing quantity to this
    fill. On a fill, promotes it into a normal tracked position exactly
    like entry_scan's/check_pending_touch_turn_orders' own tail does
    (broker-side protective stop via _place_stop at the stop_price fixed
    at creation time, then db.upsert_position) - manage_position picks it
    up on the very next tick under whichever strategy is currently active
    for that side (or _FALLBACK_EXIT_CFG if none), the same as any other
    position; there is no separate "manually triggered" management path.

    A trigger order that's no longer live at the broker (cancelled from
    within TWS/IBKR directly, or auto-cancelled some other way) is
    resolved 'cancelled' here too, so it stops being offered for
    cancellation on the dashboard and its chart line disappears.

    Fill/open-order lookups use ib.reqExecutions()/ib.reqAllOpenOrders(),
    NOT ib.trades()/ib.openTrades() - those only ever reflect orders THIS
    connection's own client id placed or was told about, but the real
    entry-trigger order is placed by a DIFFERENT client id
    (place_price_trigger.py's own connection). A real, live-money incident
    (2026-09-09/10) found exactly this: a UNH/TSCO trigger each filled for
    real at the broker but sat "pending" in our own DB for 12+ minutes
    with no stop ever placed (ib.trades() never saw the fill on this
    connection), until a stale dashboard cancel click wrongly marked one
    'cancelled' - leaving both positions completely untracked and
    unprotected. reqExecutions/reqAllOpenOrders are account-wide (same
    reasoning sync_broker_fills already relies on for the same class of
    problem), so this now finds a fill or a genuine cancel regardless of
    which client id placed the order.

    Fills are keyed by (orderId, symbol), not orderId alone - a second,
    separate live incident (2026-09-10, CHTR/TSCO) found that IBKR's own
    orderId is NOT reliably unique across this function's own lookups:
    place_price_trigger.py reuses one fixed client id for every trigger it
    places, connecting fresh and disconnecting again each time, and two
    triggers placed close together can race Gateway's own next-order-id
    bookkeeping into handing out the same numeric orderId for two
    genuinely different real orders. Grouping fills by orderId alone then
    summed BOTH orders' executions into whichever pending trigger looked
    them up first - one filled trigger absorbed the other trigger's fill
    price and quantity too (a real $ trade sized/priced from two unrelated
    symbols' fills blended together), and the second trigger never saw its
    own execution at all. orderId collisions are Gateway's behavior, not
    something this code can prevent outright - but a fill can only ever
    genuinely belong to a trigger placed on that trigger's OWN symbol, so
    keying by the pair closes the hole even when the numeric id repeats."""
    pending = db.get_price_triggers(account_id, mode, "pending")
    if not pending:
        return positions

    fills_by_key: dict[tuple[int, str], list] = {}
    for fill in ib.reqExecutions():
        if not belongs_to_account(ib, fill.execution.acctNumber):
            continue
        fills_by_key.setdefault((fill.execution.orderId, fill.contract.symbol), []).append(fill)
    ib.reqAllOpenOrders()
    ib.sleep(1)
    open_order_ids = {t.order.orderId for t in ib.openTrades() if belongs_to_account(ib, t.order.account)}

    for trig in pending:
        order_id = trig["broker_order_id"]
        matched_fills = fills_by_key.get((order_id, trig["symbol"]))
        if matched_fills:
            symbol, side = trig["symbol"], trig["side"]
            fill_qty = sum(int(f.execution.shares) for f in matched_fills)
            total_value = sum(f.execution.shares * f.execution.price for f in matched_fills)
            fill_price = (total_value / fill_qty) if fill_qty else trig["trigger_price"]
            stop_order_id = _place_stop(ib, symbol, fill_qty, trig["stop_price"], side)
            now_iso = datetime.now(ET).isoformat(timespec="seconds")
            new_position = {
                "symbol": symbol, "side": side, "entry_price": fill_price,
                "entry_time_iso": now_iso, "qty": fill_qty,
                "initial_stop": trig["stop_price"], "stop_price": trig["stop_price"],
                "stop_order_id": stop_order_id, "state": "pre_breakeven", "r_multiple": 0.0,
                "mae_price": fill_price,
                # A manually-triggered position (this "buy line"/"sell
                # line" form, or the /order_window quick-order popup - same
                # code path, no way or reason to tell them apart) is
                # explicitly NOT bot-managed: no breakeven/trailing-stop
                # (see manage_position's own early return) and no EOD
                # force-close (see force_close_all's own held_no_manage
                # partition) - only this initial protective stop, placed
                # for real just above, ever guards it. A human decided to
                # enter this by hand; the bot leaves it alone by hand too,
                # until that same human closes it.
                "no_bot_manage": True,
            }
            db.upsert_position(account_id, mode, new_position)
            positions.append(new_position)
            db.resolve_price_trigger(account_id, mode, trig["id"], "filled", filled_at=now_iso, fill_price=fill_price)
            notify(
                f"[{mode.upper()}] Price trigger FILLED: {symbol}",
                f"{side} @ ${fill_price:.2f}, qty {fill_qty}, stop ${trig['stop_price']:.2f} - NOT bot-managed (manual entry)",
                "default",
            )
            log_decision(account_id, mode, {
                "event": "price_trigger_fill", "symbol": symbol, "side": side,
                "trigger_price": trig["trigger_price"], "price": fill_price, "qty": fill_qty, "stop": trig["stop_price"],
            })
        elif order_id not in open_order_ids:
            # No fill found AND no longer among the account's real open
            # orders - genuinely gone (cancelled directly in TWS, rejected,
            # or some other terminal state), not merely invisible to this
            # connection.
            db.resolve_price_trigger(account_id, mode, trig["id"], "cancelled")
            log_decision(account_id, mode, {"event": "price_trigger_cancelled", "symbol": trig["symbol"], "side": trig["side"], "reason": "not_open_no_fill"})

    return positions


def _cancel_price_trigger(account_id: int, mode: str, ib, trig: dict):
    """Used by run_cycle's "flatten everything now" path to clear out any
    still-resting entry trigger along with open positions. Looks the
    order up via reqAllOpenOrders/openTrades (account-wide - the trigger
    order was placed by a DIFFERENT client id, place_price_trigger.py's
    own connection, which plain ib.trades() on THIS connection never sees
    - see check_price_triggers' own docstring for the real incident this
    class of bug caused), then cancels via cancel_order_any_client since
    even seeing the order account-wide isn't enough to cancel it through
    a different client id (see that function's own docstring for the
    2026-09-10 CHTR incident that found this out).

    Checks for a fill FIRST, same reasoning: a trigger that already filled
    for real must never be blindly marked 'cancelled' (that exact mistake
    is what left the incident's positions untracked and unprotected) - if
    found filled, this leaves it 'pending' so the next regular
    check_price_triggers call promotes it properly (protective stop +
    tracked position), which a later flatten cycle then closes out same as
    any other open position.

    Both checks below also require the fill's/order's own symbol to match
    trig's - same reasoning as check_price_triggers' own (orderId, symbol)
    keying (see its docstring for the 2026-09-10 CHTR/TSCO incident):
    orderId alone can collide across two genuinely different real orders,
    and this function is reached from the "Flatten all now" emergency
    button - matching only on orderId here could skip cancelling trig's
    own still-resting order (wrongly believing it already filled, off some
    OTHER symbol's execution sharing the same numeric id) or cancel a
    completely unrelated symbol's order instead."""
    order_id = trig.get("broker_order_id")
    if order_id is not None:
        for fill in ib.reqExecutions():
            if fill.execution.orderId == order_id and fill.contract.symbol == trig["symbol"] and belongs_to_account(ib, fill.execution.acctNumber):
                log_decision(account_id, mode, {"event": "price_trigger_already_filled_skip_cancel", "symbol": trig["symbol"], "side": trig["side"]})
                return
        ib.reqAllOpenOrders()
        ib.sleep(1)
        match = next((t for t in ib.openTrades() if t.order.orderId == order_id and t.contract.symbol == trig["symbol"] and belongs_to_account(ib, t.order.account)), None)
        if match is not None:
            cancel_order_any_client(ib, match.order)
    db.resolve_price_trigger(account_id, mode, trig["id"], "cancelled")
    log_decision(account_id, mode, {"event": "price_trigger_cancelled", "symbol": trig["symbol"], "side": trig["side"], "reason": "flatten_request"})


def _orb_watchlist_filters(detail: dict, rules: dict) -> dict:
    """Maps orb.evaluate_orb_entry's raw diagnostic detail (see its own
    docstring) into the same per-filter boolean shape the classic
    D1-D3/I1-I3 model already returns, for the dashboard's Watchlist
    table: V1 (RVOL >= threshold), V2 (ATR% clears its price-tiered
    minimum), CONFIRMED (price has broken the opening-range level).
    Deliberately excludes "opening range formed" as its own column -
    evaluate_orb_entry only ever reaches the point of returning these
    fields once the range HAS formed (an unformed range is one of its
    "error" early-returns instead, same as this function's own empty-dict
    return below), so it would always read as a trivial, always-true
    checkmark. CONFLUENCE is only included when this strategy's rules
    actually configure entry_confluence (an opt-in extra gate - see
    evaluate_orb_entry's own docstring) - e.g. ORB Long v2/ORB Short v2
    but not the original ORB Long/ORB Short - same "only surface what's
    actually relevant" idea as the classic model's RSI-vs-HOD/LOD I2
    variants get from the Watchlist table's RSI column."""
    if "error" in detail:
        return {}
    vol_filters = rules["volatility_filters"]
    out = {
        "V1": detail.get("rvol", 0) >= vol_filters["V1_rvol_min"],
        "V2": detail.get("atr_tier_min") is not None and detail.get("atr_pct", 0) >= detail["atr_tier_min"],
        "CONFIRMED": bool(detail.get("confirmed")),
    }
    if rules.get("entry_confluence"):
        out["CONFLUENCE"] = bool(detail.get("confluence_ok"))
    return out


def scan_watchlist_filters(account_id: int):
    """Evaluates every watchlist symbol's entry filters, for every strategy
    that's currently virtual or live (see strategy_runs' own schema
    comment, src/db.py), and stores a snapshot for the dashboard's
    Watchlist table — independent of entry_scan, which stops early once
    the day's trade/position caps are hit and so doesn't necessarily check
    every symbol. Pure yfinance, no IBKR connection needed. A strategy_run
    that's 'off' is skipped - there's no criteria to check its candidates
    against.

    Dispatches per strategy to the classic D1-D3/I1-I3 evaluator, the ORB
    one, or Touch & Turn's own, depending on THAT strategy's own rules
    (same "opening_range"/"opening_candle" key checks entry_scan/touch_
    turn_entry_scan use) - several strategies can each have a different
    model active at once, so results carries a "model" tag per row
    ("classic"/"orb"/"touch_turn") the dashboard uses to show only the
    columns relevant to whichever strategy produced that row, instead of
    hard-coding one filter set for the whole table. Every row also carries
    its own strategy_id, so a per-strategy dashboard sheet can filter this
    same shared snapshot down to just its own candidates instead of
    needing a separate fetch/scan per strategy.

    SST Swing (rules["strategy_type"] == "sst_swing") is skipped entirely,
    same reasoning as entry_scan/virtual_entry_scan's own skip: it has no
    "opening_range"/"opening_candle" key, so falling through to the
    classic D1-D3/I1-I3 branch below would evaluate rules keys this
    family never sets (KeyError) against db.get_watchlist's INTRADAY gap
    list, which isn't even SST's own candidate source (db.get_sst_
    watchlist is) - this table is simply not meaningful for a strategy
    that trades off daily bars evaluated once a day, not this per-5-
    minute intraday snapshot."""
    status = time_gate()
    if status in ("weekend", "too_early", "closed"):
        return

    results = []
    for strategy_run in db.list_strategy_runs(account_id):
        if strategy_run["run_mode"] == "off":
            continue
        strategy_id = strategy_run["strategy_id"]
        strategy = db.get_strategy(strategy_id)
        if strategy is None:
            continue
        rules = json.loads(strategy["rules_json"])
        if rules.get("strategy_type") == "sst_swing":
            continue
        side = strategy["direction"]
        is_touch_turn = "opening_candle" in rules
        is_orb = "opening_range" in rules
        # Same signal_side-vs-side watchlist scoping as entry_scan (see
        # its own comment) - a fade strategy's candidates come from its
        # signal direction's gap-scan survivors, not its trade side's.
        watchlist_direction = rules.get("signal_side") or side
        for row in db.get_watchlist(account_id, "live", direction=watchlist_direction, universe=_strategy_universe(rules)):
            if is_touch_turn:
                detail = _evaluate_touch_turn_entry(account_id, "live", row["symbol"], rules, side)
                results.append({"symbol": row["symbol"], "gap_pct": row["gap_pct"], "model": "touch_turn", "strategy_id": strategy_id, **detail})
            elif is_orb:
                detail = _evaluate_orb_entry(account_id, "live", row["symbol"], rules, side)
                results.append({
                    "symbol": row["symbol"], "gap_pct": row["gap_pct"], "model": "orb", "strategy_id": strategy_id,
                    **detail, **_orb_watchlist_filters(detail, rules),
                })
            else:
                detail = _evaluate_entry_filters(account_id, "live", row["symbol"], rules, side)
                results.append({"symbol": row["symbol"], "gap_pct": row["gap_pct"], "model": "classic", "strategy_id": strategy_id, **detail})

    for mode in db.MODES:
        db.update_watchlist_filters(account_id, mode, results)


# -------------------------------------------------------------------- main
def _rules_for_position(pos: dict, legacy_rules_by_side: dict) -> dict:
    """A position's own strategy's rules, looked up by its own strategy_id
    - regardless of whether that strategy_run is still 'live' right now
    (a position stays managed even if its strategy was deactivated or
    deleted after it was opened, same as before multi-strategy support -
    see manage_position's own docstring). Falls back to the side's legacy
    single-active-strategy rules (account_active_strategy, via db.
    get_active_rules) for a position with no strategy_id at all - one
    that predates this feature, or was opened by some other unattributed
    path."""
    strategy_id = pos.get("strategy_id")
    if strategy_id is not None:
        strategy = db.get_strategy(strategy_id)
        if strategy is not None:
            return json.loads(strategy["rules_json"])
    return legacy_rules_by_side.get(pos.get("side", "long")) or {"exit": _FALLBACK_EXIT_CFG}


def _manage_virtual_strategy_positions(account_id: int, strategy_run: dict):
    """One virtual strategy_run's own position management for this cycle
    tick - stop-out detection, then manage_virtual_position for whatever
    survives, then its own resting Touch & Turn orders. Mirrors run_
    cycle's own Step 3/4/4.5 for real positions, just against this
    strategy's own virtual_positions/virtual_pending_orders - called for
    every virtual strategy_run on every tick, independent of is_bot_
    enabled/force_close/manage_only below (same "manage what's already
    open no matter what" reasoning Step 3/4 already follow for real
    positions)."""
    strategy_id = strategy_run["strategy_id"]
    strategy_label = strategy_run.get("strategy_name", "?")
    strategy = db.get_strategy(strategy_id)
    if strategy is None:
        return
    rules = json.loads(strategy["rules_json"])
    positions = check_virtual_stop_outs(account_id, strategy_id, strategy_label, db.get_virtual_positions(account_id, strategy_id=strategy_id))
    for pos in positions:
        manage_virtual_position(account_id, strategy_id, strategy_label, pos, rules)
    check_pending_virtual_touch_turn_orders(account_id, strategy_id, strategy_label)


def _flatten_virtual_strategy(account_id: int, strategy_run: dict, reason: str):
    """Closes every one of this virtual strategy's own open positions (at
    the current price) and cancels every one of its own resting Touch &
    Turn orders - the virtual-path counterpart to force_close_all/
    _cancel_pending_touch_turn_order, called from run_cycle's own "flatten
    everything now" and EOD force_close paths (see their own comments for
    why virtual is included in both - "everything"/EOD-close means
    everything, not just real money)."""
    strategy_id = strategy_run["strategy_id"]
    strategy_label = strategy_run.get("strategy_name", "?")
    now_iso = datetime.now(ET).isoformat(timespec="seconds")
    for pos in db.get_virtual_positions(account_id, strategy_id=strategy_id):
        side = pos.get("side", "long")
        price = _current_price(pos["symbol"])
        exit_price = price if price is not None else pos["entry_price"]
        pnl = ((exit_price - pos["entry_price"]) if side == "long" else (pos["entry_price"] - exit_price)) * pos["qty"]
        db.record_virtual_trade(account_id, strategy_id, {
            "symbol": pos["symbol"], "side": side, "entry_price": pos["entry_price"],
            "entry_time_iso": pos["entry_time_iso"], "exit_price": exit_price, "exit_time_iso": now_iso,
            "qty": pos["qty"], "final_r": pos.get("r_multiple"), "pnl_dollars": pnl, "exit_reason": reason,
        })
        db.remove_virtual_position(account_id, strategy_id, pos["symbol"])
        notify(f"[VIRTUAL] {strategy_label}: FLATTEN {pos['symbol']}", f"closed @ ${exit_price:.2f}, P&L ${pnl:+.2f}", "default")
        log_decision(account_id, "live", {"event": "flatten_close", "symbol": pos["symbol"], "side": side, "price": exit_price, "pnl": pnl, "strategy_id": strategy_id, "virtual": True, "reason": reason})
    for po in db.get_virtual_pending_orders(account_id, strategy_id, "pending"):
        db.resolve_virtual_pending_order(account_id, strategy_id, po["symbol"], po["placed_date"], "cancelled")
        log_decision(account_id, "live", {"event": "touch_turn_cancelled", "symbol": po["symbol"], "side": po["side"], "strategy_id": strategy_id, "virtual": True})


def run_cycle(account_id: int, mode: str):
    """Runs one tick of the trading cycle for the given account+mode. Safe
    to call as often as run_service.py's own "cycle" job interval allows
    (currently every 1 minute — see that constant's own comment) all day —
    it self-gates on market hours."""
    status = time_gate()
    if status in ("weekend", "too_early", "closed"):
        return status

    ibkr = None
    try:
        env = _env()
        # Legacy fallback only - see _rules_for_position. Every position/
        # order opened under multi-strategy support carries its own
        # strategy_id and looks its own strategy up directly instead,
        # regardless of whether that strategy is still 'live' right now.
        legacy_rules_by_side = {"long": db.get_active_rules(account_id, "long"), "short": db.get_active_rules(account_id, "short")}
        client_id = int(env.get("IBKR_CLIENT_ID", 2))

        try:
            ibkr = _connect(env, account_id, mode, client_id)
        except Exception:
            import time as _time
            _time.sleep(5)
            ibkr = _connect(env, account_id, mode, client_id)

        ib = ibkr.ib
        positions = db.get_open_positions(account_id, mode)
        virtual_strategy_runs = db.list_strategy_runs(account_id, run_mode="virtual")

        # Emergency "flatten everything now" request from the dashboard takes
        # priority over the normal cycle, but position management always runs
        # first regardless of the enabled flag, per Step 3/4 below. Any
        # Touch & Turn order still resting in the market gets cancelled too
        # - force_close_all only ever knows about already-FILLED positions,
        # so a pending, unfilled limit order needs its own cancellation here.
        # Virtual strategies' own open positions/resting orders are flattened
        # too - "everything now" means everything, not just real money.
        if db.consume_flatten_request(account_id, mode):
            positions = check_stop_outs(account_id, mode, ib, positions)
            force_close_all(account_id, mode, ib, positions)
            for po in db.get_pending_orders(account_id, mode, "pending"):
                _cancel_pending_touch_turn_order(account_id, mode, ib, po, "cancelled")
            for trig in db.get_price_triggers(account_id, mode, "pending"):
                _cancel_price_trigger(account_id, mode, ib, trig)
            for strategy_run in virtual_strategy_runs:
                _flatten_virtual_strategy(account_id, strategy_run, "flatten_request")
            db.record_cycle_run(account_id, mode, "flattened_on_request")
            return "flattened_on_request"

        positions = check_stop_outs(account_id, mode, ib, positions)  # Step 3
        positions = [
            manage_position(account_id, mode, ib, p, _rules_for_position(p, legacy_rules_by_side))
            for p in positions
        ]  # Step 4
        positions = check_pending_touch_turn_orders(account_id, mode, ib, positions)  # Step 4.5 - fills/expiry, always runs
        positions = check_price_triggers(account_id, mode, ib, positions)  # Step 4.6 - user-set buy/sell line fills, always runs

        # Every virtual strategy's own position management - same "always
        # runs, regardless of what happens below" reasoning as Step 3/4
        # above, just against virtual_positions/virtual_pending_orders
        # instead of the real broker.
        for strategy_run in virtual_strategy_runs:
            _manage_virtual_strategy_positions(account_id, strategy_run)

        if status == "force_close":  # Step 6 - EOD: flatten real AND virtual
            force_close_all(account_id, mode, ib, positions)
            for strategy_run in virtual_strategy_runs:
                _flatten_virtual_strategy(account_id, strategy_run, "force_close")
            db.record_cycle_run(account_id, mode, status)
            return status

        if status == "manage_only":  # Step 7
            db.record_cycle_run(account_id, mode, status)
            return status

        if db.is_bot_enabled(account_id, mode):  # Step 8 - every active strategy_run scans independently
            for strategy_run in db.list_strategy_runs(account_id):
                if strategy_run["run_mode"] == "off":
                    continue
                strategy = db.get_strategy(strategy_run["strategy_id"])
                if strategy is None:
                    continue
                rules = json.loads(strategy["rules_json"])
                side = strategy["direction"]
                if strategy_run["run_mode"] == "live":
                    positions = entry_scan(account_id, mode, ib, positions, rules, env, side, strategy_run=strategy_run)
                    touch_turn_entry_scan(account_id, mode, ib, rules, env, side, strategy_run=strategy_run)
                elif strategy_run["run_mode"] == "virtual":
                    virtual_entry_scan(account_id, mode, ib, rules, env, side, strategy_run)
                    virtual_touch_turn_entry_scan(account_id, mode, ib, rules, env, side, strategy_run)
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


def sync_broker_fills(ib, account_id: int, mode: str):
    """Records any IBKR execution not already in the trades table - not
    just the entries/exits trade.py and close_position.py place and log
    themselves immediately, but ALSO fills that happen entirely at the
    broker with no script of ours involved: a stop, take-profit, or ATR
    trailing stop triggering, or a LIMIT+ATR bracket's entry filling
    (open_position.py places it but never waits around for the fill).
    Without this, Trade History only ever showed the subset of trades our
    own scripts happened to be watching when they filled - a stop firing
    while nobody's running a script for it would leave a position that
    just vanishes from Account Holdings with no record of how or when it
    actually closed.

    reqExecutions() with no filter returns today's fills across EVERY
    account this login manages, not just this mode's - a login authorized
    for more than one account (see IBKRClient.__init__) would otherwise
    let another account's real activity get silently recorded as this
    bot's own trade history. belongs_to_account narrows it back down to
    match this function's own (account_id, mode) scoping. execId is IBKR's own globally
    unique id per execution - db.trade_exec_id_exists is what keeps this
    idempotent across every periodic call, and consistent with a fill
    trade.py/close_position.py already recorded immediately themselves
    (they tag their own record_trade call with that same execId - see
    their comments)."""
    for fill in ib.reqExecutions():
        exec_id = fill.execution.execId
        if not exec_id or db.trade_exec_id_exists(exec_id):
            continue
        if not belongs_to_account(ib, fill.execution.acctNumber):
            continue
        side = "BUY" if fill.execution.side == "BOT" else "SELL"
        ts = fill.execution.time
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=ZoneInfo("UTC"))
        db.record_trade(
            account_id, mode, fill.contract.symbol, side, int(fill.execution.shares),
            fill.execution.price, fill.execution.orderId, "Filled",
            exec_id=exec_id, timestamp_iso=ts.astimezone(ET).isoformat(timespec="seconds"),
        )


# Consecutive refresh_account_info runs (~5 min apart, same cadence as the
# job that calls it - see run_service.py) a bot-tracked position has to be
# absent from the broker's own real holdings before it's treated as closed
# outside the bot (manual TWS/Mobile sell, another script, etc.) and its DB
# row removed. Requiring more than one confirms this against a single
# stale/lagging broker snapshot wrongly dropping tracking for a real,
# still-open live position - the same "don't rely on a single mechanism for
# something this important" reasoning the protective stop order itself
# already gets (see _place_touch_turn_limit's own docstring).
BROKER_MISSING_STREAK_TO_RECONCILE = 2


def reconcile_broker_positions(account_id: int, mode: str, ib, broker_symbols: set[str]):
    """Removes a bot-tracked open position (the `positions` table - what
    the dashboard's Open Positions card and every entry/management gate
    read) once it's been confirmed absent from the broker's own real
    holdings for BROKER_MISSING_STREAK_TO_RECONCILE consecutive
    refresh_account_info runs in a row.

    Without this, a position closed by anything OTHER than the bot's own
    tracked exit mechanisms - a manual sell via TWS/IBKR Mobile being the
    common case, since it doesn't fill the bot's own resting stop order and
    so isn't recognized by check_stop_outs either - leaves a permanent
    phantom row: the dashboard keeps showing a position that's already
    flat, and manage_position keeps "managing" it every cycle (fetching a
    price, evaluating breakeven/trailing, and potentially trying to place
    or cancel stop orders against a symbol with zero real shares).

    Also best-effort cancels the position's own resting stop order (if
    any) once reconciled away - IBKR does NOT auto-cancel a standalone
    stop order just because the shares it protects are already gone, so a
    manually-sold position can leave a stale SELL stop resting that would
    open an unintended short if the price ever fell to it. _cancel_stop
    already no-ops safely if the order is already gone (expired,
    cancelled, or never existed)."""
    for pos in db.get_open_positions(account_id, mode):
        if pos["symbol"] in broker_symbols:
            db.mark_position_seen_at_broker(account_id, mode, pos["symbol"])
            continue
        streak = db.bump_position_broker_missing(account_id, mode, pos["symbol"])
        if streak < BROKER_MISSING_STREAK_TO_RECONCILE:
            continue
        try:
            _cancel_stop(ib, pos.get("stop_order_id"))
        except Exception:
            pass
        db.remove_position(account_id, mode, pos["symbol"])
        notify(
            f"[{mode.upper()}] POSITION CLOSED ELSEWHERE: {pos['symbol']}",
            f"No longer held at the broker after {streak} consecutive checks (~"
            f"{5 * streak} min) - removed from tracking. Likely closed manually "
            "(TWS/Mobile) or by another process; its stop order (if any) was "
            "also cancelled - verify at the broker if in doubt.", "high",
        )
        log_decision(account_id, mode, {
            "event": "position_closed_elsewhere", "symbol": pos["symbol"],
            "side": pos.get("side", "long"), "broker_missing_streak": streak,
        })


def refresh_account_info(account_id: int, mode: str):
    """Pulls net liquidation / cash balance / buying power, every real
    position and resting order, and today's executions (see
    sync_broker_fills) from IBKR and stores them in the DB for the
    dashboard. Runs on its own IBKR client id, independent of market
    hours, so the dashboard has something to show even outside the
    trading window."""
    env = _env()
    ibkr = _connect(env, account_id, mode, ACCOUNT_REFRESH_CLIENT_ID)
    try:
        ib = ibkr.ib
        ib.sleep(2)  # let account summary data populate after connecting
        # Unfiltered, accountSummary() returns a row per tag PER managed
        # account when this login is authorized for more than one (see
        # IBKRClient.__init__) - the dict comprehension then collapses
        # same-tag rows from different accounts down to whichever one
        # iterated last, which can just as easily be the OTHER account's
        # (possibly zero/unfunded) numbers as this mode's real ones.
        values = {row.tag: row.value for row in ib.accountSummary(ibkr.account or "")}
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
            for p in scoped_positions(ib) if p.position != 0
        ]
        db.update_broker_positions(account_id, mode, broker_positions)

        # Every resting stop/limit order in the account, independent of
        # which client ID placed it — reqAllOpenOrders (unlike openTrades
        # alone) pulls in orders from other sessions too: the bot's own
        # cycle connection, a manual TWS/Mobile order, etc. Lets the
        # dashboard show a holding's real protective orders even for a
        # symbol the bot never touched. reconcile_broker_positions below
        # relies on this too - its own best-effort stop cancel (_cancel_stop)
        # calls reqAllOpenOrders() itself, but doing it here as well costs
        # nothing extra and keeps this connection's own order view fresh
        # for the broker_orders list built just below.
        ib.reqAllOpenOrders()
        ib.sleep(1)
        reconcile_broker_positions(account_id, mode, ib, {p["symbol"] for p in broker_positions})
        broker_orders = [
            {
                "symbol": t.contract.symbol,
                "order_type": t.order.orderType,
                "action": t.order.action,
                "qty": t.order.totalQuantity,
                # TRAIL orders (the ATR bracket's stop - see open_position.py)
                # carry their distance in auxPrice too, same as STP - only
                # LMT orders actually use lmtPrice. Getting this wrong means
                # showing IBKR's "unset" sentinel (a huge float) instead of
                # the real trail amount.
                "price": t.order.auxPrice if t.order.orderType in ("STP", "TRAIL") else t.order.lmtPrice,
                "order_id": t.order.orderId,
                "status": t.orderStatus.status,
            }
            for t in ib.openTrades()
            if t.orderStatus.status not in ("Cancelled", "ApiCancelled", "Filled", "Inactive")
            and belongs_to_account(ib, t.order.account)
        ]
        db.update_broker_orders(account_id, mode, broker_orders)
        sync_broker_fills(ib, account_id, mode)
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
    parser.add_argument("--mode", choices=db.MODES, default="live")
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
