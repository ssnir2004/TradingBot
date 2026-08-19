"""The autonomous trading cycle. Runs every 5 minutes from the always-on
service (see run_service.py). On each tick: checks market hours, handles any
pending emergency flatten-all request from the dashboard, reconciles
stop-outs, manages open positions (breakeven flip, partial profit, swing-low
trailing stop) — always, regardless of the enabled flag, so an open position
never goes unmanaged — then, only if the bot is enabled from the dashboard,
scans the watchlist for new Trend Join Long entries. Force-closes everything
before the close.

Time gate (America/New_York):
    Sat/Sun                        -> "weekend"   (exit <1s)
    before 10:00 or after 16:00    -> "too_early" / "closed" (exit <1s)
    10:00-10:05 or 15:30-15:51     -> "manage_only" (no new entries)
    15:51-16:00                    -> "force_close"
    10:05-15:30                    -> "ok" (full cycle: manage + scan)
"""
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

from src import db
from src.ibkr_client import IBKRClient
from src.notify import notify

PROJECT_DIR = Path(__file__).resolve().parent
ET = ZoneInfo("America/New_York")
SUBPROCESS_TIMEOUT = 30

TOO_EARLY_END = dt_time(10, 0)
MANAGE_ONLY_MORNING_END = dt_time(10, 5)
MANAGE_ONLY_AFTERNOON_START = dt_time(15, 30)
FORCE_CLOSE_START = dt_time(15, 51)
CLOSED_START = dt_time(16, 0)


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
    if t < MANAGE_ONLY_MORNING_END or t >= MANAGE_ONLY_AFTERNOON_START:
        return "manage_only"
    return "ok"


def log_decision(entry: dict):
    entry = dict(entry)
    event = entry.pop("event")
    db.log_decision(event, **entry)


def _env() -> dict:
    return dotenv_values(PROJECT_DIR / ".env")


# ---------------------------------------------------------------- Step 3 ---
def check_stop_outs(ib, positions: list[dict]) -> list[dict]:
    if not positions:
        return positions

    cutoff = datetime.now(ET) - timedelta(hours=1)
    fills = ib.fills()
    stopped_symbols = set()

    for pos in positions:
        stop_order_id = pos.get("stop_order_id")
        if stop_order_id is None:
            continue
        for fill in fills:
            fill_time = fill.time
            if fill_time.tzinfo is None:
                fill_time = fill_time.replace(tzinfo=ZoneInfo("UTC"))
            if fill_time.astimezone(ET) < cutoff:
                continue
            if fill.execution.orderId == stop_order_id:
                pnl = (fill.execution.avgPrice - pos["entry_price"]) * pos["qty"]
                notify(f"STOP {pos['symbol']}", f"exit ${fill.execution.avgPrice:.2f}, P&L ${pnl:+.2f}", "default")
                log_decision({"event": "stop_out", "symbol": pos["symbol"], "fill_price": fill.execution.avgPrice, "pnl": pnl})
                stopped_symbols.add(pos["symbol"])
                db.remove_position(pos["symbol"])

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


def _place_stop(ib, symbol: str, quantity: int, stop_price: float) -> int:
    contract = _qualify(ib, symbol)
    order = StopOrder("SELL", quantity, round(stop_price, 2))
    trade = ib.placeOrder(contract, order)
    ib.sleep(1)
    return trade.order.orderId


def _market_sell(ib, symbol: str, quantity: int) -> bool:
    proc = subprocess.run(
        [sys.executable, str(PROJECT_DIR / "trade.py"),
         "--symbol", symbol, "--side", "SELL", "--size", str(quantity)],
        capture_output=True, text=True, timeout=SUBPROCESS_TIMEOUT,
    )
    log_decision({"event": "market_sell_subprocess", "symbol": symbol, "qty": quantity, "stdout": proc.stdout})
    return proc.returncode == 0


def _get_5min_bars(symbol: str) -> pd.DataFrame | None:
    try:
        bars = yf.Ticker(symbol.replace(" ", "-")).history(period="2d", interval="5m")
        return bars if not bars.empty else None
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


def _current_price(symbol: str) -> float | None:
    bars = _get_5min_bars(symbol)
    if bars is None or bars.empty:
        return None
    return float(bars["Close"].iloc[-1])


# ---------------------------------------------------------------- Step 4 ---
def manage_position(ib, pos: dict, rules: dict) -> dict:
    exit_cfg = rules["exit"]
    price = _current_price(pos["symbol"])
    if price is None:
        return pos

    entry = pos["entry_price"]
    initial_risk = entry - pos["initial_stop"]
    if initial_risk <= 0:
        return pos
    r_multiple = (price - entry) / initial_risk
    pos["r_multiple"] = r_multiple

    if pos["state"] == "pre_breakeven":
        if r_multiple >= exit_cfg["breakeven_trigger_R"]:
            _cancel_stop(ib, pos.get("stop_order_id"))
            pos["stop_order_id"] = _place_stop(ib, pos["symbol"], pos["qty"], entry)
            pos["stop_price"] = entry
            pos["state"] = "post_breakeven_no_partial"
            notify(f"BE {pos['symbol']}", f"stop -> ${entry:.2f}", "default")
            log_decision({"event": "breakeven_flip", "symbol": pos["symbol"], "new_stop": entry})
        elif r_multiple >= exit_cfg["partial_profit_trigger_R"]:
            sell_qty = math.ceil(pos["qty"] * exit_cfg["partial_profit_fraction"])
            sell_qty = min(sell_qty, pos["qty"])
            if _market_sell(ib, pos["symbol"], sell_qty):
                remaining = pos["qty"] - sell_qty
                new_stop_price = entry * 0.99
                _cancel_stop(ib, pos.get("stop_order_id"))
                pos["qty"] = remaining
                if remaining > 0:
                    pos["stop_order_id"] = _place_stop(ib, pos["symbol"], remaining, new_stop_price)
                    pos["stop_price"] = new_stop_price
                pos["state"] = "post_breakeven_partial_done"
                notify(f"PARTIAL {pos['symbol']}", f"sold {sell_qty}/{sell_qty + remaining} @ ${price:.2f}", "default")
                log_decision({"event": "partial_profit", "symbol": pos["symbol"], "sold": sell_qty, "price": price})

    if pos["state"].startswith("post_breakeven"):
        bars = _get_5min_bars(pos["symbol"])
        if bars is not None and len(bars) > 5:
            swing_low = _find_latest_swing_low(bars)
            if swing_low is not None and swing_low - 0.01 > pos["initial_stop"]:
                candidate_stop = swing_low - 0.01
                current_stop = pos.get("stop_price", pos["initial_stop"])
                if candidate_stop > current_stop:
                    _cancel_stop(ib, pos.get("stop_order_id"))
                    pos["stop_order_id"] = _place_stop(ib, pos["symbol"], pos["qty"], candidate_stop)
                    old_stop = pos.get("stop_price", pos["initial_stop"])
                    pos["stop_price"] = candidate_stop
                    notify(f"TRAIL {pos['symbol']}", f"stop ${old_stop:.2f} -> ${candidate_stop:.2f}", "default")
                    log_decision({"event": "trail_stop", "symbol": pos["symbol"], "old": old_stop, "new": candidate_stop})

    if pos["qty"] > 0:
        db.upsert_position(pos)
    return pos


# ---------------------------------------------------------------- Step 6 ---
def force_close_all(ib, positions: list[dict]):
    if not positions:
        return

    notify("EOD Force Close", f"flattening {len(positions)} positions", "high")
    for pos in positions:
        _cancel_stop(ib, pos.get("stop_order_id"))
        _market_sell(ib, pos["symbol"], pos["qty"])
        log_decision({"event": "force_close", "symbol": pos["symbol"], "qty": pos["qty"]})
        db.remove_position(pos["symbol"])


# ---------------------------------------------------------------- Step 8 ---
def _evaluate_entry_filters(ticker: str, rules: dict) -> dict | None:
    """Evaluate D1-D3 (daily) and I1-I3 (intraday) from the active strategy's
    rules for one ticker. Returns {"price", "low_of_day"} on a full pass,
    else None. Uses yfinance only (same free/keyless data source as
    morning_prefilter.py)."""
    daily_filters = rules["daily_filters"]
    intraday_filters = rules["intraday_filters"]
    yahoo_symbol = ticker.replace(" ", "-")

    try:
        daily = yf.Ticker(yahoo_symbol).history(period="260d", interval="1d")
        if len(daily) < 201:
            return None
        prior_day = daily.iloc[-2]
        sma200 = daily["Close"].iloc[-201:-1].mean()

        intraday = yf.Ticker(yahoo_symbol).history(period="1d", interval="5m", prepost=True)
        if intraday.empty:
            return None
        intraday.index = intraday.index.tz_convert(ET)

        current_price = float(intraday["Close"].iloc[-1])
        today = datetime.now(ET).date()
        today_bars = intraday[intraday.index.date == today]
        if today_bars.empty:
            return None

        premarket_bars = today_bars[today_bars.index.time < dt_time(9, 30)]
        regular_bars = today_bars[today_bars.index.time >= dt_time(9, 30)]
        low_of_day = float(regular_bars["Low"].min()) if not regular_bars.empty else float(today_bars["Low"].min())

        # D1: price above yesterday's daily high
        d1 = current_price > float(prior_day["High"])
        # D2: yesterday's close above the 200-day SMA
        d2 = float(prior_day["Close"]) > float(sma200)
        # D3: gap >= min_gap_pct from previous close
        prior_close = float(prior_day["Close"])
        gap_pct = (current_price - prior_close) / prior_close * 100 if prior_close else 0.0
        d3 = gap_pct >= daily_filters["D3_min_gap_pct_from_prior_close"]

        # I1: price above today's premarket high
        premarket_high = float(premarket_bars["High"].max()) if not premarket_bars.empty else float("-inf")
        i1 = current_price > premarket_high

        # I2: price above today's high-so-far (excluding the current/last bar)
        hod_so_far = float(today_bars["High"].iloc[:-1].max()) if len(today_bars) > 1 else float("-inf")
        i2 = current_price > hod_so_far

        # I3: relative volume vs lookback-day average >= threshold
        lookback = intraday_filters["I3_rvol_lookback_days"]
        avg_daily_volume = float(daily["Volume"].iloc[-(lookback + 1):-1].mean())
        today_volume_so_far = float(today_bars["Volume"].sum())
        rvol = today_volume_so_far / avg_daily_volume if avg_daily_volume else 0.0
        i3 = rvol >= intraday_filters["I3_rvol_min"]

        passed = bool(d1 and d2 and d3 and i1 and i2 and i3)
        log_decision({
            "event": "filter_eval", "symbol": ticker, "pass": passed,
            "D1": bool(d1), "D2": bool(d2), "D3": bool(d3),
            "I1": bool(i1), "I2": bool(i2), "I3": bool(i3),
            "price": current_price, "rvol": rvol, "gap_pct": gap_pct,
        })

        if not passed:
            return None
        return {"price": current_price, "low_of_day": low_of_day}
    except Exception as exc:  # noqa: BLE001 - one bad ticker must not kill the scan
        log_decision({"event": "filter_eval_error", "symbol": ticker, "error": str(exc)})
        return None


def entry_scan(ib, positions: list[dict], rules: dict, env: dict) -> list[dict]:
    max_trades_per_day = int(env.get("MAX_TRADES_PER_DAY", 5))
    if db.count_todays_buys() >= max_trades_per_day:
        return positions

    max_concurrent = rules["risk"]["max_concurrent_positions"]
    if len(positions) >= max_concurrent:
        return positions

    watchlist = [row["symbol"] for row in db.get_watchlist()]
    if not watchlist:
        return positions

    held_symbols = {p.contract.symbol for p in ib.positions() if p.position > 0}
    held_symbols |= {p["symbol"] for p in positions}

    portfolio_value = float(env.get("PORTFOLIO_VALUE_USD", 25000))
    max_risk_pct = float(env.get("MAX_RISK_PER_TRADE_PCT", 1.0))
    max_position_pct = rules["risk"]["max_position_size_pct_of_portfolio"] / 100

    for ticker in watchlist:
        if len(positions) >= max_concurrent:
            break
        if ticker in held_symbols:
            continue
        if db.count_todays_buys() >= max_trades_per_day:
            break

        signal = _evaluate_entry_filters(ticker, rules)
        if signal is None:
            continue

        price = signal["price"]
        low_of_day = signal["low_of_day"]
        initial_stop = low_of_day * 0.99
        r = price - initial_stop
        if r <= 0:
            continue

        risk_dollars = portfolio_value * (max_risk_pct / 100)
        size_by_risk = math.floor(risk_dollars / r)
        size_by_cap = math.floor(portfolio_value * max_position_pct / price)
        size = min(size_by_risk, size_by_cap)
        if size < 1:
            continue

        proc = subprocess.run(
            [sys.executable, str(PROJECT_DIR / "trade.py"),
             "--symbol", ticker, "--side", "BUY", "--size", str(size)],
            capture_output=True, text=True, timeout=SUBPROCESS_TIMEOUT,
        )
        log_decision({"event": "entry_attempt", "symbol": ticker, "qty": size, "price": price, "stdout": proc.stdout})
        if proc.returncode != 0:
            continue

        stop_order_id = _place_stop(ib, ticker, size, initial_stop)
        new_position = {
            "symbol": ticker,
            "entry_price": price,
            "entry_time_iso": datetime.now(ET).isoformat(timespec="seconds"),
            "qty": size,
            "initial_stop": initial_stop,
            "stop_price": initial_stop,
            "stop_order_id": stop_order_id,
            "state": "pre_breakeven",
            "r_multiple": 0.0,
        }
        db.upsert_position(new_position)
        positions.append(new_position)
        held_symbols.add(ticker)
        notify(f"BUY {ticker}", f"@ ${price:.2f}, stop ${initial_stop:.2f}, qty {size}", "default")
        log_decision({"event": "entry", "symbol": ticker, "price": price, "stop": initial_stop, "qty": size})

    return positions


# -------------------------------------------------------------------- main
def run_cycle():
    """Runs one tick of the trading cycle. Safe to call every 5 minutes
    all day — it self-gates on market hours."""
    status = time_gate()
    if status in ("weekend", "too_early", "closed"):
        return status

    ibkr = None
    try:
        env = _env()
        rules = db.get_active_rules()

        try:
            ibkr = IBKRClient(
                env.get("IBKR_HOST", "127.0.0.1"),
                int(env.get("IBKR_PORT", 7497)),
                int(env.get("IBKR_CLIENT_ID", 2)),
            )
        except Exception:
            import time as _time
            _time.sleep(5)
            ibkr = IBKRClient(
                env.get("IBKR_HOST", "127.0.0.1"),
                int(env.get("IBKR_PORT", 7497)),
                int(env.get("IBKR_CLIENT_ID", 2)),
            )

        ib = ibkr.ib
        positions = db.get_open_positions()

        # Emergency "flatten everything now" request from the dashboard takes
        # priority over the normal cycle, but position management always runs
        # first regardless of the enabled flag, per Step 3/4 below.
        if db.consume_flatten_request():
            positions = check_stop_outs(ib, positions)
            force_close_all(ib, positions)
            db.record_cycle_run("flattened_on_request")
            return "flattened_on_request"

        positions = check_stop_outs(ib, positions)  # Step 3
        positions = [manage_position(ib, p, rules) for p in positions]  # Step 4

        if status == "force_close":  # Step 6
            force_close_all(ib, positions)
            db.record_cycle_run(status)
            return status

        if status == "manage_only":  # Step 7
            db.record_cycle_run(status)
            return status

        if db.is_bot_enabled():
            entry_scan(ib, positions, rules, env)  # Step 8
        else:
            log_decision({"event": "entries_paused", "reason": "bot_disabled"})

        db.record_cycle_run(status)
        return status
    except Exception as exc:  # noqa: BLE001
        db.log_cycle_error(traceback.format_exc())
        notify("Cycle CRASHED", str(exc)[:500], "high")
        db.record_cycle_run("error")
        if ibkr is not None:
            ibkr.disconnect()
        raise
    finally:
        if ibkr is not None:
            ibkr.disconnect()


def emergency_check():
    """Cheap poll for a pending dashboard flatten-all request. Only opens an
    IBKR connection when the flag is actually set, so this is safe to call
    frequently (e.g. every 15-30s) from the service scheduler."""
    if db.get_setting("flatten_now", "false") != "true":
        return

    env = _env()
    ibkr = IBKRClient(
        env.get("IBKR_HOST", "127.0.0.1"),
        int(env.get("IBKR_PORT", 7497)),
        int(env.get("IBKR_CLIENT_ID", 2)),
    )
    try:
        if db.consume_flatten_request():
            positions = db.get_open_positions()
            positions = check_stop_outs(ibkr.ib, positions)
            force_close_all(ibkr.ib, positions)
            db.record_cycle_run("flattened_on_request")
    finally:
        ibkr.disconnect()


def main():
    db.init_db(seed_rules_path=PROJECT_DIR / "rules.json")
    try:
        run_cycle()
    except Exception:
        sys.exit(1)


if __name__ == "__main__":
    main()
