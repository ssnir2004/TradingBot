"""Phase 3 - real order placement on the live IBKR account. Everything
upstream of this module (scanner, detectors, alert) only ever computes
levels; this is the one and only place that calls IBKR to buy or sell.

Isolated from cycle.py / the S&P 500 engine on every axis:
  - its own IBKR client id (env MOMENTUM_LIVE_CLIENT_ID, default 25 - see
    .env.example's reserved-id list; this one is intentionally far from
    2/3/4/13-18)
  - its own position/trade tables (momentum_positions/momentum_trades,
    momentum.store) - never touches or even reads the S&P engine's own
    `positions` table, and never acts on a symbol/position it didn't
    itself open
  - the one shared touchpoint is src.ibkr_client's cross-client-safe
    cancel (cancel_order_any_client) - reused deliberately, because it
    exists specifically to fix a real live incident (2026-09-10, CHTR:
    see that module's own docstring) where a same-client cancel silently
    failed and left a duplicate stop resting. Getting stop cancellation
    wrong here would be the same bug in a new place - every cancel in
    this module goes through _cancel_resting_stop, which looks the order
    up fresh via reqAllOpenOrders (never a hand-built stand-in Order)
    before handing it to cancel_order_any_client.

live.enabled (momentum.config) is the master kill switch, independent of
the scanner's own on/off switch. Circuit breakers (momentum.config's
"live" block) are checked before every new entry - per-strategy
concurrent-position/daily-trade/daily-loss caps plus one global daily-loss
backstop across both strategies. An open position is still MANAGED
(scaled, stopped, force-closed) even if a circuit breaker would block a
NEW entry - breakers only ever gate opening something new.

R-multiple bookkeeping: `stop_price` on a momentum_positions row is the
CURRENT resting stop (moves to breakeven after scale1, then trails) -
`initial_stop_price` is captured once at entry and never touched again.
Every R-multiple-based decision (red-candle hold, bailout) is computed
against initial_stop_price; using the live (possibly trailed-above-entry)
stop_price there would divide by zero or flip sign once a winner's stop
has trailed past its own entry price.

Order construction, matching the S&P engine's own conventions
(cycle.py's _qualify/_place_stop, trade.py's IBKRClient.place_order):
MarketOrder with outsideRth=True/tif=DAY for entries and scale-out exits,
StopOrder for the protective stop.
"""
import logging
import time as _time
from datetime import datetime, time as dtime
from pathlib import Path
from zoneinfo import ZoneInfo

from dotenv import dotenv_values
from ib_async import IB, MarketOrder, Stock, StopOrder

from src import db
from src.ibkr_client import belongs_to_account, cancel_order_any_client, scoped_positions
from src.notify import notify
from momentum import bars, store

log = logging.getLogger("momentum.live")

ET = ZoneInfo("America/New_York")
PROJECT_DIR = Path(__file__).resolve().parent.parent
IBKR_PORT = 4001  # the one live Gateway this box runs (see DEPLOY.md) - momentum only ever trades "live"
ORDER_FILL_TIMEOUT_SECONDS = 20


def _env() -> dict:
    return dotenv_values(PROJECT_DIR / ".env")


def _client_id(cfg: dict) -> int:
    env = _env()
    return int(env.get(cfg["live"]["client_id_env"], 25))


def _connect(cfg: dict) -> IB:
    env = _env()
    ib = IB()
    ib.connect(env.get("IBKR_HOST", "127.0.0.1"), IBKR_PORT, clientId=_client_id(cfg))
    return ib


def _qualify(ib: IB, symbol: str) -> Stock:
    (contract,) = ib.qualifyContracts(Stock(symbol, "SMART", "USD"))
    return contract


def real_equity() -> float | None:
    """Real live account net liquidation (unlike momentum.backtest's
    NOMINAL_EQUITY_USD - this is phase 3, real money, real sizing)."""
    account_id = db.get_default_account_id()
    raw = db.get_account_info(account_id, "live").get("net_liquidation", "")
    try:
        return float(raw)
    except (TypeError, ValueError):
        return None


def size_position(entry_ref: float, stop_price: float, cfg: dict) -> tuple[int, float]:
    """(shares, risk_usd) at the REAL account equity - mirrors
    momentum.backtest._nominal_shares' formula exactly, just against a
    real number instead of an illustrative one."""
    equity = real_equity()
    risk = cfg["risk"]
    if not equity or equity <= 0:
        return 0, 0.0
    risk_usd = equity * (risk["per_trade_pct"] / 100.0)
    r_unit = entry_ref - stop_price
    if r_unit <= 0:
        return 0, risk_usd
    shares = int(risk_usd / r_unit)
    notional_cap = int(risk["max_position_notional_usd"] / max(entry_ref, 0.01))
    return max(0, min(shares, notional_cap)), risk_usd


def check_circuit_breakers(strategy: str, cfg: dict) -> tuple[bool, str]:
    """Gates a NEW entry only - never blocks managing/closing an existing
    position. Checked immediately before every place_entry call."""
    live_cfg = cfg["live"]
    per = live_cfg["per_strategy"].get(strategy)
    if per is None:
        return False, f"no live.per_strategy config for {strategy}"

    today = datetime.now(ET).date().isoformat()

    open_for_strategy = store.get_open_positions(strategy)
    if len(open_for_strategy) >= per["max_concurrent_positions"]:
        return False, f"{strategy}: {len(open_for_strategy)} open >= max_concurrent_positions {per['max_concurrent_positions']}"

    trades_today = store.count_trades_today(strategy, today, reason="entry")
    if trades_today >= per["daily_max_trades"]:
        return False, f"{strategy}: {trades_today} entries today >= daily_max_trades {per['daily_max_trades']}"

    strat_pnl = store.realized_pnl_today(today, strategy)
    if strat_pnl <= -abs(per["daily_max_loss_usd"]):
        return False, f"{strategy}: today's realized P&L ${strat_pnl:.2f} <= -${per['daily_max_loss_usd']}"

    global_pnl = store.realized_pnl_today(today)
    if global_pnl <= -abs(live_cfg["global_daily_max_loss_usd"]):
        return False, f"global: today's realized P&L ${global_pnl:.2f} <= -${live_cfg['global_daily_max_loss_usd']}"

    return True, ""


def _place_stop(ib: IB, symbol: str, qty: int, stop_price: float) -> int:
    contract = _qualify(ib, symbol)
    order = StopOrder("SELL", qty, round(stop_price, 2))
    if getattr(ib, "account", None):
        order.account = ib.account
    trade = ib.placeOrder(contract, order)
    ib.sleep(1)
    return trade.order.orderId


def _place_market(ib: IB, symbol: str, side: str, qty: int) -> tuple[int, float, str]:
    """Returns (order_id, avg_fill_price, status). Waits for a terminal
    status the same way src.ibkr_client.IBKRClient.place_order does."""
    contract = _qualify(ib, symbol)
    order = MarketOrder(side, qty)
    order.outsideRth = True
    order.tif = "DAY"
    if getattr(ib, "account", None):
        order.account = ib.account
    trade = ib.placeOrder(contract, order)
    deadline = _time.monotonic() + ORDER_FILL_TIMEOUT_SECONDS
    while _time.monotonic() < deadline:
        ib.sleep(0.5)
        if trade.isDone():
            break
    return trade.order.orderId, trade.orderStatus.avgFillPrice or 0.0, trade.orderStatus.status


def _find_open_order(ib: IB, order_id: int | None):
    """The REAL Order object (with its true clientId as IBKR assigned it)
    for one of our own resting orders - reqAllOpenOrders sees every order
    on the account, filtered here to the id and account we're looking for.
    Never hand-construct a stand-in Order: cancel_order_any_client decides
    its whole cancel path from order.clientId, and a wrong/blank one there
    would either silently no-op or reconnect with a bogus client id."""
    if order_id is None:
        return None
    ib.reqAllOpenOrders()
    ib.sleep(1)
    return next((t.order for t in ib.openTrades()
                if t.order.orderId == order_id and belongs_to_account(ib, t.order.account)), None)


def _cancel_resting_stop(ib: IB, stop_order_id: int | None) -> None:
    order = _find_open_order(ib, stop_order_id)
    if order is None:
        log.info("resting stop id=%s not found among open orders (already filled/cancelled?)", stop_order_id)
        return
    cancel_order_any_client(ib, order)


def place_entry(sig, cfg: dict) -> int | None:
    """sig: a momentum.signals.Signal already past cooldown/finalize.
    Returns the new momentum_positions id, or None if skipped (breaker,
    zero size, order not filled)."""
    strategy = sig.strategy
    ok, reason = check_circuit_breakers(strategy, cfg)
    if not ok:
        log.info("live entry skipped (%s): %s", sig.symbol, reason)
        return None

    shares, risk_usd = size_position(sig.entry_ref, sig.stop_price, cfg)
    if shares <= 0:
        log.info("live entry skipped (%s): sizing produced 0 shares (equity/levels invalid)", sig.symbol)
        return None

    ib = _connect(cfg)
    try:
        order_id, fill_price, status = _place_market(ib, sig.symbol, "BUY", shares)
        if status != "Filled" or fill_price <= 0:
            log.warning("live entry NOT filled: %s %s shares, status=%s", sig.symbol, shares, status)
            notify(f"Momentum LIVE entry failed: {sig.symbol}",
                  f"{strategy} wanted {shares}sh, order status={status}")
            return None

        stop_order_id = _place_stop(ib, sig.symbol, shares, sig.stop_price)
    finally:
        ib.disconnect()

    now = datetime.now(ET)
    position_id = store.open_position({
        "signal_id": None, "strategy": strategy, "symbol": sig.symbol, "qty": shares,
        "entry_price": fill_price, "entry_time_iso": now.isoformat(timespec="seconds"),
        "trade_date": now.date().isoformat(), "stop_price": sig.stop_price,
        "initial_stop_price": sig.stop_price,
        "stop_order_id": stop_order_id, "entry_order_id": order_id, "state": "open",
    })
    store.record_position_trade({
        "position_id": position_id, "strategy": strategy, "symbol": sig.symbol, "side": "BUY",
        "qty": shares, "price": fill_price, "order_id": order_id, "reason": "entry",
        "timestamp_iso": now.isoformat(timespec="seconds"), "trade_date": now.date().isoformat(),
    })
    db.log_decision(db.get_default_account_id(), "live", "momentum_live_entry",
                    strategy=strategy, symbol=sig.symbol, shares=shares, fill_price=fill_price,
                    stop_price=sig.stop_price, risk_usd=round(risk_usd, 2))
    notify(f"Momentum LIVE entry: {strategy} {sig.symbol}",
          f"Bought {shares}sh @ {fill_price:.2f}, stop {sig.stop_price:.2f} (risk ~${risk_usd:.0f})")
    log.info("LIVE ENTRY #%s %s %s %s@%.2f stop=%.2f", position_id, strategy, sig.symbol, shares, fill_price, sig.stop_price)
    return position_id


def _close_at_market(ib: IB, pos: dict, reason: str) -> None:
    """Always cancel the resting stop FIRST - a market close racing an
    untouched resting stop on the same shares is exactly how you end up
    accidentally short (the stop firing on a position that's already
    flat)."""
    _cancel_resting_stop(ib, pos["stop_order_id"])
    order_id, fill_price, status = _place_market(ib, pos["symbol"], "SELL", pos["qty"])
    now = datetime.now(ET)
    if status == "Filled" and fill_price > 0:
        pnl = (fill_price - pos["entry_price"]) * pos["qty"] + pos.get("realized_pnl_usd", 0.0)
        store.record_position_trade({
            "position_id": pos["id"], "strategy": pos["strategy"], "symbol": pos["symbol"], "side": "SELL",
            "qty": pos["qty"], "price": fill_price, "order_id": order_id, "reason": reason,
            "timestamp_iso": now.isoformat(timespec="seconds"), "trade_date": pos["trade_date"],
        })
        store.close_position(pos["id"], now.isoformat(timespec="seconds"), reason, pnl)
        notify(f"Momentum LIVE exit: {pos['strategy']} {pos['symbol']}",
              f"Closed {pos['qty']}sh @ {fill_price:.2f} ({reason}) - P&L ${pnl:+.2f}")
        log.info("LIVE CLOSE #%s %s %s@%.2f (%s) pnl=%.2f", pos["id"], pos["symbol"], pos["qty"], fill_price, reason, pnl)
    else:
        log.error("live close NOT filled for position #%s (%s): status=%s - still open, will retry next cycle",
                  pos["id"], pos["symbol"], status)
        notify(f"Momentum LIVE close FAILED: {pos['symbol']}", f"status={status} - still open, retrying")


def _scale_out(ib: IB, pos: dict, cfg: dict) -> None:
    ex = cfg["exit"]
    half = pos["qty"] // 2
    if half <= 0:
        return
    order_id, fill_price, status = _place_market(ib, pos["symbol"], "SELL", half)
    now = datetime.now(ET)
    if status != "Filled" or fill_price <= 0:
        log.error("scale1 SELL not filled for position #%s: status=%s", pos["id"], status)
        return
    pnl_piece = (fill_price - pos["entry_price"]) * half
    store.record_position_trade({
        "position_id": pos["id"], "strategy": pos["strategy"], "symbol": pos["symbol"], "side": "SELL",
        "qty": half, "price": fill_price, "order_id": order_id, "reason": "scale1",
        "timestamp_iso": now.isoformat(timespec="seconds"), "trade_date": pos["trade_date"],
    })
    remaining_qty = pos["qty"] - half
    new_stop_price = pos["entry_price"] if ex["breakeven_after_first_scale"] else pos["stop_price"]
    new_stop_id = None
    if remaining_qty > 0:
        _cancel_resting_stop(ib, pos["stop_order_id"])
        new_stop_id = _place_stop(ib, pos["symbol"], remaining_qty, new_stop_price)
    store.update_position(pos["id"], qty=remaining_qty, state="scaled", scaled1=1,
                          stop_price=new_stop_price, stop_order_id=new_stop_id,
                          realized_pnl_usd=pos.get("realized_pnl_usd", 0.0) + pnl_piece)
    notify(f"Momentum LIVE scale-out: {pos['strategy']} {pos['symbol']}",
          f"Sold {half}sh @ {fill_price:.2f}, stop moved to {new_stop_price:.2f} for remaining {remaining_qty}sh")
    log.info("LIVE SCALE1 #%s %s sold %s@%.2f, remaining %s, new stop %.2f",
             pos["id"], pos["symbol"], half, fill_price, remaining_qty, new_stop_price)


def manage_open_positions(cfg: dict) -> None:
    """One pass over every open momentum position: reconciles against the
    broker's own real holdings first (did the resting stop already
    trigger?), then evaluates the G8/G10/G11 exit rules against fresh
    bars, acting on whichever fires. Meant to run every
    live.management_poll_seconds."""
    open_positions = store.get_open_positions()
    if not open_positions:
        return
    ex = cfg["exit"]
    force_close_h, force_close_m = (int(x) for x in cfg["live"]["force_close_et"].split(":"))
    force_close_time = dtime(force_close_h, force_close_m)

    ib = _connect(cfg)
    try:
        broker_symbols = {p.contract.symbol: p.position for p in scoped_positions(ib)}
        for pos in open_positions:
            broker_qty = broker_symbols.get(pos["symbol"])
            if broker_qty is None or broker_qty <= 0:
                # the resting stop (or something outside this loop) already
                # closed it at the broker - reconcile rather than re-sell
                log.warning("position #%s (%s) is flat at the broker but open in our DB - reconciling", pos["id"], pos["symbol"])
                store.close_position(pos["id"], datetime.now(ET).isoformat(timespec="seconds"),
                                     "broker_reconcile", pos.get("realized_pnl_usd", 0.0))
                notify(f"Momentum LIVE: {pos['symbol']} reconciled",
                      "Position was already flat at the broker (stop likely filled) - marked closed. Verify P&L manually.")
                continue

            now = datetime.now(ET)
            if now.time() >= force_close_time:
                _close_at_market(ib, pos, "eod")
                continue

            session = bars.session_bars(bars.get_intraday(pos["symbol"], "5m", archive=True))
            if session is None or len(session) < 2:
                continue
            if (now - session.index[-1].to_pydatetime()).total_seconds() < 300:
                session = session.iloc[:-1]
            if session.empty:
                continue
            last = session.iloc[-1]

            initial_r_unit = pos["entry_price"] - pos["initial_stop_price"]
            cur_r = ((float(last["Close"]) - pos["entry_price"]) / initial_r_unit) if initial_r_unit > 0 else 0.0

            if not pos["scaled1"]:
                first_scale_price = pos["entry_price"] + ex["first_scale_r"] * initial_r_unit
                if float(last["High"]) >= first_scale_price:
                    _scale_out(ib, pos, cfg)
                    continue

            if pos["scaled1"] and ex["runner_trail"] == "prior_5m_low" and len(session) >= 2:
                prior_low = float(session["Low"].iloc[-2])
                if prior_low > pos["stop_price"]:
                    _cancel_resting_stop(ib, pos["stop_order_id"])
                    new_stop_id = _place_stop(ib, pos["symbol"], pos["qty"], prior_low)
                    store.update_position(pos["id"], stop_price=prior_low, stop_order_id=new_stop_id)
                    pos["stop_price"], pos["stop_order_id"] = prior_low, new_stop_id

            if ex["red_candle_exit_enabled"] and float(last["Close"]) < float(last["Open"]) and cur_r < ex["red_candle_hold_min_r"]:
                _close_at_market(ib, pos, "red_candle")
                continue

            if not pos["scaled1"]:
                entry_dt = datetime.fromisoformat(pos["entry_time_iso"])
                minutes_open = (now - entry_dt).total_seconds() / 60.0
                if minutes_open >= ex["bailout_minutes"] and cur_r <= 0.1:
                    _close_at_market(ib, pos, "bailout")
                    continue
    finally:
        ib.disconnect()


def force_close_all(cfg: dict) -> None:
    open_positions = store.get_open_positions()
    if not open_positions:
        return
    ib = _connect(cfg)
    try:
        for pos in open_positions:
            _close_at_market(ib, pos, "eod_force_close")
    finally:
        ib.disconnect()
