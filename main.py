"""Entry point: connects to IBKR, runs the premarket scan, then monitors the
market during regular hours applying the strategy, risk management, and the
end-of-day force-close rule. Defaults to paper trading + dry-run (see
config.py / .env.example for the safety switches).
"""
from __future__ import annotations

import sys
import time

import pandas as pd
from loguru import logger

import config
from bot import market_hours, scanner, strategy, telegram_alerts, trade_journal
from bot.ibkr_client import IBKRClient
from bot.risk_manager import RiskManager

POLL_INTERVAL_SECONDS = 30


def setup_logging():
    logger.remove()
    logger.add(sys.stderr, level="INFO")
    logger.add("data/bot.log", rotation="1 day", retention="30 days", level="DEBUG")


class BotState:
    def __init__(self):
        self.candidates: list[str] = []
        self.scanned_today = False
        self.force_closed_today = False
        self.current_date = market_hours.now_eastern().date()

    def reset_for_new_day(self):
        self.candidates = []
        self.scanned_today = False
        self.force_closed_today = False
        self.current_date = market_hours.now_eastern().date()


def maybe_roll_day(state: BotState, risk: RiskManager):
    today = market_hours.now_eastern().date()
    if today != state.current_date:
        logger.info("New trading day detected, resetting state")
        state.reset_for_new_day()
        risk.realized_pnl_today = 0.0


def run_premarket_scan(state: BotState):
    if state.scanned_today:
        return
    if not market_hours.is_premarket_scan_time():
        return

    results = scanner.run_premarket_scan()
    state.candidates = [r.symbol for r in results]
    state.scanned_today = True
    telegram_alerts.scan_results(state.candidates)


def evaluate_entries(ibkr: IBKRClient, risk: RiskManager, state: BotState):
    if not risk.can_open_new_position():
        return

    for symbol in state.candidates:
        if symbol in risk.open_positions:
            continue
        if not risk.can_open_new_position():
            break
        try:
            daily_bars = ibkr.get_bars(symbol, duration="250 D", bar_size="1 day")
            intraday_bars = ibkr.get_bars(symbol, duration="2 D", bar_size="5 mins")
            if not daily_bars or not intraday_bars:
                continue

            daily_df = pd.DataFrame([b.__dict__ for b in daily_bars]).rename(
                columns={"high": "High", "low": "Low", "close": "Close", "volume": "Volume"}
            )
            intraday_df = pd.DataFrame([b.__dict__ for b in intraday_bars]).rename(
                columns={"high": "High", "low": "Low", "close": "Close", "volume": "Volume"}
            )

            signal = strategy.evaluate(symbol, daily_df, intraday_df)
        except Exception as exc:  # noqa: BLE001
            logger.warning("Failed evaluating {}: {}", symbol, exc)
            telegram_alerts.error(f"Failed evaluating {symbol}: {exc}")
            continue

        if not signal:
            continue

        quantity = risk.position_size(signal.entry_price, signal.stop_price)
        if quantity <= 0:
            continue

        ibkr.place_bracket_order(
            symbol, quantity, signal.stop_price, signal.take_profit_price,
            dry_run=config.DRY_RUN,
        )
        risk.open_position(symbol, quantity, signal.entry_price, signal.stop_price, signal.take_profit_price)
        trade_journal.log_trade(
            symbol, "ENTRY", quantity, signal.entry_price,
            stop_price=signal.stop_price, take_profit_price=signal.take_profit_price,
            reason=signal.reason,
        )
        telegram_alerts.trade_entry(symbol, quantity, signal.entry_price, signal.stop_price, signal.take_profit_price)


def manage_open_positions(ibkr: IBKRClient, risk: RiskManager):
    for symbol in list(risk.open_positions.keys()):
        pos = risk.open_positions[symbol]
        try:
            bars = ibkr.get_bars(symbol, duration="1 D", bar_size="5 mins")
            if not bars:
                continue
            last = bars[-1]
            current_price = float(last.close)
        except Exception as exc:  # noqa: BLE001
            logger.warning("Failed to update {}: {}", symbol, exc)
            continue

        if current_price <= pos.stop_price:
            ibkr.close_position(symbol, pos.quantity, dry_run=config.DRY_RUN)
            pnl = (current_price - pos.entry_price) * pos.quantity
            risk.record_realized_pnl(pnl)
            trade_journal.log_trade(symbol, "STOP_EXIT", pos.quantity, current_price, realized_pnl=pnl)
            telegram_alerts.trade_exit(symbol, pos.quantity, current_price, "stop hit")
            risk.close_position(symbol)
            continue

        if risk.should_take_partial_profit(symbol, current_price):
            partial_qty = max(int(pos.quantity * (config.PARTIAL_PROFIT_SIZE_PCT / 100)), 1)
            ibkr.close_position(symbol, partial_qty, dry_run=config.DRY_RUN)
            pnl = (current_price - pos.entry_price) * partial_qty
            risk.record_realized_pnl(pnl)
            risk.mark_partial_taken(symbol)
            pos.quantity -= partial_qty
            trade_journal.log_trade(symbol, "PARTIAL_EXIT", partial_qty, current_price, realized_pnl=pnl)
            telegram_alerts.partial_profit(symbol, partial_qty, current_price)

        atr = max(current_price * 0.01, 0.01)  # simple fallback ATR proxy for the live loop
        risk.update_trailing_stop(symbol, current_price, atr)


def force_close_if_needed(ibkr: IBKRClient, risk: RiskManager, state: BotState):
    if state.force_closed_today or not market_hours.is_past_force_close():
        return
    if risk.open_positions:
        telegram_alerts.force_close_all()
        for symbol, pos in list(risk.open_positions.items()):
            ibkr.close_position(symbol, pos.quantity, dry_run=config.DRY_RUN)
            trade_journal.log_trade(symbol, "FORCE_CLOSE", pos.quantity, pos.entry_price)
            risk.close_position(symbol)
    state.force_closed_today = True
    telegram_alerts.daily_summary(risk.realized_pnl_today, trades=0)


def main():
    setup_logging()
    logger.info(
        "Starting TradingBot | account={} | dry_run={}",
        "PAPER" if config.IS_PAPER_ACCOUNT else "LIVE",
        config.DRY_RUN,
    )
    telegram_alerts.bot_started("PAPER/DRY_RUN" if config.DRY_RUN else ("PAPER" if config.IS_PAPER_ACCOUNT else "LIVE"))

    ibkr = IBKRClient()
    ibkr.connect()

    equity = ibkr.net_liquidation() or 100_000.0
    risk = RiskManager(starting_equity=equity)
    state = BotState()

    try:
        while True:
            maybe_roll_day(state, risk)
            run_premarket_scan(state)

            if market_hours.is_market_open():
                evaluate_entries(ibkr, risk, state)
                manage_open_positions(ibkr, risk)

            force_close_if_needed(ibkr, risk, state)

            time.sleep(POLL_INTERVAL_SECONDS)
    except KeyboardInterrupt:
        logger.info("Shutting down on user request")
    finally:
        ibkr.disconnect()


if __name__ == "__main__":
    main()
