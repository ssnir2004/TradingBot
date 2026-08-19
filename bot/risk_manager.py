"""Position sizing, daily loss halt, partial-profit/trailing-stop tracking, and
the end-of-day force-close rule.
"""
from __future__ import annotations

from dataclasses import dataclass, field

from loguru import logger

import config


@dataclass
class OpenPosition:
    symbol: str
    quantity: int
    entry_price: float
    stop_price: float
    take_profit_price: float
    initial_risk_per_share: float
    partial_taken: bool = False
    highest_price: float = field(default=0.0)

    def __post_init__(self):
        self.highest_price = self.entry_price


class RiskManager:
    def __init__(self, starting_equity: float):
        self.starting_equity = starting_equity
        self.realized_pnl_today = 0.0
        self.open_positions: dict[str, OpenPosition] = {}

    # ---- Daily loss halt ----
    def daily_loss_limit_hit(self) -> bool:
        if self.starting_equity <= 0:
            return False
        loss_pct = -self.realized_pnl_today / self.starting_equity * 100
        hit = loss_pct >= config.MAX_DAILY_LOSS_PCT
        if hit:
            logger.warning(
                "Daily loss limit hit: {:.2f}% >= {:.2f}% - halting new entries",
                loss_pct, config.MAX_DAILY_LOSS_PCT,
            )
        return hit

    def record_realized_pnl(self, amount: float):
        self.realized_pnl_today += amount

    # ---- Entry sizing ----
    def can_open_new_position(self) -> bool:
        if self.daily_loss_limit_hit():
            return False
        return len(self.open_positions) < config.MAX_CONCURRENT_POSITIONS

    def position_size(self, entry_price: float, stop_price: float) -> int:
        risk_per_share = entry_price - stop_price
        if risk_per_share <= 0:
            return 0
        risk_dollars = self.starting_equity * (config.RISK_PER_TRADE_PCT / 100)
        shares = int(risk_dollars // risk_per_share)
        return max(shares, 0)

    def open_position(self, symbol: str, quantity: int, entry_price: float,
                       stop_price: float, take_profit_price: float) -> OpenPosition:
        pos = OpenPosition(
            symbol=symbol,
            quantity=quantity,
            entry_price=entry_price,
            stop_price=stop_price,
            take_profit_price=take_profit_price,
            initial_risk_per_share=entry_price - stop_price,
        )
        self.open_positions[symbol] = pos
        return pos

    # ---- Partial profit + trailing stop management ----
    def update_trailing_stop(self, symbol: str, current_price: float, atr: float) -> float | None:
        """Ratchets the stop up as price makes new highs. Returns the new stop
        if it moved, otherwise None."""
        pos = self.open_positions.get(symbol)
        if not pos:
            return None

        pos.highest_price = max(pos.highest_price, current_price)
        candidate_stop = pos.highest_price - config.TRAILING_STOP_ATR_MULT * atr

        if candidate_stop > pos.stop_price:
            pos.stop_price = candidate_stop
            logger.info("Trailing stop for {} moved up to {:.2f}", symbol, candidate_stop)
            return candidate_stop
        return None

    def should_take_partial_profit(self, symbol: str, current_price: float) -> bool:
        pos = self.open_positions.get(symbol)
        if not pos or pos.partial_taken:
            return False
        return current_price >= pos.take_profit_price

    def mark_partial_taken(self, symbol: str):
        pos = self.open_positions.get(symbol)
        if pos:
            pos.partial_taken = True

    def close_position(self, symbol: str):
        self.open_positions.pop(symbol, None)
