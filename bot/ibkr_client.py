"""Thin wrapper around ib_async for connecting to TWS/IB Gateway, pulling bars,
and placing bracket orders (entry + stop-loss + take-profit) safely.
"""
from __future__ import annotations

from dataclasses import dataclass

from ib_async import IB, Stock, MarketOrder, LimitOrder, StopOrder, Trade
from loguru import logger

import config


@dataclass
class BracketResult:
    parent: Trade
    stop: Trade
    take_profit: Trade


class IBKRClient:
    def __init__(self):
        self.ib = IB()

    def connect(self):
        logger.info(
            "Connecting to IBKR at {}:{} (clientId={}, {} account)",
            config.IBKR_HOST,
            config.IBKR_PORT,
            config.IBKR_CLIENT_ID,
            "PAPER" if config.IS_PAPER_ACCOUNT else "LIVE",
        )
        self.ib.connect(config.IBKR_HOST, config.IBKR_PORT, clientId=config.IBKR_CLIENT_ID)
        return self.ib

    def disconnect(self):
        if self.ib.isConnected():
            self.ib.disconnect()

    def account_summary(self) -> dict:
        rows = self.ib.accountSummary()
        return {r.tag: r.value for r in rows}

    def net_liquidation(self) -> float:
        summary = self.account_summary()
        return float(summary.get("NetLiquidation", 0) or 0)

    def positions(self):
        return self.ib.positions()

    def qualify_stock(self, symbol: str) -> Stock:
        contract = Stock(symbol, "SMART", "USD")
        (qualified,) = self.ib.qualifyContracts(contract)
        return qualified

    def get_bars(self, symbol: str, duration: str = "2 D", bar_size: str = "5 mins",
                 what_to_show: str = "TRADES", use_rth: bool = True):
        contract = self.qualify_stock(symbol)
        bars = self.ib.reqHistoricalData(
            contract,
            endDateTime="",
            durationStr=duration,
            barSizeSetting=bar_size,
            whatToShow=what_to_show,
            useRTH=use_rth,
            formatDate=1,
        )
        return bars

    def place_bracket_order(
        self,
        symbol: str,
        quantity: int,
        stop_loss_price: float,
        take_profit_price: float,
        dry_run: bool = True,
    ) -> BracketResult | None:
        """Submit a market entry with attached stop-loss and take-profit legs.

        In DRY_RUN mode nothing is sent to IBKR; the intended order is only logged
        so the strategy can be exercised end-to-end without touching the account.
        """
        if dry_run:
            logger.info(
                "[DRY_RUN] Would BUY {} {} | stop={} take_profit={}",
                quantity, symbol, stop_loss_price, take_profit_price,
            )
            return None

        contract = self.qualify_stock(symbol)

        parent = MarketOrder("BUY", quantity)
        parent.orderId = self.ib.client.getReqId()
        parent.transmit = False

        take_profit = LimitOrder("SELL", quantity, take_profit_price)
        take_profit.orderId = self.ib.client.getReqId()
        take_profit.parentId = parent.orderId
        take_profit.transmit = False

        stop = StopOrder("SELL", quantity, stop_loss_price)
        stop.orderId = self.ib.client.getReqId()
        stop.parentId = parent.orderId
        stop.transmit = True

        parent_trade = self.ib.placeOrder(contract, parent)
        tp_trade = self.ib.placeOrder(contract, take_profit)
        stop_trade = self.ib.placeOrder(contract, stop)

        logger.info(
            "Placed bracket order for {} x{}: entry(mkt) stop={} tp={}",
            symbol, quantity, stop_loss_price, take_profit_price,
        )
        return BracketResult(parent=parent_trade, stop=stop_trade, take_profit=tp_trade)

    def close_position(self, symbol: str, quantity: int, dry_run: bool = True):
        if dry_run:
            logger.info("[DRY_RUN] Would SELL (close) {} {}", quantity, symbol)
            return None
        contract = self.qualify_stock(symbol)
        order = MarketOrder("SELL", quantity)
        trade = self.ib.placeOrder(contract, order)
        logger.warning("Force-closed position: SELL {} {}", quantity, symbol)
        return trade

    def close_all_positions(self, dry_run: bool = True):
        for pos in self.positions():
            if pos.position > 0:
                self.close_position(pos.contract.symbol, int(pos.position), dry_run=dry_run)
