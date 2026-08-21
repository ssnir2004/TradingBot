"""Thin wrapper around ib_async used by bot.py, trade.py, and cycle.py."""
import time

from ib_async import IB, MarketOrder, Stock, Trade

SETTLED_STATUSES_TIMEOUT = 20


class IBKRClient:
    def __init__(self, host: str, port: int, client_id: int):
        self.ib = IB()
        self.ib.connect(host, port, clientId=client_id)

    def place_order(self, symbol: str, side: str, quantity: int) -> Trade:
        contract = Stock(symbol, "SMART", "USD")
        (qualified,) = self.ib.qualifyContracts(contract)

        order = MarketOrder(side, quantity)
        order.outsideRth = True
        # Market orders must be DAY (a market order can't stay open past the
        # session). Left unset, IBKR fills the TIF from the account's Order
        # Presets — on live that resolves to GTC, which is invalid for a
        # market order and gets the whole order cancelled (error 10349:
        # "Order TIF was set to GTC based on order preset").
        order.tif = "DAY"
        trade = self.ib.placeOrder(qualified, order)

        # trade.isDone() (Filled/Cancelled/ApiCancelled/Inactive) is the
        # correct "stop polling" signal — unlike a plain "not pending"
        # check, it correctly keeps waiting through "ValidationError",
        # which ib_async can report as a transient, still-live state that
        # often resolves to Submitted/Filled moments later (see
        # OrderStatus.WorkingStates in ib_async's order.py).
        deadline = time.monotonic() + SETTLED_STATUSES_TIMEOUT
        while time.monotonic() < deadline:
            self.ib.sleep(0.5)
            if trade.isDone():
                break

        return trade

    def disconnect(self):
        self.ib.disconnect()
