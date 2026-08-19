"""Thin wrapper around ib_async used by bot.py, trade.py, and cycle.py."""
import time

from ib_async import IB, MarketOrder, Stock, Trade

SETTLED_STATUSES_TIMEOUT = 10
PENDING_STATUSES = ("PendingSubmit", "PreSubmitted", "")


class IBKRClient:
    def __init__(self, host: str, port: int, client_id: int):
        self.ib = IB()
        self.ib.connect(host, port, clientId=client_id)

    def place_order(self, symbol: str, side: str, quantity: int) -> Trade:
        contract = Stock(symbol, "SMART", "USD")
        (qualified,) = self.ib.qualifyContracts(contract)

        order = MarketOrder(side, quantity)
        order.outsideRth = True
        trade = self.ib.placeOrder(qualified, order)

        deadline = time.monotonic() + SETTLED_STATUSES_TIMEOUT
        while time.monotonic() < deadline:
            self.ib.sleep(0.5)
            if trade.orderStatus.status not in PENDING_STATUSES:
                break

        return trade

    def disconnect(self):
        self.ib.disconnect()
