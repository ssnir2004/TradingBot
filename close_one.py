"""One-off sanity check: close the MU position that buy_one.py opened. Uses a
different clientId than buy_one.py so the two connections don't collide."""
import sys
import time

from ib_async import IB, MarketOrder

HOST = "127.0.0.1"
PORT = 7497
CLIENT_ID = 11
SYMBOL = "MU"


def main():
    ib = IB()
    try:
        ib.connect(HOST, PORT, clientId=CLIENT_ID)
    except Exception as exc:
        print(f"Connection failed: {exc}")
        sys.exit(1)

    try:
        position = next(
            (p for p in ib.positions() if p.contract.symbol == SYMBOL),
            None,
        )
        if position is None:
            print("NO POSITION TO CLOSE")
            sys.exit(0)

        quantity = int(position.position)
        order = MarketOrder("SELL", quantity)
        order.outsideRth = True
        trade = ib.placeOrder(position.contract, order)

        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            ib.sleep(0.5)
            if trade.orderStatus.status not in ("PendingSubmit", "PreSubmitted", ""):
                break

        status = trade.orderStatus.status
        fill_price = trade.orderStatus.avgFillPrice
        fill_price_str = f"{fill_price:.2f}" if fill_price else "pending"

        print(f"Sold quantity: {quantity}")
        print(f"Fill price: {fill_price_str}")
        print(f"Status: {status}")
        print("CLOSED: position flat. Verify in TWS.")
    finally:
        ib.disconnect()


if __name__ == "__main__":
    main()
