"""One-off sanity check: buy 1 share of MU on the IBKR paper account to prove
the whole Claude -> Python -> IBKR chain is wired up. See close_one.py to
flatten the position afterwards. Not part of the scheduled bot."""
import sys
import time

from ib_async import IB, MarketOrder, Stock

HOST = "127.0.0.1"
PORT = 7497
CLIENT_ID = 10
SYMBOL = "MU"


def main():
    ib = IB()
    try:
        ib.connect(HOST, PORT, clientId=CLIENT_ID)
    except Exception as exc:
        print(f"Connection failed: {exc}")
        sys.exit(1)

    try:
        contract = Stock(SYMBOL, "SMART", "USD")
        (qualified,) = ib.qualifyContracts(contract)

        order = MarketOrder("BUY", 1)
        order.outsideRth = True
        trade = ib.placeOrder(qualified, order)

        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            ib.sleep(0.5)
            if trade.orderStatus.status not in ("PendingSubmit", "PreSubmitted", ""):
                break

        status = trade.orderStatus.status
        fill_price = trade.orderStatus.avgFillPrice
        fill_price_str = f"{fill_price:.2f}" if fill_price else "pending"

        print(f"Order ID: {trade.order.orderId}")
        print(f"Fill price: {fill_price_str}")
        print(f"Status: {status}")

        if status in ("Cancelled", "ApiCancelled", "Inactive"):
            for entry in trade.log:
                print(f"  trade.log: {entry}")
            sys.exit(1)

        print("ORDERED: 1 share MU. Check TWS positions panel.")
    finally:
        ib.disconnect()


if __name__ == "__main__":
    main()
