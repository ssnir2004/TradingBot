"""Single-symbol dev/test evaluation used by bot.py.

This only checks the time gate and position-dedupe — it is NOT the full
Long Breakout rule set. The six D1-D3 / I1-I3 filters from the active
strategy are evaluated by cycle.py against the whole S&P 500 watchlist, not
here.
"""
from datetime import datetime
from zoneinfo import ZoneInfo

from ib_async import Stock

from src import db
from src.ibkr_client import scoped_positions

ET = ZoneInfo("America/New_York")


def evaluate(account_id: int, mode: str, symbol: str, ib) -> dict:
    # bot.py (this function's only caller) is a long-only dev tool.
    rules = db.get_active_rules(account_id, "long")
    if rules is None:
        result = {"pass": bool(False), "reasons": ["no active long strategy"], "price": 0.0}
        db.log_decision(account_id, mode, "dev_tool_evaluate", symbol=symbol, **result)
        return result
    time_filter = rules["time_filter"]
    earliest = time_filter["earliest_entry_et"]
    latest = time_filter["latest_entry_et"]

    # a) already in position
    for position in scoped_positions(ib):
        if position.contract.symbol == symbol and position.position > 0:
            result = {"pass": bool(False), "reasons": ["already in position"], "price": 0.0}
            db.log_decision(account_id, mode, "dev_tool_evaluate", symbol=symbol, **result)
            return result

    # b) time window
    now_et = datetime.now(ET)
    now_hhmm = now_et.strftime("%H:%M")
    if not (earliest <= now_hhmm <= latest):
        result = {
            "pass": bool(False),
            "reasons": [f"outside entry window {earliest}-{latest}"],
            "price": 0.0,
        }
        db.log_decision(account_id, mode, "dev_tool_evaluate", symbol=symbol, **result)
        return result

    # c) time gate ok, no existing position -> fetch current price
    contract = Stock(symbol, "SMART", "USD")
    (qualified,) = ib.qualifyContracts(contract)
    ticker = ib.reqMktData(qualified, "", False, False)
    ib.sleep(2)
    price = ticker.marketPrice()
    ib.cancelMktData(qualified)

    result = {
        "pass": bool(True),
        "reasons": ["time gate ok", "no existing position"],
        "price": float(price),
    }
    db.log_decision(account_id, mode, "dev_tool_evaluate", symbol=symbol, **result)
    return result
