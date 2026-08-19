"""Single-symbol dev/test evaluation used by bot.py.

This only checks the time gate and position-dedupe — it is NOT the full
Trend Join Long rule set. The six D1-D3 / I1-I3 filters from rules.json are
evaluated by cycle.py (via src/filters.py) against the whole S&P 500
watchlist, not here.
"""
import json
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from ib_async import Stock

RULES_PATH = Path(__file__).resolve().parent / "rules.json"
SAFETY_LOG_PATH = Path(__file__).resolve().parent / "logs" / "safety_log.jsonl"
ET = ZoneInfo("America/New_York")


def _log_result(result: dict):
    SAFETY_LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(SAFETY_LOG_PATH, "a") as f:
        f.write(json.dumps(result) + "\n")


def evaluate(symbol: str, ib) -> dict:
    rules = json.loads(RULES_PATH.read_text())
    time_filter = rules["time_filter"]
    earliest = time_filter["earliest_entry_et"]
    latest = time_filter["latest_entry_et"]

    # a) already in position
    for position in ib.positions():
        if position.contract.symbol == symbol and position.position > 0:
            result = {"pass": bool(False), "reasons": ["already in position"], "price": 0.0}
            _log_result(result)
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
        _log_result(result)
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
    _log_result(result)
    return result
