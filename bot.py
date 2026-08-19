"""Single-symbol dev/test CLI: connect, evaluate strategy.evaluate() for one
symbol, and (unless --check-only) hand off to trade.py to actually place the
order. Use this to sanity-check a symbol by hand; cycle.py is what runs the
whole watchlist autonomously on schedule.
"""
import argparse
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from dotenv import dotenv_values

import strategy
from src import db
from src.ibkr_client import IBKRClient

PROJECT_DIR = Path(__file__).resolve().parent
ET = ZoneInfo("America/New_York")
LIVE_PORTS = {7496, 4001}


def log(message: str):
    stamp = datetime.now(ET).strftime("%H:%M:%S")
    print(f"[{stamp} ET] {message}")


def main():
    db.init_db(seed_rules_path=PROJECT_DIR / "rules.json")
    parser = argparse.ArgumentParser()
    parser.add_argument("--symbol", required=True)
    parser.add_argument("--check-only", action="store_true")
    args = parser.parse_args()
    symbol = args.symbol.upper()

    env = dotenv_values(PROJECT_DIR / ".env")

    paper_trading = env.get("PAPER_TRADING", "true").strip().lower() == "true"
    ibkr_port = int(env.get("IBKR_PORT", 7497))
    if paper_trading and ibkr_port in LIVE_PORTS:
        sys.exit("ABORT: paper flag but live port")
    if not paper_trading and ibkr_port not in LIVE_PORTS:
        sys.exit("ABORT: live flag but paper port")

    max_trades_per_day = int(env.get("MAX_TRADES_PER_DAY", 5))
    if db.count_todays_buys() >= max_trades_per_day:
        log(f"MAX_TRADES_PER_DAY ({max_trades_per_day}) reached for today, skipping")
        sys.exit(0)

    ibkr = IBKRClient(
        env.get("IBKR_HOST", "127.0.0.1"),
        ibkr_port,
        int(env.get("IBKR_CLIENT_ID", 2)),
    )

    try:
        result = strategy.evaluate(symbol, ibkr.ib)
        log(f"evaluate({symbol}) -> {result}")

        if args.check_only:
            sys.exit(0)

        if not result["pass"]:
            log(f"not trading {symbol}: {result['reasons']}")
            sys.exit(0)

        portfolio_value = float(env.get("PORTFOLIO_VALUE_USD", 25000))
        max_trade_size = float(env.get("MAX_TRADE_SIZE_USD", 2500))
        budget = min(max_trade_size, portfolio_value * 0.10)
        price = result["price"]
        quantity = int(budget / price) if price else 0

        if quantity < 1:
            log("position too small")
            sys.exit(0)
    finally:
        ibkr.disconnect()

    log(f"spawning trade.py BUY {quantity} {symbol}")
    try:
        proc = subprocess.run(
            [sys.executable, str(PROJECT_DIR / "trade.py"),
             "--symbol", symbol, "--side", "BUY", "--size", str(quantity)],
            capture_output=True, text=True, timeout=30,
        )
        print(proc.stdout)
        if proc.stderr:
            print(proc.stderr, file=sys.stderr)
    except subprocess.TimeoutExpired:
        log("trade.py timed out after 30s")
        sys.exit(1)

    rows = db.get_trades(limit=1)
    if not rows:
        log("no trade row written")
        sys.exit(1)

    last = rows[0]
    if last["status"] not in ("Cancelled", "ApiCancelled", "Inactive"):
        log(f"SUCCESS: {last}")
    else:
        log(f"FAILED: {last}")
        sys.exit(1)


if __name__ == "__main__":
    main()
