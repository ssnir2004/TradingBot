"""Single-symbol dev/test CLI: connect, evaluate strategy.evaluate() for one
symbol, and (unless --check-only) hand off to trade.py to actually place the
order. Use this to sanity-check a symbol by hand; cycle.py is what runs the
whole watchlist autonomously on schedule. --mode picks which IB Gateway
(paper on 4002, live on 4001) and which risk sizing (PAPER_*/LIVE_* in
.env) to use.
"""
import argparse
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from dotenv import dotenv_values

import strategy
from src import db, mode_config
from src.ibkr_client import IBKRClient

PROJECT_DIR = Path(__file__).resolve().parent
ET = ZoneInfo("America/New_York")


def log(message: str):
    stamp = datetime.now(ET).strftime("%H:%M:%S")
    print(f"[{stamp} ET] {message}")


def main():
    db.init_db(seed_rules_path=PROJECT_DIR / "rules.json")
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=db.MODES, default="paper")
    parser.add_argument("--account-id", type=int, default=None,
                         help="Defaults to the admin account when omitted (manual/dev use).")
    parser.add_argument("--symbol", required=True)
    parser.add_argument("--check-only", action="store_true")
    args = parser.parse_args()
    symbol = args.symbol.upper()
    account_id = args.account_id if args.account_id is not None else db.get_default_account_id()

    env = dotenv_values(PROJECT_DIR / ".env")
    risk = mode_config.risk_params(env, account_id, args.mode)

    if db.count_todays_entries(account_id, args.mode, "long") >= risk["max_trades_per_day"]:
        log(f"MAX_TRADES_PER_DAY ({risk['max_trades_per_day']}) reached for today, skipping")
        sys.exit(0)

    ibkr = IBKRClient(
        env.get("IBKR_HOST", "127.0.0.1"),
        mode_config.ibkr_port(env, account_id, args.mode),
        int(env.get("IBKR_CLIENT_ID", 2)),
    )

    try:
        result = strategy.evaluate(account_id, args.mode, symbol, ibkr.ib)
        log(f"evaluate({symbol}) -> {result}")

        if args.check_only:
            sys.exit(0)

        if not result["pass"]:
            log(f"not trading {symbol}: {result['reasons']}")
            sys.exit(0)

        budget = risk["portfolio_value"] * 0.10
        price = result["price"]
        quantity = int(budget / price) if price else 0

        if quantity < 1:
            log("position too small")
            sys.exit(0)
    finally:
        ibkr.disconnect()

    log(f"spawning trade.py [{args.mode}] BUY {quantity} {symbol}")
    try:
        proc = subprocess.run(
            [sys.executable, str(PROJECT_DIR / "trade.py"), "--mode", args.mode,
             "--account-id", str(account_id), "--symbol", symbol, "--side", "BUY", "--size", str(quantity)],
            capture_output=True, text=True, timeout=40,
        )
        print(proc.stdout)
        if proc.stderr:
            print(proc.stderr, file=sys.stderr)
    except subprocess.TimeoutExpired:
        log("trade.py timed out after 40s")
        sys.exit(1)

    rows = db.get_trades(account_id, args.mode, limit=1)
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
