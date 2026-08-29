"""Fetches/refreshes the local historical-bars cache for ES (E-mini S&P
500 futures) - the same cache backtest_engine.py reads from (see
src/backtest_data.py) and the same paging/retry/pacing machinery as
fetch_backtest_data.py (see that module's own docstring for the full
"why chunks, why retries, why a warm-up request" reasoning, all of it
reused verbatim here via _fetch_span/_warm_up_connection rather than
copied), but for a single symbol - ES - and using ib_async's ContFuture
instead of Stock, since ES is a futures contract, not equity.

ContFuture resolves to whatever IBKR considers the current front-month
contract at request time, so this never has to track ES's own quarterly
roll dates (Mar/Jun/Sep/Dec) itself - the tradeoff is a small, expected
price discontinuity in the cached series right at each roll (a genuinely
different underlying contract before/after), which src/es_filter.py's
own same-day VWAP reset already absorbs for this feature's purposes
(only a same-session price to that same session's VWAP is ever compared
- never a cross-day price difference).

Cached under the plain symbol key "ES" (see ES_SYMBOL below) - reads the
exact same way any other symbol's cache would via
src/backtest_data.load_cached_bars("ES", "5 mins").

Needs a live IB Gateway connection - see fetch_backtest_data.py's own
docstring; same requirement, same reason (run on the deployed server).
Requires the account to actually hold CME futures market-data
entitlements - without that, every request here fails and this script
exits non-zero; it does NOT fall back to anything, since backtest ES
data with no real entitlement would just be wrong.
"""
import argparse
import sys
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from dotenv import dotenv_values
from ib_async import ContFuture

from fetch_backtest_data import (
    DEFAULT_INITIAL_DURATION, REQUEST_TIMEOUT_SECONDS, _duration_to_days, _fetch_span, _warm_up_connection,
)
from src import db, es_filter, mode_config
from src.backtest_data import load_cached_bars, merge_bars, save_cached_bars
from src.ibkr_client import IBKRClient
from src.notify import notify

PROJECT_DIR = Path(__file__).resolve().parent


def fetch_es(ib, initial_duration: str = DEFAULT_INITIAL_DURATION) -> dict:
    """Same incremental-top-up-or-initial-backfill shape as
    fetch_backtest_data.fetch_symbol, narrowed to ES's own ContFuture and
    without that function's "depth shortfall" backward-redeepen pass -
    ES only ever needs to cover whatever date range the equity symbols'
    own backtests already do, not years of independent depth, so a
    single incremental top-up (or one full initial backfill) is enough."""
    contract = ContFuture(es_filter.ES_SYMBOL, es_filter.ES_EXCHANGE)
    qualified = ib.qualifyContracts(contract)
    if not qualified or qualified[0] is None:
        return {"symbol": es_filter.ES_SYMBOL, "status": "no_security_definition", "new_bars": 0}
    existing = load_cached_bars(es_filter.ES_SYMBOL, es_filter.ES_BAR_SIZE)

    if existing is None or existing.empty:
        df = _fetch_span(ib, qualified[0], _duration_to_days(initial_duration))
        if df.empty:
            return {"symbol": es_filter.ES_SYMBOL, "status": "no_data", "new_bars": 0}
        merged = merge_bars(existing, df)
        save_cached_bars(es_filter.ES_SYMBOL, es_filter.ES_BAR_SIZE, merged)
        return {"symbol": es_filter.ES_SYMBOL, "status": "ok", "new_bars": len(merged), "total_bars": len(merged)}

    gap_days = (datetime.now(ZoneInfo("America/New_York")) - existing.index.max()).days
    if gap_days < 1:
        return {"symbol": es_filter.ES_SYMBOL, "status": "up_to_date", "new_bars": 0, "total_bars": len(existing)}
    forward_df = _fetch_span(ib, qualified[0], gap_days + 2)
    if forward_df.empty:
        return {"symbol": es_filter.ES_SYMBOL, "status": "up_to_date", "new_bars": 0, "total_bars": len(existing)}
    merged = merge_bars(existing, forward_df)
    save_cached_bars(es_filter.ES_SYMBOL, es_filter.ES_BAR_SIZE, merged)
    return {
        "symbol": es_filter.ES_SYMBOL, "status": "ok",
        "new_bars": len(merged) - len(existing), "total_bars": len(merged),
    }


def run_fetch(account_id: int, duration: str = DEFAULT_INITIAL_DURATION, mode: str = "paper") -> dict:
    env = dotenv_values(PROJECT_DIR / ".env")
    ibkr = IBKRClient(
        env.get("IBKR_HOST", "127.0.0.1"),
        mode_config.ibkr_port(env, account_id, mode),
        int(env.get("IBKR_BACKTEST_CLIENT_ID", 4)),
        account=mode_config.ibkr_account(env, account_id, mode),
    )
    ibkr.ib.RequestTimeout = REQUEST_TIMEOUT_SECONDS + 20
    _warm_up_connection(ibkr.ib)
    try:
        result = fetch_es(ibkr.ib, duration)
    except Exception as exc:  # noqa: BLE001 - report, don't crash the caller
        result = {"symbol": es_filter.ES_SYMBOL, "status": "error", "error": f"{type(exc).__name__}: {exc}"}
    finally:
        ibkr.disconnect()
    notify("ES backtest data fetch", f"{result['status']} ({result.get('new_bars', 0)} new bars)", "default")
    return result


def main():
    db.init_db(seed_rules_path=PROJECT_DIR / "rules.json")
    parser = argparse.ArgumentParser()
    parser.add_argument("--account-id", type=int, default=None,
                         help="Defaults to the admin account when omitted.")
    parser.add_argument("--duration", default=DEFAULT_INITIAL_DURATION,
                         help="Initial backfill depth if ES has no cache yet, e.g. '2 Y'")
    parser.add_argument("--mode", choices=["paper", "live"], default="paper",
                         help="Which IBKR Gateway to connect through (read-only, never places an order).")
    args = parser.parse_args()
    account_id = args.account_id if args.account_id is not None else db.get_default_account_id()

    result = run_fetch(account_id, args.duration, args.mode)
    print(result)
    sys.exit(0 if result["status"] in ("ok", "up_to_date") else 1)


if __name__ == "__main__":
    main()
